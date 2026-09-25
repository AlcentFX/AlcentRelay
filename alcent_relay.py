# ATOS Relay v1.8.2 — KISS V7 B008 wick structures + 1S/AOI basket protection
from __future__ import annotations

import html
import json
import os
import re
import sqlite3
import time
import threading
import urllib.request
import urllib.error
from contextlib import closing
from datetime import datetime, timezone

from flask import Flask, Response, jsonify, request

RELAY_BUILD_ID = "ATOS_KISS_RELAY_1_8_2_KISS_V7_B008"

SERVICE_NAME = "ATOS Relay"
RELAY_VERSION = "1.8.2"
EXPECTED_SYSTEM = "ATOS"
EXPECTED_AUTOMATION_VERSION = "1.0"

APP_SECRET = os.environ.get("ATOS_SECRET", os.environ.get("ALCENT_SECRET", "CHANGE_ME"))
DB_PATH = os.environ.get("ATOS_DB", os.environ.get("ALCENT_DB", "atos_events.db"))
MAX_BATCH = int(os.environ.get("ATOS_MAX_BATCH", "100"))
DEFAULT_STALE_ENTRY_MINUTES = int(os.environ.get("ATOS_STALE_ENTRY_MINUTES", "5"))

# v1.7.1 — Adds atomic KISS V5 signal/portfolio-action transport; preserves USD High-Impact News Protection calendar.
# Forex Factory public weekly export is cached server-side so MT4 does not need
# a second WebRequest allow-list entry or its own JSON calendar parser.
FF_CALENDAR_URL = os.environ.get(
    "ATOS_FF_CALENDAR_URL",
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
)
NEWS_CACHE_TTL_SECONDS = int(os.environ.get("ATOS_NEWS_CACHE_TTL_SECONDS", "600"))
NEWS_HTTP_TIMEOUT_SECONDS = float(os.environ.get("ATOS_NEWS_HTTP_TIMEOUT_SECONDS", "5"))

_news_cache_lock = threading.Lock()
_news_cache_events: list[dict] = []
_news_cache_fetched_at: float = 0.0
_news_cache_last_error: str = ""

ALLOWED_COMMANDS = {
    "KISS_V5_SIGNAL",
    "KISS_V6_SIGNAL",
    "KISS_V6_5M_SIGNAL",  # legacy compatibility
    "KISS_V6_15M_SIGNAL",
    "KISS_V7_SIGNAL",
    "KISS_V7_AOI_PORTFOLIO_PROTECT",
    "KISS_V6_15M_DIRECTION_FLIP_PROTECT",
    "KISS_V6_1H_ZONE_PORTFOLIO_PROTECT",
    "PLACE_PENDING",
    "PLACE_MARKET",
    "REPLACE_PENDING",
    "CANCEL_ORDER",
    "CANCEL_BUYS",
    "CANCEL_SELLS",
    "CLOSE_ORDER",
    "CLOSE_BUYS",
    "CLOSE_SELLS",
    "DEFEND_BUYS",
    "DEFEND_SELLS",
    "SET_BUY_TP",
    "SET_SELL_TP",
    "MODIFY_SL",
    "MODIFY_TP",
    "MODIFY_SLTP",
    "CANCEL_CT_ORDER",
    "CLOSE_CT_ORDER",
    "MODIFY_CT_SL",
    "MODIFY_CT_TP",

    # V6 D8 trade-management commands. Relay transports only; MT4 executes.
    "MANAGE_ORDER",
    "PARTIAL_CLOSE",
    "SET_ORDER_SL",
    "SET_TRAILING_SL",

    # V8 Build 029+ batched protection commands.
    # Relay transports only; MT4 v8.10+ applies these to actual broker market positions.
    "PROTECT_BUYS_1M_TO_EP",
    "PROTECT_SELLS_1M_TO_EP",
    "CLEAR_BUYS_1M_TEMP_TP",
    "CLEAR_SELLS_1M_TEMP_TP",
    "PROTECT_BUYS_3M_TO_EP",
    "PROTECT_SELLS_3M_TO_EP",
    "PROTECT_BUYS_3M_DYNAMIC_EP",
    "PROTECT_SELLS_3M_DYNAMIC_EP",
    "HARD_EXIT_BUYS_3M_IF_OPEN",
    "HARD_EXIT_SELLS_3M_IF_OPEN",
    "PROTECT_BUYS_5M_TO_EP",
    "PROTECT_SELLS_5M_TO_EP",
    "CLEAR_BUYS_3M_TEMP_TP",
    "CLEAR_SELLS_3M_TEMP_TP",
    "CLEAR_BUYS_5M_TEMP_TP",
    "CLEAR_SELLS_5M_TEMP_TP",
    "CLEAR_BUYS_TP_1M_OVERRIDE",
    "CLEAR_SELLS_TP_1M_OVERRIDE",
    "HARD_EXIT_BUYS",
    "HARD_EXIT_SELLS",

    # KISS V1/V2 isolated commands. strategy_id is preserved in the raw payload.
    "PROTECT_KISS_V1_BUYS",
    "PROTECT_KISS_V1_SELLS",
    "PROTECT_KISS_V2_BUYS",
    "PROTECT_KISS_V2_SELLS",
    "PROTECT_STAGE2_KISS_V2_BUYS",
    "PROTECT_STAGE2_KISS_V2_SELLS",
    "CLEAR_KISS_V1_BUYS_PROTECTION_TP",
    "CLEAR_KISS_V1_SELLS_PROTECTION_TP",
    "CLEAR_KISS_V2_BUYS_PROTECTION_TP",
    "CLEAR_KISS_V2_SELLS_PROTECTION_TP",
    "HARD_EXIT_KISS_V1_BUYS",
    "HARD_EXIT_KISS_V1_SELLS",
    "HARD_EXIT_KISS_V2_BUYS",
    "HARD_EXIT_KISS_V2_SELLS",

    # V6 protective/logical-order commands. Transport only; no strategy logic here.
    "V6_INVALIDATE_ORDER",
    "V6_SET_LOGICAL_TP",
}

app = Flask(__name__)

CONSUMER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

# v1.5.4: MT4 polling must not contend with TradingView webhook ingestion.
# Poll metadata is persisted at most once every 2 seconds per consumer.
_POLL_TELEMETRY_INTERVAL = 2.0
_poll_telemetry_lock = threading.Lock()
_poll_telemetry_last_write: dict[str, float] = {}


def _record_poll_telemetry(consumer_id: str, after_seq: int) -> None:
    now_mono = time.monotonic()
    with _poll_telemetry_lock:
        last = _poll_telemetry_last_write.get(consumer_id, 0.0)
        if now_mono - last < _POLL_TELEMETRY_INTERVAL:
            return
        _poll_telemetry_last_write[consumer_id] = now_mono

    now = int(time.time())
    try:
        with closing(db(timeout=0.25)) as conn:
            # Legacy dashboard state + per-consumer state in ONE short transaction.
            conn.execute(
                "INSERT INTO relay_state(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                ("last_poll_at", str(now)),
            )
            conn.execute(
                "INSERT INTO relay_state(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                ("last_poll_after", str(after_seq)),
            )
            conn.execute(
                """
                INSERT INTO consumers(
                    consumer_id,first_seen_at,last_seen_at,last_poll_at,last_poll_after,
                    last_ack_at,last_ack_event_id,last_ack_status
                ) VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(consumer_id) DO UPDATE SET
                    last_seen_at=excluded.last_seen_at,
                    last_poll_at=excluded.last_poll_at,
                    last_poll_after=excluded.last_poll_after
                """,
                (consumer_id, now, now, now, after_seq, None, None, None),
            )
            conn.commit()
    except sqlite3.OperationalError:
        # Poll telemetry is non-critical. Never delay MT4 or TradingView for dashboard stats.
        pass


def _consumer_id(raw: str | None) -> str:
    value = (raw or "").strip()
    if not value:
        return "default"
    if not CONSUMER_ID_RE.fullmatch(value):
        raise ValueError("consumer_id must be 1-64 chars: A-Z a-z 0-9 _ -")
    return value


def db(timeout: float = 2.0) -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=timeout)
    conn.row_factory = sqlite3.Row
    # WAL lets MT4 reads proceed while TradingView writes.
    # NORMAL materially reduces fsync latency while retaining durable WAL commits.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=2000")
    return conn


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _ensure_column(conn: sqlite3.Connection, table: str, name: str, definition: str) -> None:
    if name not in _column_names(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")


def init_db() -> None:
    with closing(db()) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        # Preserve compatibility with the legacy Alcent table while extending it.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                payload TEXT NOT NULL,
                received_at INTEGER NOT NULL
            )
            """
        )
        for name, definition in [
            ("system", "TEXT"),
            ("automation_version", "TEXT"),
            ("strategy_version", "TEXT"),
            ("command", "TEXT"),
            ("direction", "TEXT"),
            ("reason", "TEXT"),
            ("trading_period_id", "TEXT"),
            ("campaign_id", "TEXT"),
            ("ct_campaign_id", "TEXT"),
            ("order_id", "TEXT"),
            ("event_time_ms", "INTEGER"),
            ("close_percent", "REAL"),
            ("trailing_distance", "REAL"),
            ("new_stop_loss", "REAL"),
            ("acked_at", "INTEGER"),
            ("ack_status", "TEXT"),
            ("ack_detail", "TEXT"),
        ]:
            _ensure_column(conn, "events", name, definition)

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rejected_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT,
                command TEXT,
                reason TEXT NOT NULL,
                payload TEXT,
                received_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS relay_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )

        # v1.5.0 multi-account consumer registry.
        # Delivery remains append-only/broadcast: every consumer has its own local cursor.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS consumers (
                consumer_id TEXT PRIMARY KEY,
                first_seen_at INTEGER NOT NULL,
                last_seen_at INTEGER NOT NULL,
                last_poll_at INTEGER,
                last_poll_after INTEGER NOT NULL DEFAULT 0,
                last_ack_at INTEGER,
                last_ack_event_id TEXT,
                last_ack_status TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS consumer_acks (
                consumer_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                acked_at INTEGER NOT NULL,
                status TEXT,
                detail TEXT,
                PRIMARY KEY (consumer_id, event_id)
            )
            """
        )
        conn.commit()


def _set_state(key: str, value: str) -> None:
    with closing(db()) as conn:
        conn.execute(
            "INSERT INTO relay_state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        conn.commit()


def _get_state(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM relay_state WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def _touch_consumer(
    consumer_id: str,
    *,
    poll_after: int | None = None,
    ack_event_id: str | None = None,
    ack_status: str | None = None,
) -> None:
    now = int(time.time())
    with closing(db()) as conn:
        conn.execute(
            """
            INSERT INTO consumers(
                consumer_id,first_seen_at,last_seen_at,last_poll_at,last_poll_after,
                last_ack_at,last_ack_event_id,last_ack_status
            ) VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(consumer_id) DO UPDATE SET
                last_seen_at=excluded.last_seen_at,
                last_poll_at=COALESCE(excluded.last_poll_at,consumers.last_poll_at),
                last_poll_after=CASE
                    WHEN excluded.last_poll_at IS NOT NULL THEN excluded.last_poll_after
                    ELSE consumers.last_poll_after
                END,
                last_ack_at=COALESCE(excluded.last_ack_at,consumers.last_ack_at),
                last_ack_event_id=COALESCE(excluded.last_ack_event_id,consumers.last_ack_event_id),
                last_ack_status=COALESCE(excluded.last_ack_status,consumers.last_ack_status)
            """,
            (
                consumer_id, now, now,
                now if poll_after is not None else None,
                int(poll_after or 0),
                now if ack_event_id is not None else None,
                ack_event_id,
                ack_status,
            ),
        )
        conn.commit()


def _authorised(payload: dict | None = None) -> bool:
    # TradingView can use ?secret=... in the webhook URL. MT4 already does this.
    supplied = request.args.get("secret", "")
    if not supplied:
        supplied = request.headers.get("X-ATOS-Secret", "")
    if not supplied and isinstance(payload, dict):
        supplied = str(payload.get("secret", ""))
    return supplied == APP_SECRET


def _record_rejection(payload: dict | None, reason: str) -> None:
    payload = payload if isinstance(payload, dict) else {}
    with closing(db()) as conn:
        conn.execute(
            "INSERT INTO rejected_events(event_id,command,reason,payload,received_at) VALUES(?,?,?,?,?)",
            (
                str(payload.get("event_id", "")),
                str(payload.get("command", "")),
                reason,
                json.dumps(payload, separators=(",", ":")) if payload else None,
                int(time.time()),
            ),
        )
        conn.commit()


def _validate_event(payload: dict) -> tuple[bool, str, int]:
    if payload.get("system") != EXPECTED_SYSTEM:
        return False, "invalid system", 400
    if str(payload.get("automation_version", "")) != EXPECTED_AUTOMATION_VERSION:
        return False, "unsupported automation_version", 400

    event_id = str(payload.get("event_id", "")).strip()
    if not event_id:
        return False, "event_id required", 400

    command = str(payload.get("command", "")).strip().upper()
    if command not in ALLOWED_COMMANDS:
        return False, "unknown command", 400

    # V4 dual-engine ownership. The relay preserves the complete payload;
    # MT4 uses campaign_id/engine_id to scope cancel/defend/inventory actions.
    strategy_version = str(payload.get("strategy_version", "")).strip().upper()
    if strategy_version.startswith("V4-"):
        engine_id = str(payload.get("engine_id", "")).strip().upper()
        campaign_id = str(payload.get("campaign_id", "")).strip().upper()
        if engine_id not in ("5M", "15M"):
            return False, "V4 engine_id must be 5M or 15M", 400
        expected_campaign = "V4_" + engine_id
        if campaign_id != expected_campaign:
            return False, "V4 campaign_id does not match engine_id", 400

    # Role-gating defence in depth. Current EXECUTION alerts send true.
    if "execution_allowed" in payload and payload.get("execution_allowed") is not True:
        return False, "execution_not_allowed", 403

    # D8 management command contract.
    if command in {"MANAGE_ORDER", "PARTIAL_CLOSE", "SET_ORDER_SL", "SET_TRAILING_SL"}:
        order_id = str(payload.get("order_id", "")).strip()
        if not order_id:
            return False, "order_id required for management command", 400

    if command in {"MANAGE_ORDER", "PARTIAL_CLOSE"}:
        try:
            close_percent = float(payload.get("close_percent"))
        except (TypeError, ValueError):
            return False, "close_percent required for partial management command", 400
        if close_percent <= 0 or close_percent >= 100:
            return False, "close_percent must be >0 and <100", 400

    if command in {"MANAGE_ORDER", "SET_ORDER_SL"}:
        try:
            new_sl = float(payload.get("new_stop_loss"))
        except (TypeError, ValueError):
            return False, "new_stop_loss required for SL management command", 400
        if new_sl <= 0:
            return False, "new_stop_loss must be >0", 400

    if command == "SET_TRAILING_SL":
        try:
            trail = float(payload.get("trailing_distance"))
        except (TypeError, ValueError):
            return False, "trailing_distance required for SET_TRAILING_SL", 400
        if trail <= 0:
            return False, "trailing_distance must be >0", 400

    # Stale-age protection applies ONLY to new entries.
    if command == "KISS_V7_AOI_PORTFOLIO_PROTECT":
        direction = str(payload.get("direction", "")).strip().upper()
        if direction not in {"BUY", "SELL"}:
            return False, "direction BUY/SELL required for KISS V7 AOI protection", 400
        if str(payload.get("strategy_id", "")).strip() != "KISS_V7_5M":
            return False, "strategy_id KISS_V7_5M required for KISS V7 AOI protection", 400

    if command in {"KISS_V5_SIGNAL", "KISS_V6_SIGNAL", "KISS_V6_5M_SIGNAL", "KISS_V6_15M_SIGNAL", "KISS_V7_SIGNAL"}:
        direction = str(payload.get("direction", "")).strip().upper()
        if direction not in {"BUY", "SELL"}:
            return False, "BUY/SELL direction required for KISS V5/V6 signal", 400
        entry_command = str(payload.get("entry_command", "")).strip().upper()
        if entry_command not in {"PLACE_MARKET", "PLACE_PENDING"}:
            return False, "entry_command PLACE_MARKET/PLACE_PENDING required for KISS V5/V6 signal", 400
        if not str(payload.get("order_id", "")).strip():
            return False, "order_id required for KISS V5/V6 signal", 400
        try:
            if float(payload.get("entry_price", 0) or 0) <= 0:
                return False, "positive entry_price required for KISS V5/V6 signal", 400
        except (TypeError, ValueError):
            return False, "valid entry_price required for KISS V5/V6 signal", 400

        # v1.7.1/B019: pending KISS V5 entries are permitted only when the
        # transported entry_price exactly matches the EP printed on TradingView.
        if entry_command == "PLACE_PENDING":
            try:
                entry_price = float(payload.get("entry_price", 0) or 0)
                label_ep = float(payload.get("label_ep", 0) or 0)
            except (TypeError, ValueError):
                return False, "valid label_ep required for pending KISS V5/V6 signal", 400
            if label_ep <= 0:
                return False, "positive label_ep required for pending KISS V5/V6 signal", 400
            if abs(entry_price - label_ep) > 1e-6:
                return False, "pending KISS V5/V6 signal entry_price does not match label_ep", 400

    if command in {"KISS_V5_SIGNAL", "KISS_V6_SIGNAL", "KISS_V6_5M_SIGNAL", "KISS_V6_15M_SIGNAL", "KISS_V7_SIGNAL", "PLACE_PENDING", "PLACE_MARKET", "REPLACE_PENDING"}:
        try:
            event_time_ms = int(payload.get("event_time_ms"))
        except (TypeError, ValueError):
            return False, "event_time_ms required for new-entry command", 400

        try:
            stale_minutes = int(payload.get("stale_entry_age_minutes", DEFAULT_STALE_ENTRY_MINUTES))
        except (TypeError, ValueError):
            stale_minutes = DEFAULT_STALE_ENTRY_MINUTES
        stale_minutes = max(1, stale_minutes)

        age_ms = int(time.time() * 1000) - event_time_ms
        if age_ms > stale_minutes * 60_000:
            return False, f"stale new-entry command ({age_ms / 60000:.1f} min old)", 409

    return True, "", 200


def _insert_event(payload: dict) -> tuple[bool, int | None]:
    """
    Fast durable webhook ingress.

    TradingView waits only for local validation + one SQLite WAL commit.
    It never waits for MT4 polling, ACKs, dashboard work, or downstream execution.
    """
    now = int(time.time())
    compact = json.dumps(payload, separators=(",", ":"))

    # Short bounded retries cover a momentary SQLite writer collision without
    # allowing TradingView's webhook request to sit behind a 10-second DB timeout.
    deadline = time.monotonic() + 1.25
    while True:
        try:
            with closing(db(timeout=0.20)) as conn:
                try:
                    cur = conn.execute(
                        """
                        INSERT INTO events(
                            event_id,payload,received_at,system,automation_version,strategy_version,
                            command,direction,reason,trading_period_id,campaign_id,ct_campaign_id,
                            order_id,event_time_ms,close_percent,trailing_distance,new_stop_loss
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            str(payload.get("event_id", "")), compact, now,
                            str(payload.get("system", "")), str(payload.get("automation_version", "")),
                            str(payload.get("strategy_version", "")), str(payload.get("command", "")),
                            str(payload.get("direction", "")), str(payload.get("reason", "")),
                            str(payload.get("trading_period_id", "")), str(payload.get("campaign_id", "")),
                            str(payload.get("ct_campaign_id", "")), str(payload.get("order_id", "")),
                            int(payload.get("event_time_ms", 0) or 0),
                            float(payload.get("close_percent", 0) or 0),
                            float(payload.get("trailing_distance", 0) or 0),
                            float(payload.get("new_stop_loss", 0) or 0),
                        ),
                    )
                    conn.commit()
                    return True, int(cur.lastrowid)
                except sqlite3.IntegrityError:
                    # Duplicate event_id is an idempotent success.
                    return False, None
        except sqlite3.OperationalError as exc:
            if time.monotonic() >= deadline:
                raise exc
            time.sleep(0.015)



def _parse_event_datetime(value: str) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        # Forex Factory weekly JSON uses ISO-8601 with an explicit offset.
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _normalise_ff_events(raw_events: object) -> list[dict]:
    if not isinstance(raw_events, list):
        return []
    out: list[dict] = []
    for item in raw_events:
        if not isinstance(item, dict):
            continue
        when = _parse_event_datetime(str(item.get("date", "")))
        if when is None:
            continue
        out.append({
            "title": str(item.get("title", "")).strip(),
            "country": str(item.get("country", "")).strip().upper(),
            "impact": str(item.get("impact", "")).strip().title(),
            "when_utc": when,
        })
    out.sort(key=lambda e: e["when_utc"])
    return out


def _refresh_news_cache(force: bool = False) -> tuple[list[dict], bool, str, int | None]:
    global _news_cache_events, _news_cache_fetched_at, _news_cache_last_error

    now = time.time()
    with _news_cache_lock:
        fresh = (
            _news_cache_events
            and _news_cache_fetched_at > 0
            and now - _news_cache_fetched_at < max(60, NEWS_CACHE_TTL_SECONDS)
        )
        if fresh and not force:
            return list(_news_cache_events), True, "", int(now - _news_cache_fetched_at)

        try:
            req = urllib.request.Request(
                FF_CALENDAR_URL,
                headers={
                    "User-Agent": "ATOS-Relay/1.6.3",
                    "Accept": "application/json",
                },
                method="GET",
            )
            with urllib.request.urlopen(req, timeout=NEWS_HTTP_TIMEOUT_SECONDS) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            parsed = _normalise_ff_events(payload)
            if not parsed:
                raise ValueError("calendar returned no parseable events")
            _news_cache_events = parsed
            _news_cache_fetched_at = now
            _news_cache_last_error = ""
            return list(_news_cache_events), True, "", 0
        except Exception as exc:
            _news_cache_last_error = f"{type(exc).__name__}: {exc}"
            # Keep serving the last successful weekly cache if a refresh fails.
            if _news_cache_events and _news_cache_fetched_at > 0:
                return (
                    list(_news_cache_events),
                    True,
                    _news_cache_last_error,
                    int(now - _news_cache_fetched_at),
                )
            return [], False, _news_cache_last_error, None


def _news_protection_snapshot(before_minutes: int, after_minutes: int) -> dict:
    events, calendar_ok, refresh_error, cache_age = _refresh_news_cache()
    now_utc = datetime.now(timezone.utc)
    before = max(0, min(240, int(before_minutes)))
    after = max(0, min(240, int(after_minutes)))

    candidates = [
        e for e in events
        if e["country"] == "USD" and e["impact"] == "High"
    ]

    active_event = None
    next_event = None
    for e in candidates:
        start = e["when_utc"].timestamp() - before * 60
        end = e["when_utc"].timestamp() + after * 60
        now_ts = now_utc.timestamp()
        if start <= now_ts <= end:
            active_event = e
            break
        if e["when_utc"] > now_utc and next_event is None:
            next_event = e

    chosen = active_event or next_event
    result = {
        "ok": True,
        "calendar_ok": calendar_ok,
        "active": active_event is not None,
        "currency": "USD",
        "impact": "High",
        "minutes_before": before,
        "minutes_after": after,
        "source": FF_CALENDAR_URL,
        "cache_age_seconds": cache_age,
        "refresh_error": refresh_error or None,
        "server_time_epoch": int(now_utc.timestamp()),
    }

    if chosen is not None:
        when = chosen["when_utc"]
        result.update({
            "event_title": chosen["title"],
            "event_time_epoch": int(when.timestamp()),
            "event_time_utc": when.isoformat(),
            "seconds_to_event": int(when.timestamp() - now_utc.timestamp()),
            "window_start_epoch": int(when.timestamp() - before * 60),
            "window_end_epoch": int(when.timestamp() + after * 60),
        })
    else:
        result.update({
            "event_title": None,
            "event_time_epoch": 0,
            "event_time_utc": None,
            "seconds_to_event": None,
            "window_start_epoch": 0,
            "window_end_epoch": 0,
        })
    return result


@app.get("/atos/news-protection")
def news_protection():
    if not _authorised():
        return jsonify({"ok": False, "error": "unauthorised"}), 401
    try:
        before = int(request.args.get("before", "30"))
        after = int(request.args.get("after", "30"))
    except ValueError:
        return jsonify({"ok": False, "error": "before/after must be integers"}), 400

    try:
        return jsonify(_news_protection_snapshot(before, after)), 200
    except Exception as exc:
        return jsonify({
            "ok": False,
            "calendar_ok": False,
            "active": False,
            "error": str(exc),
        }), 500


@app.get("/health")
@app.get("/atos/health")
def health():
    try:
        raw_consumer = request.args.get("consumer_id")
        consumer_id = None
        if raw_consumer:
            try:
                consumer_id = _consumer_id(raw_consumer)
            except ValueError as exc:
                return jsonify({"ok": False, "error": str(exc)}), 400
            _touch_consumer(consumer_id)

        with closing(db()) as conn:
            total = conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()["n"]
            rejected = conn.execute("SELECT COUNT(*) AS n FROM rejected_events").fetchone()["n"]
            consumer_count = conn.execute("SELECT COUNT(*) AS n FROM consumers").fetchone()["n"]
            latest = conn.execute(
                "SELECT seq,event_id,command,received_at FROM events ORDER BY seq DESC LIMIT 1"
            ).fetchone()
            last_poll = _get_state(conn, "last_poll_at", "")
            c_row = None
            if consumer_id:
                c_row = conn.execute(
                    "SELECT last_seen_at,last_poll_at,last_poll_after,last_ack_at,last_ack_event_id,last_ack_status "
                    "FROM consumers WHERE consumer_id=?",
                    (consumer_id,),
                ).fetchone()
        return jsonify({
            "ok": True,
            "service": SERVICE_NAME,
            "relay_version": RELAY_VERSION,
            "database": "OK",
            "events": total,
            "rejected_events": rejected,
            "latest_seq": latest["seq"] if latest else 0,
            "latest_event_id": latest["event_id"] if latest else None,
            "latest_command": latest["command"] if latest else None,
            "last_mt4_poll_at": int(last_poll) if last_poll else None,
            "consumer_count": consumer_count,
            "consumer_id": consumer_id,
            "consumer_last_poll_after": c_row["last_poll_after"] if c_row else None,
            "consumer_last_ack_event_id": c_row["last_ack_event_id"] if c_row else None,
            "consumer_last_ack_status": c_row["last_ack_status"] if c_row else None,
            "server_time": int(time.time()),
        })
    except Exception as exc:
        return jsonify({"ok": False, "service": SERVICE_NAME, "error": str(exc)}), 500


@app.post("/tradingview")
@app.post("/atos/event")
def tradingview():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "JSON body required"}), 400

    if not _authorised(payload):
        _record_rejection(payload, "unauthorised")
        return jsonify({"ok": False, "error": "unauthorised"}), 401

    valid, reason, code = _validate_event(payload)
    if not valid:
        _record_rejection(payload, reason)
        return jsonify({"ok": False, "error": reason, "event_id": payload.get("event_id")}), code

    try:
        inserted, seq = _insert_event(payload)
    except sqlite3.OperationalError:
        # Fail fast rather than exceeding TradingView's webhook timeout.
        # TradingView will visibly report delivery failure instead of an ambiguous long hang.
        return jsonify({
            "ok": False,
            "error": "relay database temporarily busy",
            "event_id": payload.get("event_id"),
        }), 503

    return jsonify({
        "ok": True,
        "inserted": inserted,
        "duplicate": not inserted,
        "event_id": payload.get("event_id"),
        "seq": seq,
    })


@app.get("/events")
@app.get("/atos/next")
def events():
    if not _authorised():
        return "UNAUTHORISED", 401

    after = request.args.get("after", "0")
    try:
        after_seq = max(0, int(after))
    except ValueError:
        return "INVALID_AFTER", 400

    try:
        consumer_id = _consumer_id(request.args.get("consumer_id"))
    except ValueError as exc:
        return str(exc), 400

    # v1.5.4: non-critical poll telemetry is throttled and never blocks event delivery.
    _record_poll_telemetry(consumer_id, after_seq)

    with closing(db(timeout=0.5)) as conn:
        rows = conn.execute(
            "SELECT seq,payload FROM events WHERE seq>? ORDER BY seq ASC LIMIT ?",
            (after_seq, MAX_BATCH),
        ).fetchall()

    # Backward compatible with the existing ATOS MT4 EA parser:
    # <sequence>|<compact-json>\n
    body = "\n".join(f"{row['seq']}|{row['payload']}" for row in rows)
    return body, 200, {"Content-Type": "text/plain; charset=utf-8"}


@app.post("/atos/ack")
def ack():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "JSON body required"}), 400
    if not _authorised(payload):
        return jsonify({"ok": False, "error": "unauthorised"}), 401

    event_id = str(payload.get("event_id", "")).strip()
    if not event_id:
        return jsonify({"ok": False, "error": "event_id required"}), 400

    try:
        consumer_id = _consumer_id(str(payload.get("consumer_id", "default")))
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    status = str(payload.get("status", "PROCESSED"))[:80]
    detail = str(payload.get("detail", ""))[:1000]
    now = int(time.time())
    with closing(db()) as conn:
        exists = conn.execute("SELECT 1 FROM events WHERE event_id=?", (event_id,)).fetchone()
        if not exists:
            return jsonify({"ok": False, "error": "event_id not found"}), 404

        # Legacy aggregate ACK fields remain for backward compatibility.
        conn.execute(
            "UPDATE events SET acked_at=?,ack_status=?,ack_detail=? WHERE event_id=?",
            (now, status, detail, event_id),
        )
        conn.execute(
            """
            INSERT INTO consumer_acks(consumer_id,event_id,acked_at,status,detail)
            VALUES(?,?,?,?,?)
            ON CONFLICT(consumer_id,event_id) DO UPDATE SET
                acked_at=excluded.acked_at,
                status=excluded.status,
                detail=excluded.detail
            """,
            (consumer_id, event_id, now, status, detail),
        )
        # Update consumer ACK metadata in the SAME transaction to reduce writer contention.
        conn.execute(
            """
            INSERT INTO consumers(
                consumer_id,first_seen_at,last_seen_at,last_poll_at,last_poll_after,
                last_ack_at,last_ack_event_id,last_ack_status
            ) VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(consumer_id) DO UPDATE SET
                last_seen_at=excluded.last_seen_at,
                last_ack_at=excluded.last_ack_at,
                last_ack_event_id=excluded.last_ack_event_id,
                last_ack_status=excluded.last_ack_status
            """,
            (consumer_id, now, now, None, 0, now, event_id, status),
        )
        conn.commit()

    return jsonify({
        "ok": True,
        "consumer_id": consumer_id,
        "event_id": event_id,
        "status": status,
    })


@app.get("/dashboard")
def dashboard():
    # Read-only operational dashboard. No secret required by default; it contains no account data.
    # Set ATOS_DASHBOARD_SECRET_REQUIRED=1 if you want it protected.
    if os.environ.get("ATOS_DASHBOARD_SECRET_REQUIRED", "0") == "1" and not _authorised():
        return "UNAUTHORISED", 401

    now = int(time.time())
    with closing(db()) as conn:
        total = conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()["n"]
        rejected = conn.execute("SELECT COUNT(*) AS n FROM rejected_events").fetchone()["n"]
        acked = conn.execute("SELECT COUNT(*) AS n FROM events WHERE acked_at IS NOT NULL").fetchone()["n"]
        consumers = conn.execute(
            "SELECT consumer_id,last_seen_at,last_poll_at,last_poll_after,last_ack_at,last_ack_event_id,last_ack_status "
            "FROM consumers ORDER BY consumer_id"
        ).fetchall()
        latest = conn.execute(
            "SELECT seq,event_id,command,direction,reason,campaign_id,order_id,received_at "
            "FROM events ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        last_poll_raw = _get_state(conn, "last_poll_at", "")
        recent = conn.execute(
            "SELECT seq,event_id,command,direction,reason,received_at FROM events ORDER BY seq DESC LIMIT 12"
        ).fetchall()

    last_poll = int(last_poll_raw) if last_poll_raw else 0
    poll_age = now - last_poll if last_poll else None
    poll_state = "CONNECTED" if poll_age is not None and poll_age <= 15 else "WAITING"

    def ts(v: int | None) -> str:
        if not v:
            return "—"
        return datetime.fromtimestamp(v, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    consumer_rows_html = "".join(
        "<tr>"
        f"<td class='mono'>{html.escape(c['consumer_id'])}</td>"
        f"<td>{ts(c['last_seen_at'])}</td>"
        f"<td>{ts(c['last_poll_at'])}</td>"
        f"<td>{c['last_poll_after']}</td>"
        f"<td>{html.escape(c['last_ack_status'] or '—')}</td>"
        f"<td class='mono'>{html.escape(c['last_ack_event_id'] or '—')}</td>"
        "</tr>" for c in consumers
    )

    rows_html = "".join(
        "<tr>"
        f"<td>{r['seq']}</td>"
        f"<td>{html.escape(r['command'] or '')}</td>"
        f"<td>{html.escape(r['direction'] or '')}</td>"
        f"<td>{html.escape(r['reason'] or '')}</td>"
        f"<td class='mono'>{html.escape(r['event_id'] or '')}</td>"
        f"<td>{ts(r['received_at'])}</td>"
        "</tr>" for r in recent
    )

    latest_command = latest["command"] if latest else "—"
    latest_event = latest["event_id"] if latest else "—"
    latest_seq = latest["seq"] if latest else 0
    latest_received = ts(latest["received_at"]) if latest else "—"
    poll_text = f"{poll_age}s ago" if poll_age is not None else "Never"

    page = f"""<!doctype html>
<html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>ATOS Relay</title>
<style>
body{{font-family:Inter,Arial,sans-serif;background:#0e1621;color:#e9eef5;margin:0;padding:24px}}
.wrap{{max-width:1200px;margin:auto}} h1{{margin:0 0 6px}} .muted{{color:#9ba9b8}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px;margin:22px 0}}
.card{{background:#162231;border:1px solid #26374a;border-radius:12px;padding:16px}}
.value{{font-size:24px;font-weight:700;margin-top:8px;word-break:break-word}} .ok{{color:#69d391}} .warn{{color:#f0c36a}}
table{{width:100%;border-collapse:collapse;background:#162231;border-radius:12px;overflow:hidden}}
th,td{{padding:10px;border-bottom:1px solid #26374a;text-align:left;font-size:13px}} th{{color:#9ba9b8}} .mono{{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px}}
</style></head><body><div class='wrap'>
<h1>ATOS Relay v{RELAY_VERSION}</h1><div class='muted'>Transport, validation and audit only — no strategy logic.</div>
<div class='grid'>
<div class='card'><div class='muted'>Relay</div><div class='value ok'>RUNNING</div></div>
<div class='card'><div class='muted'>MT4 Poll</div><div class='value {'ok' if poll_state=='CONNECTED' else 'warn'}'>{poll_state}</div><div class='muted'>{poll_text}</div></div>
<div class='card'><div class='muted'>Latest Seq</div><div class='value'>{latest_seq}</div></div>
<div class='card'><div class='muted'>Latest Command</div><div class='value'>{html.escape(latest_command or '—')}</div></div>
<div class='card'><div class='muted'>Accepted Events</div><div class='value'>{total}</div></div>
<div class='card'><div class='muted'>Rejected Events</div><div class='value'>{rejected}</div></div>
<div class='card'><div class='muted'>Acknowledged</div><div class='value'>{acked}</div></div>
<div class='card'><div class='muted'>MT4 Consumers</div><div class='value'>{len(consumers)}</div></div>
<div class='card'><div class='muted'>Latest Received</div><div class='value' style='font-size:15px'>{latest_received}</div></div>
</div>
<div class='card' style='margin-bottom:12px'><div class='muted'>Latest Event ID</div><div class='mono' style='margin-top:8px'>{html.escape(latest_event or '—')}</div></div>
<h2>MT4 Consumers</h2>
<table><thead><tr><th>Consumer</th><th>Last Seen</th><th>Last Poll</th><th>Cursor</th><th>Last ACK</th><th>Last Event</th></tr></thead><tbody>{consumer_rows_html}</tbody></table>
<h2>Recent Events</h2><table><thead><tr><th>Seq</th><th>Command</th><th>Side</th><th>Reason</th><th>Event ID</th><th>Received</th></tr></thead><tbody>{rows_html}</tbody></table>
</div></body></html>"""
    return Response(page, mimetype="text/html")


# Initialize on import so Gunicorn/Render deployments are ready immediately.
init_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
