# ATOS Relay v1.2.1 — V6 command-contract compatibility
from __future__ import annotations

import html
import json
import os
import re
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timezone

from flask import Flask, Response, jsonify, request

SERVICE_NAME = "ATOS Relay"
RELAY_VERSION = "1.5.0"
EXPECTED_SYSTEM = "ATOS"
EXPECTED_AUTOMATION_VERSION = "1.0"

APP_SECRET = os.environ.get("ATOS_SECRET", os.environ.get("ALCENT_SECRET", "CHANGE_ME"))
DB_PATH = os.environ.get("ATOS_DB", os.environ.get("ALCENT_DB", "atos_events.db"))
MAX_BATCH = int(os.environ.get("ATOS_MAX_BATCH", "100"))
DEFAULT_STALE_ENTRY_MINUTES = int(os.environ.get("ATOS_STALE_ENTRY_MINUTES", "5"))

ALLOWED_COMMANDS = {
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

    # V6 protective/logical-order commands. Transport only; no strategy logic here.
    "V6_INVALIDATE_ORDER",
    "V6_SET_LOGICAL_TP",
}

app = Flask(__name__)

CONSUMER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def _consumer_id(raw: str | None) -> str:
    value = (raw or "").strip()
    if not value:
        return "default"
    if not CONSUMER_ID_RE.fullmatch(value):
        raise ValueError("consumer_id must be 1-64 chars: A-Z a-z 0-9 _ -")
    return value


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _ensure_column(conn: sqlite3.Connection, table: str, name: str, definition: str) -> None:
    if name not in _column_names(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")


def init_db() -> None:
    with closing(db()) as conn:
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
    if command in {"PLACE_PENDING", "PLACE_MARKET", "REPLACE_PENDING"}:
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
    now = int(time.time())
    compact = json.dumps(payload, separators=(",", ":"))
    with closing(db()) as conn:
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
            return False, None


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

    inserted, seq = _insert_event(payload)
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

    # Legacy global state retained for backward dashboard compatibility.
    _set_state("last_poll_at", str(int(time.time())))
    _set_state("last_poll_after", str(after_seq))
    _touch_consumer(consumer_id, poll_after=after_seq)

    with closing(db()) as conn:
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
        conn.commit()

    _touch_consumer(
        consumer_id,
        ack_event_id=event_id,
        ack_status=status,
    )
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
