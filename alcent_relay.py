# ATOS Relay v1.0.2 — DEFENSE command whitelist update
from __future__ import annotations

import html
import json
import os
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timezone

from flask import Flask, Response, jsonify, request

SERVICE_NAME = "ATOS Relay"
RELAY_VERSION = "1.0"
EXPECTED_SYSTEM = "ATOS"
EXPECTED_AUTOMATION_VERSION = "1.0"

APP_SECRET = os.environ.get("ATOS_SECRET", os.environ.get("ALCENT_SECRET", "CHANGE_ME"))
DB_PATH = os.environ.get("ATOS_DB", os.environ.get("ALCENT_DB", "atos_events.db"))
MAX_BATCH = int(os.environ.get("ATOS_MAX_BATCH", "100"))
DEFAULT_STALE_ENTRY_MINUTES = int(os.environ.get("ATOS_STALE_ENTRY_MINUTES", "5"))

ALLOWED_COMMANDS = {
    "PLACE_PENDING",
    "CANCEL_ORDER",
    "CANCEL_BUYS",
    "CANCEL_SELLS",
    "CLOSE_ORDER",
    "CLOSE_BUYS",
    "CLOSE_SELLS",
    "DEFEND_BUYS",
    "DEFEND_SELLS",
    "MODIFY_SL",
    "MODIFY_TP",
    "MODIFY_SLTP",
    "CANCEL_CT_ORDER",
    "CLOSE_CT_ORDER",
    "MODIFY_CT_SL",
    "MODIFY_CT_TP",
}

app = Flask(__name__)


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

    # Role-gating defence in depth. Current EXECUTION alerts send true.
    if "execution_allowed" in payload and payload.get("execution_allowed") is not True:
        return False, "execution_not_allowed", 403

    # Stale-age protection applies ONLY to new entries.
    if command == "PLACE_PENDING":
        try:
            event_time_ms = int(payload.get("event_time_ms"))
        except (TypeError, ValueError):
            return False, "event_time_ms required for PLACE_PENDING", 400

        try:
            stale_minutes = int(payload.get("stale_entry_age_minutes", DEFAULT_STALE_ENTRY_MINUTES))
        except (TypeError, ValueError):
            stale_minutes = DEFAULT_STALE_ENTRY_MINUTES
        stale_minutes = max(1, stale_minutes)

        age_ms = int(time.time() * 1000) - event_time_ms
        if age_ms > stale_minutes * 60_000:
            return False, f"stale PLACE_PENDING ({age_ms / 60000:.1f} min old)", 409

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
                    order_id,event_time_ms
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(payload.get("event_id", "")), compact, now,
                    str(payload.get("system", "")), str(payload.get("automation_version", "")),
                    str(payload.get("strategy_version", "")), str(payload.get("command", "")),
                    str(payload.get("direction", "")), str(payload.get("reason", "")),
                    str(payload.get("trading_period_id", "")), str(payload.get("campaign_id", "")),
                    str(payload.get("ct_campaign_id", "")), str(payload.get("order_id", "")),
                    int(payload.get("event_time_ms", 0) or 0),
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
        with closing(db()) as conn:
            total = conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()["n"]
            rejected = conn.execute("SELECT COUNT(*) AS n FROM rejected_events").fetchone()["n"]
            latest = conn.execute(
                "SELECT seq,event_id,command,received_at FROM events ORDER BY seq DESC LIMIT 1"
            ).fetchone()
            last_poll = _get_state(conn, "last_poll_at", "")
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

    _set_state("last_poll_at", str(int(time.time())))
    _set_state("last_poll_after", str(after_seq))

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

    status = str(payload.get("status", "PROCESSED"))[:80]
    detail = str(payload.get("detail", ""))[:1000]
    with closing(db()) as conn:
        cur = conn.execute(
            "UPDATE events SET acked_at=?,ack_status=?,ack_detail=? WHERE event_id=?",
            (int(time.time()), status, detail, event_id),
        )
        conn.commit()
    if cur.rowcount == 0:
        return jsonify({"ok": False, "error": "event_id not found"}), 404
    return jsonify({"ok": True, "event_id": event_id, "status": status})


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
<div class='card'><div class='muted'>Latest Received</div><div class='value' style='font-size:15px'>{latest_received}</div></div>
</div>
<div class='card' style='margin-bottom:12px'><div class='muted'>Latest Event ID</div><div class='mono' style='margin-top:8px'>{html.escape(latest_event or '—')}</div></div>
<h2>Recent Events</h2><table><thead><tr><th>Seq</th><th>Command</th><th>Side</th><th>Reason</th><th>Event ID</th><th>Received</th></tr></thead><tbody>{rows_html}</tbody></table>
</div></body></html>"""
    return Response(page, mimetype="text/html")


# Initialize on import so Gunicorn/Render deployments are ready immediately.
init_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
