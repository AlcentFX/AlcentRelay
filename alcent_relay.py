from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import closing
from flask import Flask, jsonify, request

APP_SECRET = os.environ.get("ALCENT_SECRET", "CHANGE_ME")
DB_PATH = os.environ.get("ALCENT_DB", "alcent_events.db")

app = Flask(__name__)


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with closing(db()) as conn:
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
        conn.commit()


@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "alcent-relay"})


@app.post("/tradingview")
def tradingview():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "JSON body required"}), 400

    if payload.get("secret") != APP_SECRET:
        return jsonify({"ok": False, "error": "unauthorised"}), 401

    event_id = str(payload.get("event_id", "")).strip()
    if not event_id:
        return jsonify({"ok": False, "error": "event_id required"}), 400

    with closing(db()) as conn:
        try:
            conn.execute(
                "INSERT INTO events(event_id,payload,received_at) VALUES(?,?,?)",
                (event_id, json.dumps(payload, separators=(",", ":")), int(time.time())),
            )
            conn.commit()
            inserted = True
        except sqlite3.IntegrityError:
            inserted = False

    return jsonify({"ok": True, "inserted": inserted, "event_id": event_id})


@app.get("/events")
def events():
    if request.args.get("secret") != APP_SECRET:
        return "UNAUTHORISED", 401

    after = request.args.get("after", "0")
    try:
        after_seq = max(0, int(after))
    except ValueError:
        return "INVALID_AFTER", 400

    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT seq,payload FROM events WHERE seq>? ORDER BY seq ASC LIMIT 100",
            (after_seq,),
        ).fetchall()

    # MT4-friendly response: one compact JSON object per line prefixed by seq.
    return "\n".join(f"{row['seq']}|{row['payload']}" for row in rows), 200, {
        "Content-Type": "text/plain; charset=utf-8"
    }


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
