import sqlite3
from typing import Dict
from configs.constants import DB_PATH

def insert_message(conn: sqlite3.Connection, msg: Dict) -> None:
    conn.execute("""
        INSERT OR IGNORE INTO messages (
            msg_id, channel, text, date, url, session_id, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        msg["msg_id"], msg["channel"], msg["text"], msg["date"],
        msg["url"], msg["session_id"], msg["raw_json"]
    ))
