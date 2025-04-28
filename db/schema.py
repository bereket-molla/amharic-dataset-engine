import sqlite3
import os
from configs.constants import DB_PATH

def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        msg_id INTEGER NOT NULL,
        channel TEXT NOT NULL,
        text TEXT NOT NULL,
        date TEXT,
        url TEXT,
        session_id TEXT,
        raw_json TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(msg_id, channel)
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS channel_graph (
        handle TEXT PRIMARY KEY,
        visited BOOLEAN,
        priority REAL,
        last_visited TEXT,
        last_msg_id INTEGER,
        neighbors TEXT
    )
    """)

    conn.commit()
    conn.close()
