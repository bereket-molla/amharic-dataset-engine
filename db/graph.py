import sqlite3
import json
from typing import List, Optional
from configs.constants import DB_PATH


def upsert_channel(
    handle: str,
    visited: bool = False,
    priority: float = 0.5,
    last_msg_id: Optional[int] = None
) -> None:
    conn = sqlite3.connect(DB_PATH)
    with conn:
        if last_msg_id is not None:
            conn.execute("""
                INSERT INTO channel_graph (handle, visited, priority, last_visited, last_msg_id, neighbors)
                VALUES (?, ?, ?, NULL, ?, ?)
                ON CONFLICT(handle) DO UPDATE SET
                    visited = excluded.visited,
                    priority = excluded.priority,
                    last_msg_id = excluded.last_msg_id
            """, (handle, visited, priority, last_msg_id, json.dumps([])))
        else:
            conn.execute("""
                INSERT INTO channel_graph (handle, visited, priority, last_visited, neighbors)
                VALUES (?, ?, ?, NULL, ?)
                ON CONFLICT(handle) DO UPDATE SET
                    visited = excluded.visited,
                    priority = excluded.priority
            """, (handle, visited, priority, json.dumps([])))
    conn.close()


def get_all_channels() -> List[str]:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute("SELECT handle FROM channel_graph")
    channels = [row[0] for row in cursor.fetchall()]
    conn.close()
    return channels


def get_last_msg_id(handle: str) -> Optional[int]:
    if not handle.startswith("@"):
        handle = f"@{handle}"
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute("SELECT last_msg_id FROM channel_graph WHERE handle = ?", (handle,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None


def update_last_msg_id(handle: str, last_msg_id: int) -> None:
    if not handle.startswith("@"):
        handle = f"@{handle}"
    conn = sqlite3.connect(DB_PATH)
    with conn:
        conn.execute("""
            UPDATE channel_graph
            SET last_msg_id = ?
            WHERE handle = ?
        """, (last_msg_id, handle))
    conn.close()
