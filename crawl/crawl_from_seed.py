import os
import ray
from dotenv import load_dotenv
from typing import List
from telethon.sync import TelegramClient
from crawl.tasks import crawl_channel_remote
from db.insert import insert_message
from db.schema import init_db
from db.graph import upsert_channel, get_all_channels, update_last_msg_id
from configs.constants import (
    DEFAULT_SESSION_NAME,
    DEFAULT_LIMIT,
    BASE_PRIORITY,
    DB_PATH,
    SEED_FILE,
    DEBUG,
)

load_dotenv()
ray.init(ignore_reinit_error=True)
init_db()

if os.path.exists(SEED_FILE):
    with open(SEED_FILE, "r") as f:
        for handle in f.readlines():
            handle = handle.strip()
            if handle and not handle.startswith("@"):
                handle = f"@{handle}"
            upsert_channel(handle, visited=False, priority=BASE_PRIORITY)

all_channels: List[str] = get_all_channels()
futures = [
    crawl_channel_remote.remote(DEFAULT_SESSION_NAME, channel, DEFAULT_LIMIT)
    for channel in all_channels
]

results = ray.get(futures)

for channel, messages, max_seen, _ in results:
    if messages:
        from sqlite3 import connect
        conn = connect(DB_PATH)
        with conn:
            for msg in messages:
                insert_message(conn, msg)
        conn.close()
    if max_seen:
        update_last_msg_id(channel, max_seen)

if DEBUG:
    print(f"DB path: {DB_PATH}")
