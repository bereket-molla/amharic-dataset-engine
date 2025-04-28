import os
import sys
import time
import sqlite3
from typing import List, Optional

from dotenv import load_dotenv
from telethon.sync import TelegramClient
from telethon.errors import SessionPasswordNeededError

from configs.constants import SESSION_DIR, DEFAULT_SESSION_NAME

load_dotenv()
API_ID = os.getenv("TG_API_ID")
API_HASH = os.getenv("TG_API_HASH")

if not API_ID or not API_HASH:
    print("API credentials missing in .env.")
    sys.exit(1)

API_ID = int(API_ID)

def _authorise(client: TelegramClient, name: str) -> None:
    phone = input("enter your phone number (with +): ").strip()
    client.send_code_request(phone)
    code = input("enter the code you received: ").strip()

    try:
        client.sign_in(phone=phone, code=code)
    except SessionPasswordNeededError:
        password = input("two-step-verification password: ").strip()
        client.sign_in(password=password)
    print("authorised!")

def load_client(
    session_name: Optional[str] = None,
    retries: int = 5,
    backoff: float = 2.0,
) -> TelegramClient:
    name = session_name or DEFAULT_SESSION_NAME
    os.makedirs(SESSION_DIR, exist_ok=True)
    session_path = os.path.join(SESSION_DIR, name)

    for attempt in range(retries):
        try:
            client = TelegramClient(session_path, API_ID, API_HASH)
            client.connect()

            if not client.is_user_authorized():
                _authorise(client, name)

            return client

        except sqlite3.OperationalError as e:
            if "database is locked" not in str(e):
                raise

            if attempt == retries - 1:
                raise

            wait = backoff * (attempt + 1)
            print(f"'{name}.session' locked – retrying in {wait}s …")
            time.sleep(wait)

        except Exception as e:
            print(f"Failed to load session '{name}': {e}")
            raise

def load_all_clients() -> List[TelegramClient]:
    if not os.path.isdir(SESSION_DIR):
        return []

    clients: List[TelegramClient] = []
    for fname in os.listdir(SESSION_DIR):
        if fname.endswith(".session"):
            clients.append(load_client(fname[:-8]))
    return clients
