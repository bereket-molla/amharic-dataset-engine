from __future__ import annotations
import re, time
from typing import List, Optional, Tuple, Set

import ray
from telethon.errors import FloodWaitError, UsernameInvalidError, UsernameNotOccupiedError
from telethon.sync import TelegramClient

from configs.constants import BASE_PRIORITY
from db.graph import get_last_msg_id, upsert_channel
from models.message_model import to_message_dict
from utils.session_manager import load_client
from utils.text_cleaner import clean_text, contains_fidel

MENTION_RE = re.compile(r"(?:t\.me/|@)([A-Za-z0-9_]{5,})")

def _extract_handles(msg) -> Set[str]:
    text = msg.message or ""
    handles = {"@" + h.lower() for h in MENTION_RE.findall(text)}
    if msg.forward and msg.forward.chat and msg.forward.chat.username:
        handles.add("@" + msg.forward.chat.username.lower())
    return handles

@ray.remote
def crawl_channel_remote(
    session_name: str,
    channel: str,
    limit: int,
    queue_actor: Optional[ray.actor.ActorHandle] = None
) -> Tuple[str, List[dict], Optional[int], bool]:
    channel = channel.lower()
    client: TelegramClient = load_client(session_name)

    try:
        last_seen = get_last_msg_id(channel) or 0
        messages, max_seen = [], None

        for msg in client.iter_messages(
                channel,
                limit=limit,
                offset_id=last_seen,
                reverse=True):
            if not msg.message:
                continue
            clean = clean_text(msg.message)
            if not clean or not contains_fidel(clean):
                continue

            messages.append(
                to_message_dict(
                    msg_id=msg.id,
                    channel=channel,
                    text=clean,
                    date=msg.date.isoformat(),
                    url=f"https://t.me/{channel.strip('@')}/{msg.id}",
                    session_id=session_name,
                    raw_json=str(msg.to_dict())
                )
            )
            max_seen = msg.id

            for h in _extract_handles(msg):
                upsert_channel(h, priority=BASE_PRIORITY)
                if queue_actor:
                    queue_actor.push.remote(h, BASE_PRIORITY)

        should_requeue = bool(messages)
        return channel, messages, max_seen, should_requeue

    except (UsernameInvalidError, UsernameNotOccupiedError):
        return channel, [], last_seen, False

    except FloodWaitError as e:
        wait = e.seconds + 1
        time.sleep(wait)
        return crawl_channel_remote(session_name, channel, limit, queue_actor)

    except Exception:
        return channel, [], last_seen, False

    finally:
        client.disconnect()
