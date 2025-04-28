from typing import Dict

def to_message_dict(
    msg_id: int,
    channel: str,
    text: str,
    date: str,
    url: str,
    session_id: str,
    raw_json: str
) -> Dict:
    return {
        "msg_id": msg_id,
        "channel": channel,
        "text": text,
        "date": date,
        "url": url,
        "session_id": session_id,
        "raw_json": raw_json
    }
