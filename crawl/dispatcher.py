from typing import List
import ray
from crawl.tasks import crawl_channel_remote

def dispatch(session_name: str, channel: str, limit: int, queue_actor: ray.actor.ActorHandle) -> ray.ObjectRef:
    return crawl_channel_remote.remote(session_name, channel, limit, queue_actor)
