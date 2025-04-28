import os
from typing import List, Optional

import ray
from sqlite3 import connect

from configs.constants import (
    BASE_PRIORITY,
    DEFAULT_SESSION_NAME,
    DEFAULT_LIMIT,
    DB_PATH,
    DEBUG,
    SESSION_DIR,
)
from db.schema import init_db
from db.graph import get_all_channels, update_last_msg_id
from db.insert import insert_message
from crawl.scheduler import ChannelQueue
from crawl.dispatcher import dispatch


def _discover_session_names() -> List[str]:
    if not os.path.isdir(SESSION_DIR):
        return [DEFAULT_SESSION_NAME]

    names = [
        fname[:-8]
        for fname in os.listdir(SESSION_DIR)
        if fname.endswith(".session")
    ]
    return names or [DEFAULT_SESSION_NAME]


def run_parallel_crawl(
    limit: int = DEFAULT_LIMIT,
    channels: Optional[List[str]] = None,
    max_workers: int = 8,
) -> None:
    init_db()
    ray.init(ignore_reinit_error=True)

    session_names = _discover_session_names()
    num_sessions = len(session_names)
    max_workers = min(max_workers, num_sessions)

    queue = ChannelQueue.remote()

    for handle in (channels or get_all_channels()):
        ray.get(queue.push.remote(handle, BASE_PRIORITY))

    pending_tasks: List[ray.ObjectRef] = []

    while True:
        while len(pending_tasks) < max_workers:
            popped = ray.get(queue.pop.remote())
            if popped is None:
                break

            handle, prio = popped
            sess_name = session_names[len(pending_tasks) % num_sessions]

            pending_tasks.append(
                dispatch(sess_name, handle, limit, queue_actor=queue)
            )

        if not pending_tasks:
            break

        done, pending_tasks = ray.wait(pending_tasks, num_returns=1)
        channel, messages, max_id, should_requeue = ray.get(done[0])

        if messages:
            with connect(DB_PATH) as conn:
                for m in messages:
                    insert_message(conn, m)
            if max_id:
                update_last_msg_id(channel, max_id)

        if should_requeue:
            queue.push.remote(channel, prio * 0.5)

    if DEBUG:
        print(f"Crawl complete. DB: {DB_PATH}")


if __name__ == "__main__":
    run_parallel_crawl()
