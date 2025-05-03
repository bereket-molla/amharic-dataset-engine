import os
import random
from typing import List, Optional, Dict

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

    pending: List[ray.ObjectRef] = []
    prio_by_ref: Dict[ray.ObjectRef, float] = {}
    chan_by_ref: Dict[ray.ObjectRef, str] = {}
    iteration = 0

    while True:
        if iteration % 10 == 0:
            qc = ray.get(queue.get_queue.remote())
            print(f"\nQueue @ iter {iteration}:")
            for p, h in sorted(qc, key=lambda x: x[0], reverse=True):
                print(f"  {p:.2f} → {h}")
            print("-" * 30)

        while len(pending) < max_workers:
            item = ray.get(queue.pop.remote())
            if item is None:
                break
            handle, prio = item
            sess = session_names[len(pending) % num_sessions]
            ref = dispatch(sess, handle, limit, queue_actor=queue)

            pending.append(ref)
            prio_by_ref[ref] = prio
            chan_by_ref[ref] = handle

        if not pending:
            break

        done, not_ready = ray.wait(pending, num_returns=1, timeout=10)

        if not done:
            ref = pending.pop(0)
            old_prio = prio_by_ref.pop(ref, None)
            channel = chan_by_ref.pop(ref, None)

            print(f"Skipping {channel} (took >10s)")
            try:
                ray.cancel(ref)
            except Exception:
                pass

            if old_prio is not None:
                new_prio = old_prio * 0.5
                if new_prio > 0.001:
                    queue.push.remote(channel, new_prio)
            continue

        ref = done[0]
        pending = not_ready

        old_prio = prio_by_ref.pop(ref, None)
        channel = chan_by_ref.pop(ref, None)

        try:
            _, messages, max_id, _ = ray.get(ref)
        except Exception as e:
            print(f"Error crawling {channel}: {e}")
            continue

        print(f"Crawled channel: {channel} → {len(messages)} msg(s)")

        if messages:
            with connect(DB_PATH) as conn:
                for m in messages:
                    insert_message(conn, m)
        if max_id:
            update_last_msg_id(channel, max_id)

        if len(messages) > 0:
            new_prio = (old_prio or BASE_PRIORITY) * 1.2
        else:
            new_prio = (old_prio or BASE_PRIORITY) * 0.5

        if new_prio > 0.001:
            queue.push.remote(channel, new_prio)

        iteration += 1

    if DEBUG:
        print(f"\nCrawl complete. DB: {DB_PATH}")

    ray.shutdown()


if __name__ == "__main__":
    run_parallel_crawl()
