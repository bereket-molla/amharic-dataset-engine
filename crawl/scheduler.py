import time
import heapq
from typing import Optional, Tuple

import ray

@ray.remote
class ChannelQueue:
    def __init__(self):
        self.heap: list[tuple[float, str]] = []

    def push(self, handle: str, priority: float = 1.0) -> None:
        heapq.heappush(self.heap, (-priority, handle))

    def pop(self) -> Optional[Tuple[str, float]]:
        if not self.heap:
            return None
        neg_p, handle = heapq.heappop(self.heap)
        return handle, -neg_p

    def size(self) -> int:
        return len(self.heap)
