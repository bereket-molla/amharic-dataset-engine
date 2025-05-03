import time
import heapq
from typing import Optional, Tuple, List, Set

import ray

@ray.remote
class ChannelQueue:
    def __init__(self):
        self.heap: list[tuple[float, str]] = []
        self.queued_handles: Set[str] = set()

    def push(self, handle: str, priority: float = 1.0) -> None:
        if handle not in self.queued_handles:
            heapq.heappush(self.heap, (-priority, handle))
            self.queued_handles.add(handle)

    def pop(self) -> Optional[Tuple[str, float]]:
        if not self.heap:
            return None
        neg_p, handle = heapq.heappop(self.heap)
        self.queued_handles.discard(handle)  # Remove when popped
        return handle, -neg_p

    def peek(self) -> Optional[Tuple[str, float]]:
        if not self.heap:
            return None
        neg_p, handle = self.heap[0]
        return handle, -neg_p

    def get_queue(self) -> List[Tuple[str, float]]:
        return sorted([(-p, h) for p, h in self.heap], key=lambda x: x[0])

    def size(self) -> int:
        return len(self.heap)