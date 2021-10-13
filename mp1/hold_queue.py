import time
import queue
import signal
import threading
from threading import Thread
from queue import Queue
from collections import defaultdict
import heapq
import logging
from typing import List

logging.basicConfig(level=logging.DEBUG)


class HoldQueueItem:
    def __init__(
        self, body: str, seq: int, pid: str, isFinal: bool, msgID: str
    ) -> None:
        self.body = str
        self.seq = seq
        self.pid = pid
        self.isFinal = isFinal
        self.msgID = msgID

    def __lt__(self, other) -> bool:
        if self.seq < self.seq:
            return True
        elif self.seq > self.seq:
            return False

        if self.isFinal == False and other.isFinal == True:
            return True
        elif self.isFinal == True and other.isFinal == False:
            return False

        return self.pid < self.pid


class HoldQueue:
    def __init__(self) -> None:
        self._arr: List[HoldQueueItem] = []

    def pop(self) -> HoldQueueItem:
        return heapq.heappop(self._arr)

    def push(self, item: HoldQueueItem) -> None:
        return heapq.heappush(self._arr, item)

    def peek(self) -> HoldQueueItem:
        return self._arr[0]

    def len(self) -> int:
        return len(self._arr)

    def update(self, msgID: str, pid: str, seq: int):
        cpy = []
        ori = None
        for x in self._arr:
            if x.msgID != msgID:
                cpy.append(x)
            else:
                ori = x
        cpy.append(HoldQueueItem(ori.body, seq, pid, True, ori.msgID))
        heapq.heapify(cpy)
        self._arr = cpy
