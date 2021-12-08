import threading


class ReadWriteLock:
    """A lock object that allows many simultaneous "read locks", but
    only one "write lock." """

    def __init__(self):
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0

    def RLock(self):
        """Acquire a read lock. Blocks only if a thread has
        acquired the write lock."""
        self._read_ready.acquire()
        try:
            self._readers += 1
        finally:
            self._read_ready.release()

    def RUnlock(self):
        """Release a read lock."""
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if not self._readers:
                self._read_ready.notifyAll()
        finally:
            self._read_ready.release()

    def Lock(self):
        """Acquire a write lock. Blocks until there are no
        acquired read or write locks."""
        self._read_ready.acquire()
        while self._readers > 0:
            self._read_ready.wait()

    def Unlock(self):
        """Release a write lock."""
        self._read_ready.release()


class ReadWriteUpgradeableLock:
    def __init__(self) -> None:
        self.rwlock = ReadWriteLock()
        self.lock = threading.Lock()

    def Lock(self) -> None:
        with self.lock:
            self.rwlock.Lock()

    def Unlock(self) -> None:
        self.rwlock.Unlock()

    def RLock(self) -> None:
        with self.lock:
            self.rwlock.RLock()

    def RUnlock(self) -> None:
        self.rwlock.RUnlock()

    def Upgrade(self) -> None:
        with self.lock:
            self.rwlock.RUnlock()
            self.rwlock.Lock()
