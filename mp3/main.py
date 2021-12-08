import threading
import sys
import queue
import time
import logging

logging.basicConfig(level=logging.DEBUG)

class Channel:
    def __init__(self, size: int) -> None:
        self.queue = queue.Queue(maxsize=size)

    def put(self, item) -> None:
        self.queue.put(item, block=True)
    
    def put_nowait(self, item) -> None:
        try:
            self.queue.put_nowait(item)
        except queue.Full:
            pass
    def get(self):
        return self.queue.get(block=True)

def ParseLine(line: str) -> dict:
    line = line.strip()
    match line:
        case "ABORT":
            return { "path": "ABORT" }
        case "BEGIN":
            return { "path": "BEGIN" }
        case _:
            return {}

forceSeq = Channel(size=1)
forceSeq.put(None)

for line in sys.stdin:
    event = ParseLine(line)
    match event:
        case { "path": "ABORT" }:
            logging.info("ABORT")
            forceSeq.put_nowait(None)
            pass
        case _:
            forceSeq.get()
            def handler(event):
                match event:
                    case { "path": "BEGIN" }:
                        logging.info("BEGIN")
                        time.sleep(5)
                        logging.info("BEGIN DONE")
                        forceSeq.put_nowait(None)
                    case _:
                        forceSeq.put_nowait(None)
            threading.Thread(target=handler, args=(event,), daemon=True).start()

