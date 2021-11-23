from typing import NoReturn
import sys
import json
import socket
import os
import logging

logging.basicConfig(level=logging.DEBUG)


def main() -> NoReturn:
    name = sys.argv[1]
    host = sys.argv[2]
    port = int(sys.argv[3])

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        for line in sys.stdin:
            line = line.strip()
            tmp = line.split(" ")
            if len(tmp) != 2:
                logging.error("invalid input format, skip")
                continue
            timestamp, payload = tmp
            msg = {"timestamp": timestamp, "payload": payload, "from": name}
            payload = json.dumps(msg)
            s.send(payload.encode())
            s.send(b"\n")


if __name__ == "__main__":
    pass
