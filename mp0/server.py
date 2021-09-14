from json.decoder import JSONDecodeError
from typing import NoReturn
import sys
import socket
import time
import json
import logging
import threading

logging.basicConfig(level=logging.DEBUG)


def handler(conn):
    f = conn.makefile()
    with conn:
        while True:
            try:
                line = f.readline()
                payload = json.loads(line)
                print(json.dumps(payload))
            except JSONDecodeError as err:
                logging.error("EOF")
                return


def main() -> NoReturn:
    host = "0.0.0.0"
    port = int(sys.argv[1])
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()
        while True:
                conn, addr = s.accept()
                threading.Thread(target=handler, args=(conn,)).start()


if __name__ == "__main__":
    pass
