import json
from json.decoder import JSONDecodeError
import os
import sys
import socket
import threading
import logging
import time
from typing import Dict
import signal

logging.basicConfig(level=logging.DEBUG)


class WaitGroup:
    def __init__(self) -> None:
        self._counter = 0
        self._lock = threading.Lock()
        self._wait = threading.Condition()

    def Add(self, delta: int) -> None:
        with self._lock:
            self._counter += delta

    def Done(self):
        self.Add(-1)

    def Wait(self):
        while True:
            counter = 0
            with self._lock:
                counter = self._counter
            if counter == 0:
                return
            time.sleep(2)


def startClients(nodeID: str, port: int):
    # self
    startClient(nodeID, nodeID, SERVER_HOST, port)


def startClient(src: str, dst: str, host: str, port: int):
    client = Client(src, dst, host, port)
    SyncWaitGroup.Done()
    Senders[client] = client


def handler(conn):
    f = conn.makefile()
    with conn:
        SyncWaitGroup.Wait()
        while True:
            try:
                line = f.readline()
                payload = json.loads(line)
                print(json.dumps(payload))
            except JSONDecodeError as err:
                logging.error("invalid request")
                continue


class Client:
    def __init__(self, src: str, dst: str, host: str, port: int) -> None:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        while True:
            try:
                s.connect((host, port))
            except Exception:
                logging.info("reconnect")
                time.sleep(5)
                continue
            break
        self._client = s

    def Send(self, msg):
        payload = json.dumps(msg)
        self._client.send(payload.encode())
        self._client.send(b"\n")

    def Close(self):
        self._client.close()


SERVER_HOST = "0.0.0.0"
SyncWaitGroup = WaitGroup()
Senders: Dict[str, Client] = dict()
Server = None


def closeServer():
    logging.info("close server")
    Server.close()
    sys.exit(0)


def startServer(nodeID: str, port: int):
    global Server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((SERVER_HOST, port))
        s.listen()
        logging.info(f"listen: {port}")
        Server = s
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handler, args=(conn,)).start()


def readStdin() -> object:
    for line in sys.stdin:
        line = line.strip()
        msg = {"a": line, "b": "c"}
        yield msg


def main():
    try:
        nodeID = sys.argv[1]
        port = int(sys.argv[2])
        configPath = sys.argv[3]
        SyncWaitGroup.Add(1)
        st = threading.Thread(target=startServer, args=(nodeID, port), daemon=True)
        st.start()
        startClients(nodeID, port)
        for msg in readStdin():
            for dstID, sender in Senders.items():
                sender.Send(msg)
    except KeyboardInterrupt:
        closeServer()
