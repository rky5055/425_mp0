from typing import NoReturn

import logging
import sys
from queue import Queue
from threading import Thread, Timer
import random
import secrets
import json
from dataclasses import dataclass

logging.basicConfig(level=logging.DEBUG)

STATE_FOLLOWER  = "FOLLOWER"
STATE_CANDIDATE = "CANDIDATE"
STATE_LEADER    = "LEADER"

# MS
ELECTION_TIMEOUT_MAX = 3000
ELECTION_TIMEOUT_MIN = 1500
HEARTBEATINTERVALS = 300

class IntProxy:
    def __init__(self, v: int, f) -> None:
        self.v = v
        self.f = f
    
    def get(self) -> int:
        return self.v
    
    def set(self, v: int) -> None:
        if self.v == v:
            return
        else:
            self.f(self.v, v)
            self.v = v
            return

class StrProxy:
    def __init__(self, v: str, f) -> None:
        self.v = v
        self.f = f

    def get(self) -> str:
        return self.v
    
    def set(self, v: str) -> None:
        if self.v == v:
            return
        else:
            self.f(self.v, v)
            self.v = v
            return

class Framework:
    def __init__(self, nid: str, numOfNodes: int) -> None:
        self.nid = nid
        self.numOfNodes = numOfNodes
    
    @staticmethod
    def encodeFrameworkMsg(dstNID: str, v: any) -> str:
        payload = json.dumps(v)
        return f"SEND {dstNID} {payload}\n"
    
    def logState(self, varName: str, v: str) -> None:
        payload = f"STATE {varName}={v}\n"
        sys.stdout.write(payload)
        sys.stdout.flush()


    def unicast(self, dstNID: str, v: any) -> None:
        payload = Framework.encodeFrameworkMsg(dstNID, v)
        sys.stdout.write(payload)
        sys.stdout.flush()

    def bMulticastWithoutSelf(self, v: any) -> None:
        for i in range(self.numOfNodes):
            dstNID = str(i)
            if dstNID == self.nid:
                continue
            self.unicast(dstNID, v)
        return


class Raft:
    def __init__(self, nid: str, numOfNodes: int, bufSize: int = 100) -> None:
        random.seed(secrets.randbits(64))
        self.nid = nid
        self.numOfNodes = numOfNodes
        self.framework = Framework(nid, numOfNodes)
        self.eventChan = Queue(maxsize=bufSize)
        self.majorityNum = (numOfNodes // 2) + 1
        self.currTerm = IntProxy(0, self.logTerm)
        self.currLeader = StrProxy("", self.logLeader)
        self.state = StrProxy(STATE_FOLLOWER, self.logState)
        self.voteFor = ""
    
    def randomTimeout(self) -> float:
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX) / 1000

    def logTerm(self, old: int, new: int) -> None:
        self.framework.logState("term", str(new))
    
    def logState(self, old: str, new: str) -> None:
        self.framework.logState("state", f'"{new}"')

    def logLeader(self, old: str, new: str) -> None:
        if new == "":
            self.framework.logState("leader", "null")
            return
        else:
            self.framework.logState("leader", f'"{new}"')
            return

    def sendFollowerTimeOutMsg(self) -> None:
        event = { "path": "FollowerTimeout" }
        self.eventChan.put(event, block=True)
    
    def sendCandidateTimeOutMsg(self) -> None:
        event = { "path": "CandidateTimeout" }
        self.eventChan.put(event, block=True)

    def consumer(self):
        while True:
            line = sys.stdin.readline()
            fields = line.split()
            match fields:
                case ["RECEIVE", nid, payload]:
                    event = json.loads(payload)
                    event = {
                        **event,
                        "nid": nid
                    }
                    self.eventChan.put(event, block=True)
                case _:
                    logging.info(f"invalid input format, skip; {fields}")
                    pass

    def start(self) -> NoReturn:
        Thread(target=self.consumer, args=(), daemon=True).start()
        logging.info(f"start raft daemon service: {self.nid}; {self.numOfNodes}")
        while True:
            match self.state.get():
                case state if state == STATE_FOLLOWER:
                    self.state.set(self.follower())
                    logging.Infof(f"state:: {STATE_FOLLOWER} -> {self.state.get()}")
                    continue
                case state if state == STATE_CANDIDATE:
                    self.state.set(self.candidate())
                    logging.Infof(f"state:: {STATE_CANDIDATE} -> {self.state.get()}")
                    continue
                case state if state == STATE_LEADER:
                    self.state.set(self.leader())
                    logging.Infof(f"state:: {STATE_LEADER} -> {self.state.get()}")
                    continue
                case _:
                    logging.Infof("raft state is invalid")
                    return
            
    def follower(self) -> str:
        timeout = self.randomTimeout()
        timer = Timer(timeout, self.sendFollowerTimeOutMsg)
        timer.start()
        while True:
            event = self.eventChan.get(block=True)
            match event:
                case { "path": "FollowerTimeout", **v }:
                    logging.info(event)
                case {"path": "AppendEntriesReq", "nid": nid, **v}:
                    logging.info(event)
                    pass
                case {"path": "AppendEntriesRes", "nid": nid, **v}:
                    logging.info(event)
                    pass
                case {"path": "RequestVoteReq", "nid": nid, **v}:
                    logging.info(event)
                    pass
                case {"path": "RequestVoteRes", "nid": nid, **v}:
                    logging.info(event)
                    pass
                case _:
                    logging.info(f"unknown event, skip; {event}")
                    pass

    def candidate(self) -> str:
        while True:
            event = self.eventChan.get(block=True)
            match event:
                case {"path": "AppendEntriesReq", "nid": nid, **v}:
                    logging.info(event)
                    pass
                case {"path": "AppendEntriesRes", "nid": nid, **v}:
                    logging.info(event)
                    pass
                case {"path": "RequestVoteReq", "nid": nid, **v}:
                    logging.info(event)
                    pass
                case {"path": "RequestVoteRes", "nid": nid, **v}:
                    logging.info(event)
                    pass
                case _:
                    logging.info(f"unknown event, skip; {event}")
                    pass

    def leader(self) -> str:
        while True:
            event = self.eventChan.get(block=True)
            match event:
                case {"path": "AppendEntriesReq", "nid": nid, **v}:
                    logging.info(event)
                    pass
                case {"path": "AppendEntriesRes", "nid": nid, **v}:
                    logging.info(event)
                    pass
                case {"path": "RequestVoteReq", "nid": nid, **v}:
                    logging.info(event)
                    pass
                case {"path": "RequestVoteRes", "nid": nid, **v}:
                    logging.info(event)
                    pass
                case _:
                    logging.info(f"unknown event, skip; {event}")
                    pass

def main() -> NoReturn:
    nodeID = sys.argv[1]
    numOfNodes = int(sys.argv[2])
    raft = Raft(nodeID, numOfNodes)
    raft.start()


