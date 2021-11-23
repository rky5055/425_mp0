from __future__ import annotations

import threading
from typing import Any, NoReturn
import logging
import sys
from queue import Queue, Empty
from threading import Thread
import random
import secrets
import json
import time
from dataclasses import dataclass
import dataclasses

class Timer:
    def __init__(self, timeout: float) -> None:
        self._chan = Queue(maxsize=1)
        timer = threading.Timer(timeout, lambda: self._chan.put(None, block=False))
        timer.start()

    def chan(self) -> Queue:
        return self._chan


class Ticker:
    def _tick(self, interval: float) -> None:
        while True:
            if self._chanQuit.is_set():
                return
            time.sleep(interval)
            self._chan.put(None, block=False)

    def __init__(self, interval: float) -> None:
        self._chan = Queue(maxsize=0)
        self._chanQuit = threading.Event()
        t = Thread(target=self._tick, args=(interval,), daemon=True)
        t.start()

    def chan(self) -> Queue:
        return self._chan

    def stop(self) -> None:
        self._chanQuit.set()

def select(*chans):
    mergedChan = Queue(maxsize=0)
    def join(chan: Queue):
        while True:
            mergedChan.put((chan, chan.get(block=True)))
    for chan in chans:
        t = Thread(target=join, args=(chan,), daemon=True)
        t.start()
    while True:
        yield mergedChan.get(block=True)


def getChan(chan: Queue) -> tuple[bool, Any]:
    """return (ok, value)"""
    v = None
    try:
      v = chan.get_nowait()
    except Empty:
        return (False, None)
    return (True, v)


logging.basicConfig(level=logging.DEBUG)

STATE_FOLLOWER = "FOLLOWER"
STATE_CANDIDATE = "CANDIDATE"
STATE_LEADER = "LEADER"

# MS
ELECTION_TIMEOUT_MAX = 3000
ELECTION_TIMEOUT_MIN = 1500
HEARTBEAT_INTERVALS = 300

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)

@dataclass
class LogEntry:
    term: int
    body: str


class LogProxy:
    def __init__(self, entries: list[LogEntry], f) -> LogProxy:
        self.entries = entries
        self.f = f

    def get(self, index: int) -> LogEntry:
        return self.entries[index]

    def set(self, index: int, entry: LogEntry) -> None:
        self.entries[index] = entry

    def append(self, entry: LogEntry) -> None:
        self.entries.append(entry)
        self.f(len(self.entries), entry)

    def shrink(self, start: int, end: int) -> list[LogEntry]:
        self.entries = [*self.entries[start:end]]

    def slice(self, start: int, end: int) -> list[LogEntry]:
        return [*self.entries[start:end]]

    def len(self):
        return len(self.entries)


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
        payload = json.dumps(v, cls=JSONEncoder)
        return f"SEND {dstNID} {payload}\n"

    def logState(self, varName: str, v: str) -> None:
        payload = f"STATE {varName}={v}\n"
        sys.stdout.write(payload)
        sys.stdout.flush()

    def logLogEntry(self, index: int, entry: LogEntry) -> None:
        k = f"log[{index}]"
        v = f'[{entry.term},"{entry.body}"]'
        self.logState(k, v)

    def logCommitIndex(self, index: int) -> None:
        self.logState("commitIndex", index)

    def logCommitted(self, v: str, index: int) -> None:
        payload = f"COMMITTED {v} {index}\n"
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
    def __init__(self, nid: str, numOfNodes: int, bufSize: int = 0) -> Raft:
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
        self.logs = LogProxy([], self.logEntry)
        self.commitIndex = IntProxy(0, self.logCommited)
        self.nextIndex = [0 for _ in range(numOfNodes)]
        self.matchIndex = [0 for _ in range(numOfNodes)]
        self.lastApplied = 0

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

    def logEntry(self, index, entry) -> None:
        self.framework.logLogEntry(index, entry)

    def logCommited(self, old, new) -> None:
        self.framework.logCommitIndex(new)

    def logEntryMatch(self, prev_index, prev_term):
        if prev_index == -1 and prev_term == -1:
            return True
        if prev_index < 0 or prev_index >= self.logs.len():
            return False
        logEntry = self.logs.get(prev_index)
        return logEntry.term == prev_term

    def alignLogEntries(self, prev_index: int, entries: list[LogEntry]):
        start_index = prev_index + 1
        for i in entries:
            if start_index >= self.logs.len():
                self.logs.append(i)
                start_index += 1
                continue
            currEntry = self.logs.get(start_index)
            if i.term != currEntry.term:
                self.logs.shrink(0, start_index)
                self.logs.append(i)
                start_index += 1
                continue
            else:
                self.logs.set(start_index, currEntry)
                start_index += 1
                continue
    def updateTerm(self, term : int) -> bool:
        if term > self.currTerm.get():
            self.currLeader.set("")
            self.currTerm.set(term)
            self.voteFor = ""
            return True
        return False
    
    def lastLogIndex(self) -> int:
        if self.logs.len() == 0:
            return -1
        else:
            return self.logs.len() - 1
    
    def lastLogTerm(self) -> int:
        if self.logs.len() == 0:
            return -1
        else:
            entry = self.logs.get(self.logs.len() - 1)
            return entry.term

    def moreOrEqualUpToDateLog(self, lastLogIndex: int, lastLogTerm: int) -> bool:
        selfLastLogIndex = self.lastLogIndex()
        selfLastLogTerm = self.lastLogTerm()
        if selfLastLogTerm != lastLogTerm:
            return selfLastLogTerm > lastLogTerm
        else:
            return selfLastLogIndex >= lastLogIndex

    def resetNextIndex(self) -> None:
        for i in range(len(self.nextIndex)):
            self.nextIndex[i] = max(0,self.logs.len() - 1)

    def tryIncCommitIndex(self, matchIndex: int, term: int) -> None:
        if (matchIndex + 1) < self.commitIndex.get():
            return
        count = 1
        for v in self.matchIndex:
            if v >= matchIndex:
                count += 1
        if count >= self.majorityNum and self.logs.get(matchIndex).term == term:
            self.commitIndex.set(matchIndex + 1)
            self.apply(self.logs.get(matchIndex).body, matchIndex + 1)

    def bMulticastAppendEntriesReq(self):
        for i in range(self.numOfNodes):
            dstID = str(i)
            if dstID != self.nid:
                msg = self.buildEntryReq(i)
                self.framework.unicast(dstID, msg)


    def apply(self, op: str, index: int):
        self.framework.logCommitted(op, index)
        self.lastApplied = index

    def buildEntryReq(self, i):
        request = {
            "path": "AppendEntriesReq",
            "nid": self.nid,
            "term": self.currTerm.get(),
            "leader": self.currLeader.get(),
            "leader_commit": self.commitIndex.get(),
            "prev_index": "",
            "prev_term": "",
            "entries": [],
        }

        next_index = self.nextIndex[i]
        prev_index = next_index - 1
        
        if prev_index == -1:
            request["prev_index"] = -1
            request["prev_term"] = -1
        else:
            request["prev_index"] = prev_index
            request["prev_term"] = self.logs.get(prev_index).term
        lower = max(0, next_index)
        upper = min(self.logs.len(), next_index + 1)
        request["entries"] = self.logs.slice(lower, upper)
        return request


    def follower(self) -> str:
        timeout = self.randomTimeout()
        timer = Timer(timeout)
        timerChan = timer.chan()
        while True:
            ok, event = getChan(timerChan)
            if ok:
                event = { "path": "FollowerTimeout" }
            else:
                ok, event = getChan(self.eventChan)
                if not ok:
                    continue
            match event:
                case { "path": "FollowerTimeout" }:
                    logging.info(event)
                    logging.info(f"follower election timeout after {timeout}")
                    return STATE_CANDIDATE
                case { 
                        "path": "AppendEntriesReq", 
                        "nid": nid, 
                        "term": term, 
                        "leader": leader, 
                        "leader_commit": leader_commit,
                        "prev_index": prev_index, 
                        "prev_term": prev_term, 
                        "entries": entries,
                        **v
                    }:
                    logging.info(event)
                    entries = [ LogEntry(**x) for x in entries ]
                    if term < self.currTerm.get():
                        continue
                    elif term > self.currTerm.get():
                        self.voteFor=""
                        self.currLeader.set("")
                        self.currTerm.set(term)
                        self.currLeader.set(leader)

                    if not self.logEntryMatch(prev_index, prev_term):
                        response = {
                            "path": "AppendEntriesRes", 
                            "nid":self.nid, 
                            "term": self.currTerm.get(), 
                            "success": False
                        }
                        self.framework.unicast(nid, response)
                        return STATE_FOLLOWER

                    self.currLeader.set(leader)
                    self.alignLogEntries(prev_index, entries)

                    if leader_commit > self.commitIndex.get() and self.logs.len() == leader_commit:
                        self.commitIndex.set(leader_commit)

                    response={
                        "path": "AppendEntriesRes", 
                        "nid":self.nid, 
                        "term": self.currTerm.get(), 
                        "success":True
                    }

                    if len(entries) != 0:
                        self.framework.unicast(nid, response)
                    
                    return STATE_FOLLOWER

                case {
                    "path": "AppendEntriesRes",
                    "term": term,
                    "nid": nid, 
                    **v
                }:
                    logging.info(event)
                    if term > self.currTerm.get():
                        self.currLeader.set("")
                        self.currTerm.set(term)
                        self.voteFor=""
                        return STATE_FOLLOWER
                    continue
                case {
                    "path": "RequestVoteReq", 
                    "nid": nid,
                    "candidate": candidate,
                    "term": term,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term,
                    **v
                }:  
                    logging.info(event)
                    if term < self.currTerm.get() or not self.moreOrEqualUpToDateLog(last_log_index, last_log_term):
                        response = {
                            "path": "RequestVoteRes",
                            "term": self.currTerm.get(),
                            "vote_granted": False
                        }
                        self.framework.unicast(candidate, response)
                        continue
                    if term > self.currTerm.get():
                        self.updateTerm(term)
                    
                    response = {
                        "path": "RequestVoteRes",
                        "term": self.currTerm.get(),
                        "vote_granted": False
                    }

                    if self.voteFor == "" or self.voteFor == candidate:
                        self.voteFor = candidate
                        response["vote_granted"] = True
                        self.framework.unicast(candidate, response)
                        return STATE_FOLLOWER
                    else:
                        response["vote_granted"] = False
                        self.framework.unicast(candidate, response)
                    continue
                case {
                    "path": "RequestVoteRes", 
                    "term": term,
                    "nid": nid, 
                    "vote_granted": vote_granted,
                    **v
                }:
                    logging.info(event)
                    if term > self.currTerm.get():
                        self.updateTerm(term)
                        return STATE_FOLLOWER
                    continue
                case _:
                    logging.info(f"Follower gets unknown event, skip; {event}")
                    pass


    def candidate(self) -> str:
        self.currLeader.set("")
        self.currTerm.set(self.currTerm.get() + 1)
        self.voteFor=self.nid
        voteCount=1

        request={
            "path": "RequestVoteReq", 
            "nid": self.nid, 
            "term":self.currTerm.get(),
            "candidate": self.nid,
            "last_log_index": self.lastLogIndex(),
            "last_log_term": self.lastLogTerm()
        }

        self.framework.bMulticastWithoutSelf(request)

        timeout = self.randomTimeout()
        timer = Timer(timeout)
        timerChan = timer.chan()

        while True:
            ok, event = getChan(timerChan)
            if ok:
                event = { "path": "CandidateTimeout" }
            else:
                ok, event = getChan(self.eventChan)
                if not ok:
                    continue
            match event:
                case { "path": "CandidateTimeout" }:
                    logging.info(event)
                    return STATE_CANDIDATE
                case { 
                    "path": "AppendEntriesReq",
                    "nid": nid, 
                    "term": term,
                    "leader": leader,
                     **v
                }:
                    logging.info(event)
                    if term < self.currTerm.get():
                        continue
                    else:
                        self.currLeader.set("")
                        self.currTerm.set(term)
                        self.currLeader.set(leader)
                        self.voteFor=""
                        return STATE_FOLLOWER
                case {
                    "path": "AppendEntriesRes", 
                    "nid": nid,
                    "term": term,
                    **v
                }:
                    logging.info(event)
                    if term <= self.currTerm.get():
                        continue
                    else:
                        self.currLeader.set("")
                        self.currTerm.set(term)
                        self.voteFor=""
                        return STATE_FOLLOWER
                case {
                    "path": "RequestVoteReq", 
                    "nid": nid, 
                    "candidate": candidate,
                    "term": term,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term,
                    **v
                }:
                    logging.info(event)

                    if term <= self.currTerm.get() or not self.moreOrEqualUpToDateLog(last_log_index, last_log_term):
                        response = {
                            "path": "RequestVoteRes",
                            "term": self.currTerm.get(),
                            "vote_granted": False
                        }
                        self.framework.unicast(candidate, response)
                        continue

                    self.currLeader.set("")
                    self.currTerm.set(term)
                    self.voteFor=candidate

                    response = {
                        "path": "RequestVoteRes",
                        "term": self.currTerm.get(),
                        "nid": self.currTerm.get(),
                        "vote_granted": True
                    }
                    self.framework.unicast(nid, response)
                    return STATE_FOLLOWER

                case {
                    "path": "RequestVoteRes", 
                    "nid": nid,
                    "term": term,
                    "vote_granted": vote_granted,
                    **v
                }:
                    logging.info(event)
                    if term < self.currTerm.get():
                        continue
                    if term > self.currTerm.get():
                        self.updateTerm(term)
                        return STATE_FOLLOWER
                    if vote_granted:
                        logging.info(f"candidate get new vote from {nid}")
                        voteCount += 1
                        if voteCount >= self.majorityNum:
                            return STATE_LEADER
                    continue
                case _:
                    logging.info(f"Candidate gets unknown event, skip; {event}")
                    pass


    def leader(self) -> str:
        self.currLeader.set(self.nid)
        self.resetNextIndex()
        self.bMulticastAppendEntriesReq()

        ticker = Ticker(HEARTBEAT_INTERVALS / 1000)
        tickerChan = ticker.chan()

        while True:
            ok, event = getChan(tickerChan)
            if ok:
                event = { "path": "HeartBeat" }
            else:
                ok, event = getChan(self.eventChan)
                if not ok:
                    continue
            match event:
                case {"path": "HeartBeat"}:
                    logging.info(event)
                    self.bMulticastAppendEntriesReq()
                    continue
                case {
                    "path": "AppendEntriesReq",
                    "term": term,
                    "nid": nid,
                    **v
                }:
                    logging.info(event)
                    if term <= self.currTerm.get():
                        continue

                    self.currLeader.set("")
                    self.currTerm.set(term)
                    self.currLeader.set(nid)
                    self.voteFor=""
                    return STATE_FOLLOWER

                case {
                    "path": "AppendEntriesRes", 
                    "nid": nid, 
                    "term": term,
                    "success": success,
                    **v
                }:
                    if term > self.currTerm.get():
                        self.updateTerm(term)
                        return STATE_FOLLOWER

                    nid=int(nid)
                    if success:
                        self.matchIndex[nid]=self.nextIndex[nid]
                        self.nextIndex[nid] += 1
                        matchIndex = self.matchIndex[nid]
                        self.tryIncCommitIndex(matchIndex, term)
                    else:
                        self.nextIndex[nid] -= 1
                        msg = self.buildEntryReq(nid)
                        self.framework.unicast(nid, msg)
                    continue
                case {
                    "path": "RequestVoteReq", 
                    "nid": nid, 
                    "candidate": candidate,
                    "term": term,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term,
                    **v
                }:
                    logging.info(event)

                    if term <= self.currTerm.get() or not self.moreOrEqualUpToDateLog(last_log_index, last_log_term):
                        response = {
                            "path": "RequestVoteRes",
                            "term": self.currTerm.get(),
                            "vote_granted": False
                        }
                        self.framework.unicast(candidate, response)
                        continue

                    self.currLeader.set("")
                    self.currTerm.set(term)
                    self.voteFor=candidate

                    response = {
                        "path": "RequestVoteRes",
                        "term": self.currTerm.get(),
                        "nid": self.currTerm.get(),
                        "vote_granted": True
                    }
                    self.framework.unicast(nid, response)
                    return STATE_FOLLOWER
                case {
                    "path": "RequestVoteRes", 
                    "nid": nid, 
                    "term": term,
                    **v
                }:
                    logging.info(event)
                    if term > self.currTerm.get():
                        self.currTerm.set(term)
                        return STATE_FOLLOWER
                    continue
                case {
                    "log": body, 
                    **v
                }:
                    logging.info(event)
                    logEntry = LogEntry(self.currTerm.get(), body)
                    self.logs.append(logEntry)
                    continue
                case _:
                    logging.info(f"Leader gets unknown event, skip; {event}")
                    pass

    def consumer(self) -> NoReturn:
        while True:
            line = sys.stdin.readline()
            fields = line.split()
            match fields:
                case ["RECEIVE", nid, *payload]:
                    event = json.loads("".join(payload))
                    event = {
                        **event,
                        "nid": nid
                    }
                    self.eventChan.put(event, block=True)
                    continue
                case ["LOG", payloadStr]:
                    event = {
                        "log": payloadStr
                    }
                    self.eventChan.put(event, block=True)
                    continue
                case _:
                    if len(fields) != 0:
                        logging.info(f"invalid input format, skip; {fields}")
                    continue

    def start(self) -> NoReturn:
        Thread(target=self.consumer, args=(), daemon=True).start()
        logging.info(f"start raft daemon service: {self.nid}; {self.numOfNodes}")

        while True:
            match self.state.get():
                case state if state == STATE_FOLLOWER:
                    self.state.set(self.follower())
                    logging.info(f"state:: {STATE_FOLLOWER} -> {self.state.get()}")
                    continue
                case state if state == STATE_CANDIDATE:
                    self.state.set(self.candidate())
                    logging.info(f"state:: {STATE_CANDIDATE} -> {self.state.get()}")
                    continue
                case state if state == STATE_LEADER:
                    self.state.set(self.leader())
                    logging.info(f"state:: {STATE_LEADER} -> {self.state.get()}")
                    continue
                case _:
                    logging.info("raft state is invalid")
                    return


def main() -> NoReturn:
    nodeID = sys.argv[1]
    numOfNodes = int(sys.argv[2])
    raft = Raft(nodeID, numOfNodes, 0)
    raft.start()

if __name__ == "__main__":
    main()
