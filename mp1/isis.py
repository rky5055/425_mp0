import hashlib
import secrets
import json
import logging
from typing import List
import time
import queue
import signal
import threading
from threading import Thread
from queue import Queue
from collections import defaultdict

import logging
from typing import List

from .hold_queue import HoldQueueItem
from .hold_queue import HoldQueue
from .voter import Voter

logging.basicConfig(level=logging.DEBUG)


class Multicaster:
    def __init__(self) -> None:
        pass

    def multicast(self, msg) -> None:
        logging.info(f"multicast {msg}")


class Unicaster:
    def __init__(self) -> None:
        pass

    def multicast(self, msg) -> None:
        logging.info(f"multicast {msg}")


class Vote:
    def __init__(self, seq: int, pid: str, msgID: str) -> None:
        self.seq = seq
        self.pid = pid
        self.msgID = msgID

    def __str__(self) -> str:
        return f"seq :{self.seq}, pid: {self.pid}, msgID: {self.msgID}"


class VoterEventNewVote:
    def __init__(self, vote: Vote) -> None:
        self.vote = vote


class VoterEventUpdateThreshold:
    def __init__(self, threshold: int) -> None:
        self.threshold = threshold


class VoteEventQuit:
    def __init__(self) -> None:
        pass


def maxVotes(votes: List[Vote]) -> Vote:
    return max(votes, key=lambda v: (v.seq, v.pid))


class Voter:
    def __init__(self, multicaster: Multicaster, threshold: int, bufSize=10) -> None:
        # msg_id -> votes [...]
        self._votes = dict()
        self._chan = Queue(maxsize=bufSize)
        self._threshold = threshold
        self._multicaster = multicaster

    def quit(self) -> None:
        self._chan.put(VoteEventQuit(), block=True)

    def vote(self, seq: int, pid: str, msgID: str) -> None:
        self._chan.put(VoterEventNewVote(seq, pid, msgID), block=True)

    def updateThreshold(self, threshold: int) -> None:
        self._chan.put(VoterEventUpdateThreshold(threshold), block=True)

    def send(self, event) -> None:
        self._chan.put(event, block=True)

    def run(self):
        logging.info("start voter daemon service")
        while True:
            event = self._chan.get(block=True)
            if isinstance(event, VoteEventQuit):
                logging.info("voter quit")
                return
            elif isinstance(event, VoterEventUpdateThreshold):
                logging.info(
                    f"voter threshold update to {event.threshold}, re-check vote count"
                )
                self._threshold = event.threshold
                for msgID, votes in [*self._votes.items()]:
                    if len(votes) < self._threshold:
                        continue

                    finalVote = maxVotes(votes)
                    finalVoteMsg = TotalOrderAnnounceMsg(
                        finalVote.pid, finalVote.seq, finalVote.msgID
                    )
                    self._multicaster.multicast(finalVoteMsg)
                    del self._votes[msgID]

            elif isinstance(event, VoterEventNewVote):
                logging.info(f"voter new vote {event.vote}")
                vote = event.vote
                if vote.msgID not in self._votes:
                    self._votes[vote.msgID] = []

                votes = self._votes[vote.msgID]
                votes.append(vote)

                if len(votes) < self._threshold:
                    continue

                finalVote = maxVotes(votes)
                finalVoteMsg = TotalOrderAnnounceMsg(
                    finalVote.pid, finalVote.seq, finalVote.msgID
                )
                self._multicaster.multicast(finalVoteMsg)
                del self._votes[vote.msgID]

    def start(self):
        return Thread(target=self.run, args=(), daemon=True).start()


class TotalOrderAskMsg:
    def __init__(self, pid: str, body: str) -> None:
        self.pid = pid
        self.body = body
        self.msgID = secrets.token_hex(15) + hashlib.md5(body.encode()).hexdigest()


class TotalOrderReplyAskMsg:
    def __init__(self, pid: str, seq: int, msgID: str) -> None:
        self.pid = pid
        self.seq = seq
        self.msgID = msgID


class TotalOrderAnnounceMsg:
    def __init__(self, pid: str, seq: int, msgID: str) -> None:
        self.pid = pid
        self.seq = seq
        self.msgID = msgID


class TotalOrder:
    def __init__(
        self, src: str, members, multicaster: Multicaster, unicaster: Unicaster
    ) -> None:
        self._lock = threading.Lock()
        self._src = src
        self._multicaster = multicaster
        self._unicaster = unicaster
        self._maxFinalSeq = 0
        self._maxProposalSeq = 0
        self._holdQueue = HoldQueue()
        self._voter = Voter(multicaster, len(members), 100000)

    def multicast(self, msg: any) -> None:
        body = json.dumps(msg)
        ask = TotalOrderAskMsg(self._src, body)
        self._multicaster.multicast(ask)

    def onAsk(self, msg: TotalOrderAskMsg):
        with self._lock:
            proposalSeq = max(self._maxFinalSeq, self._maxProposalSeq) + 1
            self._maxProposalSeq = proposalSeq
            item = HoldQueueItem(msg.body, proposalSeq, msg.pid, False, msg.msgID)
            self._holdQueue.push(item)
            replyAsk = TotalOrderReplyAskMsg(self._src, proposalSeq, msg.msgID)
            self._multicaster.multicast(replyAsk)

    def onReplyAsk(self, msg: TotalOrderReplyAskMsg):
        self._voter.vote(msg.seq, msg.pid, msg.msgID)

    def onFinal(self, msg: TotalOrderAnnounceMsg):
        self._maxFinalSeq = max(self._maxFinalSeq, msg.seq)

        self._holdQueue.update(msg.msgID, msg.pid, msg.seq)

        while len(self._holdQueue) != 0:
            item = self._holdQueue.peek()
            if not item.isFinal:
                break

            logging.info(f"to deliver {item.seq}:{item.pid}:{item.msgID}: {item.body}")
            self._holdQueue.pop()


if __name__ == "__main__":
    multicaster = Multicaster()
    voter = Voter(multicaster, threshold=3, bufSize=1000)
    voter.start()
    events = [
        VoterEventNewVote(Vote(0, "A", "msg0")),
        VoterEventNewVote(Vote(1, "A", "msg0")),
        VoterEventNewVote(Vote(2, "A", "msg0")),
        VoterEventNewVote(Vote(0, "A", "msg1")),
        VoterEventNewVote(Vote(0, "B", "msg1")),
        VoterEventNewVote(Vote(0, "C", "msg1")),
        VoterEventNewVote(Vote(0, "A", "msg2")),
        VoterEventNewVote(Vote(1, "A", "msg2")),
        VoterEventNewVote(Vote(0, "B", "msg3")),
        VoterEventNewVote(Vote(1, "B", "msg3")),
        VoterEventUpdateThreshold(2),
        VoterEventNewVote(Vote(0, "B", "msg4")),
        VoterEventNewVote(Vote(1, "B", "msg4")),
        VoteEventQuit(),
    ]
    for event in events:
        time.sleep(1)
        voter.send(event)
    signal.pause()
