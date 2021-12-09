from _typeshed import Self
from mp3.rwlock import ReadWriteUpgradeableLock
import rwlock
import threading
import time
import logging

logging.basicConfig(level=logging.DEBUG)


class AtomicBool:
    def __init__(self, b: bool) -> None:
        self.b = b
        self.lock = threading.Lock()

    def get(self) -> bool:
        with self.lock:
            return self.b

    def set(self, b: bool) -> None:
        with self.lock:
            self.b = b


class AccountInfo:
    def __init__(self, amount: int, lock: rwlock.ReadWriteUpgradeableLock) -> None:
        self.releaseEvent = threading.Event()
        self.amount = amount
        self.amountLock = threading.Lock()
        self.lock = lock
        self.originAmount = amount
        self.setRLock = AtomicBool(False)
        self.setWLock = AtomicBool(False)
        t = threading.Thread(target=self._release, args=(), daemon=True)
        t.start()

    def _release(self) -> None:
        while True:
            if self.releaseEvent.is_set():
                break
            time.sleep(1)
        if self.Unlock():
            return
        if self.RUnlock():
            return
        while True:
            time.sleep(1)
            if self.Unlock():
                return
            if self.RUnlock():
                return

    def release(self):
        self.releaseEvent.set()

    def getAmount(self) -> int:
        with self.amountLock:
            return self.amount

    def setAmount(self, amount: int) -> None:
        with self.amountLock:
            self.amount = amount

    def diff(self) -> int:
        with self.amountLock:
            return self.amount - self.originAmount

    def RLock(self):
        if self.setRLock.get():
            return
        if self.setWLock.get():
            return
        self.lock.RLock()
        self.setRLock.set(True)

    def Lock(self):
        if self.setWLock.get():
            return
        if self.setRLock.get():
            self.lock.Upgrade()
            self.setWLock.set(True)
            return
        self.lock.Lock()
        self.setWLock.set(True)

    def RUnlock(self) -> bool:
        if not self.setRLock.get():
            return False
        self.lock.RUnlock()
        return True

    def Unlock(self) -> bool:
        if not self.setWLock.get():
            return False
        self.lock.Unlock()
        return True


class Session:
    def __init__(self) -> None:
        self.accounts: dict[str, AccountInfo] = dict()
        self.lock = threading.Lock()

    def getAccount(self, accountName: str) -> AccountInfo | None:
        with self.lock:
            return self.accounts.get(accountName)

    def createAccount(
        self, accountName: str, amount: int, lock: ReadWriteUpgradeableLock
    ) -> AccountInfo:
        with self.lock:
            account = AccountInfo(amount, lock)
            self.accounts[accountName] = account
            return account

    def getAccounts(self) -> dict[str, AccountInfo]:
        return self.accounts

    def release(self) -> None:
        with self.lock:
            for _, accountInfo in self.accounts.items():
                accountInfo.release()


class SessionManager:
    def __init__(self) -> None:
        self.sessions = dict()
        self.lock = threading.Lock()

    @staticmethod
    def GenSessionID(clientID: str, transactionSeq: int) -> str:
        return clientID + str(transactionSeq)

    def createSession(self, sessionID: str) -> None:
        with self.lock:
            self.sessions[sessionID] = Session()

    def getSession(self, sessionID: str) -> Session | None:
        with self.lock:
            return self.sessions.get(sessionID)

    def release(self, sessionID: str) -> None:
        with self.lock:
            session = self.sessions.get(sessionID)
            if session != None:
                session.release()
                del self.sessions[sessionID]


class BankAccount:
    def __init__(self) -> None:
        self.amount = -1
        self.lock = ReadWriteUpgradeableLock()

    def getAmount(self) -> int:
        return self.amount

    def getLock(self) -> ReadWriteUpgradeableLock:
        return self.lock

    def setAmount(self, amount: int) -> None:
        self.amount = amount


class Bank:
    def __init__(self) -> None:
        self.balances: dict[str, BankAccount] = dict()
        self.lock = threading.Lock()

    def getLock(self) -> ReadWriteUpgradeableLock:
        return self.lock

    def getOrCreateAccount(self, accountName: str) -> BankAccount:
        account = self.balances.get(accountName)
        if account != None:
            return account
        account = BankAccount()
        self.balances[accountName] = account
        return account

    def getAccount(self, accountName: str) -> BankAccount | None:
        return self.balances.get(accountName)
