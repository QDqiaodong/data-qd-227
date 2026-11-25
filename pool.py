"""Bounded checkout for a single local SQLite database."""
from contextlib import contextmanager
from math import isfinite
import sqlite3
import threading
import time


class PoolClosed(RuntimeError):
    pass


class PoolTimeout(TimeoutError):
    pass


class ConnectionPool:
    def __init__(self, path, *, capacity):
        if type(capacity) is not int or capacity < 1:
            raise ValueError("capacity must be a positive integer")
        self._path = path
        self._capacity = capacity
        self._cond = threading.Condition()
        self._idle = []
        self._open = 0
        self._closed = False

    def _checkout(self, timeout):
        if (type(timeout) not in (int, float) or not isfinite(timeout)
                or not 0 <= timeout <= threading.TIMEOUT_MAX):
            raise ValueError("timeout must be a finite nonnegative wait")
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                if self._closed:
                    raise PoolClosed("pool has closed")
                if self._idle:
                    return self._idle.pop()
                if self._open < self._capacity:
                    db = sqlite3.connect(self._path, check_same_thread=False)
                    self._open += 1
                    return db
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise PoolTimeout("all database connections are leased")
                self._cond.wait(remaining)

    def _return(self, db):
        reusable = True
        try:
            db.rollback()
        except sqlite3.Error:
            reusable = False
        with self._cond:
            if reusable and not self._closed:
                self._idle.append(db)
            else:
                try:
                    db.close()
                finally:
                    self._open -= 1
                    self._cond.notify()
                return
            self._cond.notify()

    @contextmanager
    def borrow(self, *, timeout=1):
        """Exclusively lease a connection for this block, without auto-commit."""
        db = self._checkout(timeout)
        try:
            yield db
        finally:
            self._return(db)

    def close(self):
        """Stop admission; outstanding leases remain usable until returned."""
        with self._cond:
            self._closed = True
            while self._idle:
                self._idle.pop().close()
                self._open -= 1
            self._cond.notify_all()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
