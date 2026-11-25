"""Bounded checkout for a single local SQLite database.

Two modes are available:

* The original bounded lease mode (``borrow``), where up to ``capacity``
  connections may be leased at the same time.
* An opt-in snapshot-group mode (``snapshot_groups=True``) for the same local
  WAL database.  In this mode write leases are mutually exclusive and
  ``borrow_snapshot_group`` hands a caller a fixed batch of independent query
  connections that all observe one committed database snapshot.

Snapshot groups do not copy the database.  Each group connection holds an
open WAL read transaction that was started (and pinned by an initial read)
while the group gate prevented any writer from committing.  Writers may keep
committing after the group has been delivered; readers keep their snapshot
until the group exits.
"""
from contextlib import contextmanager
from math import isfinite
import os
import sqlite3
import stat
import threading
import time


class PoolClosed(RuntimeError):
    pass


class PoolTimeout(TimeoutError):
    pass


class _SnapshotGroup:
    """One registered group request (pending while it is being set up)."""

    __slots__ = ("connections", "state")

    def __init__(self):
        self.connections = ()
        self.state = "pending"


class ConnectionPool:
    def __init__(self, path, *, capacity, snapshot_groups=False):
        if type(snapshot_groups) is not bool:
            raise ValueError("snapshot_groups must be a boolean")
        if type(capacity) is not int or capacity < 1:
            raise ValueError("capacity must be a positive integer")
        if snapshot_groups:
            if not 3 <= capacity <= 16:
                raise ValueError(
                    "snapshot pool capacity must be between 3 and 16")
            _verify_wal_database(path)
        self._path = path
        self._capacity = capacity
        self._snapshot = snapshot_groups
        self._cond = threading.Condition()
        self._idle = []
        self._open = 0
        self._closed = False
        # Snapshot-mode state.  ``_writers_out`` is 0 or 1 because write
        # leases are mutually exclusive; ``_group`` is the single registered
        # group request (pending, being primed, delivered, or being cleaned
        # up after its caller left the with-block).
        self._writers_out = 0
        self._group = None

    # ------------------------------------------------------------------
    # Shared helpers

    def _wait(self, remaining):
        """Wait on the pool condition; a hook for deterministic tests."""
        self._cond.wait(remaining)

    def _connect(self):
        return sqlite3.connect(self._path, check_same_thread=False)

    @staticmethod
    def _validate_timeout(timeout):
        if (type(timeout) not in (int, float) or not isfinite(timeout)
                or not 0 <= timeout <= threading.TIMEOUT_MAX):
            raise ValueError("timeout must be a finite nonnegative wait")

    def _checkout(self, timeout):
        """Original mode: lease any free connection."""
        self._validate_timeout(timeout)
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                if self._closed:
                    raise PoolClosed("pool has closed")
                if self._idle:
                    return self._idle.pop()
                if self._open < self._capacity:
                    db = self._connect()
                    self._open += 1
                    return db
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise PoolTimeout("all database connections are leased")
                self._wait(remaining)

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

    # ------------------------------------------------------------------
    # Snapshot mode: write leases

    def _checkout_write(self, timeout):
        """Lease the one write slot, respecting a pending group gate."""
        self._validate_timeout(timeout)
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                if self._closed:
                    raise PoolClosed("pool has closed")
                group = self._group
                if group is not None and group.state == "pending":
                    # A group is being registered or built.  It must reach
                    # its snapshot point before any new writer may run.
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise PoolTimeout(
                            "a snapshot group is being established")
                    self._wait(remaining)
                    continue
                if self._writers_out:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise PoolTimeout("the writer slot is leased")
                    self._wait(remaining)
                    continue
                if self._idle:
                    db = self._idle.pop()
                elif self._open < self._capacity:
                    db = self._connect()
                    self._open += 1
                else:
                    # Defensive: with at most one writer and groups holding
                    # fewer than ``capacity`` connections a slot always
                    # exists, but do not spin if bookkeeping ever disagrees.
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise PoolTimeout(
                            "all database connections are leased")
                    self._wait(remaining)
                    continue
                self._writers_out = 1
                return db

    def _return_write(self, db):
        """Return the write slot; rollback finishes before it is released."""
        reusable = True
        try:
            db.rollback()
        except sqlite3.Error:
            reusable = False
        with self._cond:
            # The slot is only free once rollback has finished.
            self._writers_out = 0
            if reusable and not self._closed:
                self._idle.append(db)
            else:
                try:
                    db.close()
                finally:
                    self._open -= 1
            self._cond.notify_all()

    # ------------------------------------------------------------------
    # Snapshot mode: groups

    def _abort_locked(self, group, connections):
        """Discard group resources.  Caller holds ``self._cond``."""
        for db in connections:
            try:
                db.close()
            except sqlite3.Error:
                pass
            self._open -= 1
        if self._group is group:
            self._group = None
        self._cond.notify_all()

    def _abort_group(self, group, connections):
        """Rollback then discard group resources; safe without the lock."""
        for db in connections:
            try:
                db.rollback()
            except sqlite3.Error:
                pass
        with self._cond:
            self._abort_locked(group, connections)

    def _pin_snapshot(self, db):
        """Start a WAL read transaction and pin it with a first read.

        ``BEGIN`` only allocates the read transaction; SQLite fixes its
        visible snapshot at the first read, so that read is mandatory.
        """
        db.execute("BEGIN")
        db.execute("SELECT count(*) FROM sqlite_master").fetchall()

    def _register_group(self, count, timeout):
        """Reserve, acquire and pin a group.  Returns a delivered group."""
        self._validate_timeout(timeout)
        deadline = time.monotonic() + timeout
        with self._cond:
            if self._closed:
                raise PoolClosed("pool has closed")
            if self._group is not None:
                # At most one group request is registered at a time; the
                # second one is rejected immediately, without waiting and
                # without disturbing the existing group or write leases.
                raise PoolTimeout("a snapshot group is already registered")
            group = _SnapshotGroup()
            self._group = group
            self._cond.notify_all()

            connections = []
            try:
                # Wait (bounded by the admission timeout) for the current
                # write lease to be committed or rolled back and returned.
                # The snapshot point must come after that cleanup.
                while self._writers_out:
                    if self._closed:
                        raise PoolClosed("pool has closed")
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise PoolTimeout(
                            "write lease did not return before timeout")
                    self._wait(remaining)
                if self._closed:
                    raise PoolClosed("pool has closed")

                # No lease of any kind can be outstanding here, so every
                # open connection is idle and at most ``capacity`` slots are
                # in use.  Acquire the whole group atomically under the lock.
                while len(connections) < count:
                    if self._idle:
                        connections.append(self._idle.pop())
                    elif self._open < self._capacity:
                        db = self._connect()
                        self._open += 1
                        connections.append(db)
                    else:  # pragma: no cover - invariant guard
                        if self._closed:
                            raise PoolClosed("pool has closed")
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise PoolTimeout(
                                "not enough capacity for the group")
                        self._wait(remaining)
            except BaseException:
                self._abort_locked(group, connections)
                raise

        # Pin every connection to the same committed view.  New writers are
        # blocked by the pending group gate, so no commit can interleave
        # between these BEGIN/first-read pairs.  This work is deliberately
        # outside the admission timeout.
        try:
            for db in connections:
                self._pin_snapshot(db)
        except BaseException:
            self._abort_group(group, connections)
            raise

        with self._cond:
            if not self._closed:
                group.connections = tuple(connections)
                group.state = "delivered"
                self._cond.notify_all()
                return group
        # Closed while the snapshot was being pinned: the group must not be
        # delivered afterwards.
        self._abort_group(group, connections)
        raise PoolClosed("pool has closed")

    def _release_group(self, group):
        """End read transactions and return the group's connections."""
        reusable = []
        for db in group.connections:
            ok = True
            try:
                db.rollback()
            except sqlite3.Error:
                ok = False
            reusable.append((db, ok))
        with self._cond:
            for db, ok in reusable:
                if ok and not self._closed:
                    self._idle.append(db)
                else:
                    try:
                        db.close()
                    except sqlite3.Error:
                        pass
                    self._open -= 1
            if self._group is group:
                self._group = None
            self._cond.notify_all()

    # ------------------------------------------------------------------
    # Public API

    @contextmanager
    def borrow(self, *, timeout=1):
        """Exclusively lease a connection for this block, without auto-commit.

        In snapshot mode only one write lease exists at a time.  In the
        original mode the behavior is unchanged.
        """
        db = (self._checkout_write(timeout) if self._snapshot
              else self._checkout(timeout))
        try:
            yield db
        finally:
            if self._snapshot:
                self._return_write(db)
            else:
                self._return(db)

    def borrow_snapshot_group(self, count, timeout=1):
        """Lease ``count`` query connections bound to one committed snapshot.

        Only available on pools constructed with ``snapshot_groups=True``.
        Returns a context manager yielding a tuple of exactly ``count``
        independent connections in fixed positions.

        Arguments are validated immediately (before the block is entered), so
        a disabled pool or an invalid ``count``/``timeout`` raises
        ``ValueError`` at the call site.  Admission (waiting for the current
        write lease to return and for capacity) is bounded by ``timeout``;
        setting up the SQLite snapshot is not subject to that deadline.

        Callers must use each connection from one worker at a time, issue
        read-only SQL, keep no cursor or connection beyond the with-block,
        and must not manage transactions on the group connections.
        """
        if not self._snapshot:
            raise ValueError("snapshot groups are not enabled for this pool")
        if type(count) is not int or not 2 <= count < self._capacity:
            raise ValueError(
                "count must be an integer between 2 and capacity - 1")
        self._validate_timeout(timeout)
        return self._snapshot_group(count, timeout)

    @contextmanager
    def _snapshot_group(self, count, timeout):
        group = self._register_group(count, timeout)
        try:
            yield group.connections
        finally:
            self._release_group(group)

    @property
    def open_connections(self):
        """Number of connections the pool currently owns (idle or leased)."""
        with self._cond:
            return self._open

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


def _verify_wal_database(path):
    """Reject anything that is not an existing local WAL database file.

    The journal mode is only read, never written, so a rejected database is
    left exactly as it was.
    """
    try:
        info = os.stat(path)
    except (OSError, TypeError, ValueError) as exc:
        raise ValueError(
            "snapshot pools require an existing local WAL database") from exc
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(
            "snapshot pools require an existing local database file")
    try:
        db = sqlite3.connect(path)
    except sqlite3.Error as exc:  # pragma: no cover - unusual local paths
        raise ValueError(
            "snapshot pools require an existing local WAL database") from exc
    try:
        row = db.execute("PRAGMA journal_mode").fetchone()
    except sqlite3.Error as exc:  # pragma: no cover
        raise ValueError("could not read the database journal mode") from exc
    finally:
        db.close()
    mode = row[0] if row else None
    if not isinstance(mode, str) or mode.lower() != "wal":
        raise ValueError("snapshot pools require a WAL database")
