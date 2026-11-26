"""Bounded checkout for a single local SQLite database.

Two modes are available:

* The original bounded lease mode (``borrow``), where up to ``capacity``
  connections may be leased at the same time.
* An opt-in snapshot-group mode (``snapshot_groups=True``) for the same local
  WAL database.  In this mode write leases are mutually exclusive and
  ``borrow_snapshot_group`` hands a caller a fixed batch of independent query
  connections that all observe one committed database snapshot.

Snapshot groups do not copy the database by default.  Each group connection
holds an open WAL read transaction that was started (and pinned by an initial
read) while the group gate prevented any writer from committing.  Writers may
keep committing after the group has been delivered; readers keep their
snapshot until the group exits.

Optional WAL space maintenance (``wal_soft_limit``) adds an actual
``PRAGMA wal_checkpoint(TRUNCATE)`` based reclamation entry point
(:meth:`ConnectionPool.reclaim_wal`) and write-borrow admission: whenever a
new write lease would be granted at or above the limit, the pool first tries
to really shrink the ``-wal`` file and only grants the lease once the measured
file length is below the limit.  When an old snapshot group makes truncation
impossible, the borrow is rejected with :class:`PoolWalPressure` instead of
letting the WAL grow without bound.

When an existing, usable ``wal_scratch_dir`` is supplied together with the
limit, each delivered snapshot group is additionally materialized as one
shared on-disk snapshot in that directory; group queries then read the
snapshot file rather than pinning the main WAL, so the WAL can still be
truncated while a long reconciliation group keeps its old view.  Without the
directory no copy is ever made and groups keep the main WAL read view.
"""
from contextlib import contextmanager
from math import isfinite
import os
import sqlite3
import stat
import tempfile
import threading
import time


class PoolClosed(RuntimeError):
    pass


class PoolTimeout(TimeoutError):
    pass


class PoolWalPressure(RuntimeError):
    """WAL is at/over the soft limit and cannot be actually reclaimed yet.

    Raised at write-borrow admission when a real truncate attempt did not get
    the measured ``-wal`` length below :data:`wal_soft_limit`, typically
    because a delivered snapshot group still pins an old WAL view.  No
    connection is handed out and the caller is not expected to wait for the
    old group; once the obstruction goes away a later borrow reclaims the WAL
    itself and proceeds.
    """


#: Inclusive accepted range for ``wal_soft_limit`` (bytes).
WAL_SOFT_LIMIT_MIN = 65536
WAL_SOFT_LIMIT_MAX = 16777216

#: Hard ceiling for one group's scratch snapshot resource (database plus
#: sidecar files), per the deployment contract.
WAL_SCRATCH_TOTAL_MAX = 72 * 1024 * 1024


class _SnapshotGroup:
    """One registered group request (pending while it is being set up)."""

    __slots__ = ("connections", "state", "scratch_path")

    def __init__(self):
        self.connections = ()
        self.state = "pending"
        # When scratch snapshots are enabled, the unique base path of the
        # one shared snapshot file for this whole group (sidecars share the
        # path plus -wal/-shm/-journal suffixes).  ``None`` means the group
        # pins the main database WAL view instead.
        self.scratch_path = None


class ConnectionPool:
    def __init__(self, path, *, capacity, snapshot_groups=False,
                 wal_soft_limit=None, wal_scratch_dir=None):
        if type(snapshot_groups) is not bool:
            raise ValueError("snapshot_groups must be a boolean")
        if type(capacity) is not int or capacity < 1:
            raise ValueError("capacity must be a positive integer")

        # WAL maintenance configuration is validated before any state is set
        # up so an illegal constructor call leaves no resource behind.
        if wal_soft_limit is not None:
            if type(wal_soft_limit) is not int:
                # Exact-type check rejects bool and float subclasses.
                raise ValueError(
                    "wal_soft_limit must be a non-boolean integer byte count")
            if not (WAL_SOFT_LIMIT_MIN <= wal_soft_limit
                    <= WAL_SOFT_LIMIT_MAX):
                raise ValueError(
                    "wal_soft_limit must be between %d and %d bytes"
                    % (WAL_SOFT_LIMIT_MIN, WAL_SOFT_LIMIT_MAX))
            if not snapshot_groups:
                raise ValueError(
                    "wal_soft_limit requires snapshot_groups=True")
        if wal_scratch_dir is not None and wal_soft_limit is None:
            raise ValueError(
                "wal_scratch_dir requires wal_soft_limit to be set")
        scratch_path = None
        if wal_scratch_dir is not None:
            scratch_path = _validate_scratch_dir(wal_scratch_dir)

        if snapshot_groups:
            if not 3 <= capacity <= 16:
                raise ValueError(
                    "snapshot pool capacity must be between 3 and 16")
            _verify_wal_database(path)
        self._path = path
        self._path_str = os.fsdecode(os.fspath(path))
        self._wal_path = self._path_str + "-wal"
        self._capacity = capacity
        self._snapshot = snapshot_groups
        self._wal_limit = wal_soft_limit
        self._wal_scratch_dir = scratch_path
        self._cond = threading.Condition()
        self._idle = []
        self._open = 0
        # High-water mark of ``_open`` for capacity tests/ops inspection.
        self._open_peak = 0
        self._closed = False
        # Snapshot-mode state.  ``_writers_out`` is 0 or 1 because write
        # leases are mutually exclusive; ``_group`` is the single registered
        # group request (pending, being primed, delivered, or being cleaned
        # up after its caller left the with-block).
        self._writers_out = 0
        self._group = None
        # Gate held by the pool's own ``reclaim_wal`` checkpoint so write
        # admission and group establishment cannot interleave with it.
        self._maintaining = False

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
                    self._bump_open_locked()
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
        if self._wal_limit is None:
            return self._checkout_write_unlimited(timeout)
        return self._checkout_write_wal(timeout)

    def _checkout_write_unlimited(self, timeout):
        """Original snapshot-mode admission, without WAL thresholds."""
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
                    self._bump_open_locked()
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

    def _checkout_write_wal(self, timeout):
        """Write admission with a measured WAL soft-limit gate.

        All pool-state waits keep the original ``timeout`` semantics.  Once
        the write slot and a connection are acquired, the WAL length is
        sampled freshly; at/over the limit the pool attempts a real truncate
        on the leased connection and only hands it out after a fresh sample
        below the limit.  A truncate blocked by an old reader raises
        :class:`PoolWalPressure` and returns the slot cleanly.
        """
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                if self._closed:
                    raise PoolClosed("pool has closed")
                group = self._group
                if group is not None and group.state == "pending":
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise PoolTimeout(
                            "a snapshot group is being established")
                    self._wait(remaining)
                    continue
                if self._maintaining:
                    # An explicit reclaim_wal checkpoint is running with
                    # its maintenance gate held.
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise PoolTimeout("WAL maintenance is in progress")
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
                    self._bump_open_locked()
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise PoolTimeout(
                            "all database connections are leased")
                    self._wait(remaining)
                    continue
                # The write slot is ours until _return_write; group setup and
                # other writers order behind it.
                self._writers_out = 1
                break

        try:
            self._admit_under_wal_limit(db)
        except BaseException:
            # Never leave a half-granted write lease or gate behind.
            self._return_write(db)
            raise
        return db

    # ------------------------------------------------------------------
    # WAL maintenance and pressure admission

    def _wal_length(self):
        """Actual main-database ``-wal`` file length; absent file is 0."""
        try:
            return os.path.getsize(self._wal_path)
        except FileNotFoundError:
            return 0

    def _bump_open_locked(self):
        """Account one newly opened pool connection and update the peak."""
        self._open += 1
        if self._open > self._open_peak:
            self._open_peak = self._open

    def _run_truncate_checkpoint(self, db):
        """Invoke the real SQLite TRUNCATE checkpoint without reader waits.

        Python's default connection busy timeout (5 s) would make
        ``wal_checkpoint(TRUNCATE)`` sleep waiting for old snapshot readers;
        the maintenance contract classifies that case immediately as
        ``reader_busy`` instead.  Temporarily set ``busy_timeout=0`` for the
        attempt and restore the connection's previous setting afterwards, so
        a pooled connection keeps its original lock-wait behavior for
        business transactions.  SQLite errors propagate unchanged.
        """
        try:
            row = db.execute("PRAGMA busy_timeout").fetchone()
            previous = int(row[0]) if row and row[0] is not None else None
        except sqlite3.Error:
            previous = None
        try:
            if previous != 0:
                db.execute("PRAGMA busy_timeout=0")
            return db.execute(
                "PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        finally:
            if previous and previous > 0:
                try:
                    db.execute("PRAGMA busy_timeout=%d" % previous)
                except sqlite3.Error:
                    # A broken connection is discarded by the return path;
                    # never mask the original checkpoint result/error.
                    pass

    def _admit_under_wal_limit(self, db):
        """Reclaim first when the current WAL is at/over the soft limit.

        Called with the exclusive write slot held but before the connection
        is handed to business code.  The decision uses fresh file samples,
        never a measurement taken before queueing.
        """
        if self._wal_length() < self._wal_limit:
            return
        self._run_truncate_checkpoint(db)
        if self._wal_length() < self._wal_limit:
            return
        raise PoolWalPressure(
            "WAL is at or above wal_soft_limit=%d and could not be "
            "truncated; an old snapshot group may still pin it"
            % self._wal_limit)

    def reclaim_wal(self):
        """Actively try to shrink the main WAL to zero bytes.

        Returns ``(status, before_bytes, after_bytes)`` where ``status`` is:

        * ``"reclaimed"`` -- the measured WAL is now zero bytes (this also
          covers the already-zero no-work case, returned as ``(0, 0)``);
        * ``"lease_busy"`` -- a business write lease is out (including its
          return rollback cleanup) or a snapshot group is being established.
          One file sample fills both byte fields; no checkpoint is attempted
          and the call does not wait;
        * ``"reader_busy"`` -- no write/build is in flight, but a delivered
          snapshot group (or another SQLite reader obstruction under the
          single-pool contract) prevents actual truncation; the fields are
          the real measured lengths before and after the attempt.

        Only a measured zero-length WAL yields ``"reclaimed"``.  Real SQLite
        or file-operation errors propagate untouched.  Must be enabled via
        ``wal_soft_limit``; otherwise :class:`ValueError`.  Calls after
        :meth:`close` raise :class:`PoolClosed`.
        """
        if self._wal_limit is None:
            raise ValueError(
                "WAL maintenance is not enabled (set wal_soft_limit)")

        db = None
        before = 0
        with self._cond:
            if self._closed:
                raise PoolClosed("pool has closed")
            while self._maintaining:
                # Order concurrent maintenance calls instead of running two
                # checkpoints at once; the previous one finishes quickly.
                if self._closed:
                    raise PoolClosed("pool has closed")
                self._cond.wait()
            if self._closed:
                raise PoolClosed("pool has closed")
            group = self._group
            if self._writers_out or (
                    group is not None and group.state == "pending"):
                # One coherent sample, copied into both result columns.
                size = self._wal_length()
                return "lease_busy", size, size

            # The sample and the reservation happen atomically under the
            # pool state lock, so it is a protected decision sample.
            before = self._wal_length()
            if before == 0:
                # Nothing to reclaim: do not open a maintenance connection.
                return "reclaimed", 0, 0

            # Reserve the maintenance connection through the same capacity
            # accounting as every other pool connection.
            if self._idle:
                db = self._idle.pop()
            elif self._open < self._capacity:
                db = self._connect()
                self._bump_open_locked()
            else:  # pragma: no cover - invariant guard
                # With no writer and no pending group, any full pool has idle
                # connections; wait on the same condition if the bookkeeping
                # invariant is ever disturbed.
                while not self._idle:
                    if self._closed:
                        raise PoolClosed("pool has closed")
                    self._cond.wait()
                db = self._idle.pop()
            self._maintaining = True

        try:
            # Any sqlite3.Error / OSError leaves here untouched; the finally
            # below only restores pool bookkeeping.
            self._run_truncate_checkpoint(db)
            after = self._wal_length()
            if after == 0:
                return "reclaimed", before, 0
            # Under the single-process exclusive-pool contract the only
            # remaining truncation obstruction is a delivered read group.
            return "reader_busy", before, after
        finally:
            self._return_maintenance(db)

    def _return_maintenance(self, db):
        """Return/close the reclaim connection and release the gate."""
        reusable = True
        try:
            db.rollback()
        except sqlite3.Error:
            reusable = False
        with self._cond:
            self._maintaining = False
            if reusable and not self._closed:
                self._idle.append(db)
            else:
                try:
                    db.close()
                finally:
                    self._open -= 1
            self._cond.notify_all()

    # ------------------------------------------------------------------
    # Scratch snapshots

    def _reserve_extra_slot_locked(self):
        """Make ``_open < _capacity`` by closing idle pooled connections.

        The backup destination and scratch group connections count against
        capacity just like every other connection; when the counter is full
        an unused idle main connection may be closed to free its slot, but no
        connection is ever opened beyond ``capacity``.  Caller holds cond.
        """
        while self._open >= self._capacity:
            if self._closed:
                raise PoolClosed("pool has closed")
            if self._idle:
                old = self._idle.pop()
                try:
                    old.close()
                except sqlite3.Error:
                    pass
                self._open -= 1
                return
            # Defensive: should not be reachable for the one-source plus
            # count-scratch setup with count < capacity.
            self._cond.wait()

    def _open_counted_scratch_connection_path(self, path):
        """Open one connection to the shared snapshot, counting capacity."""
        with self._cond:
            self._reserve_extra_slot_locked()
            db = sqlite3.connect(path, check_same_thread=False)
            self._bump_open_locked()
        return db

    def _create_group_snapshot(self, source):
        """Materialize the source's pinned view as one shared scratch file.

        Returns the unique base file path.  The Online Backup API reads the
        exact snapshot currently pinned by ``source`` (verified against real
        SQLite WAL behavior), so late first queries on the scratch
        connections still see the group's framed view.
        """
        fd, snap_path = tempfile.mkstemp(
            prefix="pool-wal-snap-", suffix=".db",
            dir=self._wal_scratch_dir)
        os.close(fd)
        dest = None
        opened = False
        try:
            with self._cond:
                self._reserve_extra_slot_locked()
                dest = sqlite3.connect(snap_path, check_same_thread=False)
                self._bump_open_locked()
                opened = True
            try:
                # Default to a single rollback-journal file so readers do
                # not grow a second WAL/SHM sidecar in the scratch budget.
                dest.execute("PRAGMA journal_mode=DELETE")
                source.backup(dest)
                # The copied page-1 header still says WAL; convert the
                # finished snapshot back to DELETE and drop its WAL sidecar.
                dest.execute("PRAGMA journal_mode=DELETE")
            finally:
                if dest is not None:
                    try:
                        dest.close()
                    finally:
                        if opened:
                            with self._cond:
                                self._open -= 1
                                self._cond.notify_all()
                            opened = False
            total = _scratch_resource_bytes(snap_path)
            if total > WAL_SCRATCH_TOTAL_MAX:
                raise ValueError(
                    "group scratch snapshot needs %d bytes, over the %d "
                    "byte scratch limit" % (total, WAL_SCRATCH_TOTAL_MAX))
            return snap_path
        except BaseException:
            _remove_scratch_files(snap_path)
            raise

    def _register_scratch_group(self, count, timeout):
        """Register a group whose connections read one shared scratch copy."""
        deadline = time.monotonic() + timeout
        with self._cond:
            if self._closed:
                raise PoolClosed("pool has closed")
            if self._group is not None:
                raise PoolTimeout("a snapshot group is already registered")
            group = _SnapshotGroup()
            self._group = group
            self._cond.notify_all()

            source = None
            try:
                # Order after the current write lease AND any pool
                # maintenance checkpoint.
                while self._writers_out or self._maintaining:
                    if self._closed:
                        raise PoolClosed("pool has closed")
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise PoolTimeout(
                            "write lease did not return before timeout")
                    self._wait(remaining)
                if self._closed:
                    raise PoolClosed("pool has closed")

                # One main connection is enough to fix and copy the view.
                if self._idle:
                    source = self._idle.pop()
                elif self._open < self._capacity:
                    source = self._connect()
                    self._bump_open_locked()
                else:  # pragma: no cover - invariant guard
                    while not self._idle:
                        if self._closed:
                            raise PoolClosed("pool has closed")
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise PoolTimeout(
                                "not enough capacity for the group")
                        self._wait(remaining)
                    source = self._idle.pop()
            except BaseException:
                self._abort_locked(group, ())
                raise

        scratch_conns = []
        scratch_path = None
        try:
            # Pin the exact committed view before backing it up.
            self._pin_snapshot(source)
            scratch_path = self._create_group_snapshot(source)
            group.scratch_path = scratch_path

            for _ in range(count):
                scratch_conns.append(
                    self._open_counted_scratch_connection_path(scratch_path))
            # Fix each scratch connection's read snapshot as well; the file
            # is private and immutable afterwards, but this keeps the same
            # explicit framing as main-WAL groups.
            for db in scratch_conns:
                self._pin_snapshot(db)
        except BaseException:
            self._abort_scratch_setup(
                group, source, scratch_conns, scratch_path)
            raise

        # The main source connection is reusable immediately; the scratch
        # connections carry the delivered view.
        self._return(source)

        with self._cond:
            if self._closed:
                self._discard_scratch_connections_locked(scratch_conns)
                if self._group is group:
                    self._group = None
                self._cond.notify_all()
            else:
                group.connections = tuple(scratch_conns)
                group.state = "delivered"
                self._cond.notify_all()
                return group

        _remove_scratch_files(scratch_path)
        raise PoolClosed("pool has closed")

    def _abort_scratch_setup(self, group, source, scratch_conns, scratch_path):
        """Clean up a failed scratch group build without leaking resources."""
        for db in scratch_conns:
            try:
                db.rollback()
            except sqlite3.Error:
                pass
        if source is not None:
            try:
                source.rollback()
            except sqlite3.Error:
                pass
        with self._cond:
            self._discard_scratch_connections_locked(scratch_conns)
            if source is not None:
                try:
                    source.close()
                except sqlite3.Error:
                    pass
                self._open -= 1
            if self._group is group:
                self._group = None
            self._cond.notify_all()
        if scratch_path is not None:
            _remove_scratch_files(scratch_path)

    def _discard_scratch_connections_locked(self, connections):
        """Close one-use scratch connections and balance ``_open``."""
        for db in connections:
            try:
                db.close()
            except sqlite3.Error:
                pass
            self._open -= 1

    def _release_scratch_group(self, group):
        """Close the group's scratch connections and remove its snapshot."""
        for db in group.connections:
            try:
                db.rollback()
            except sqlite3.Error:
                pass
        with self._cond:
            self._discard_scratch_connections_locked(group.connections)
            if self._group is group:
                self._group = None
            self._cond.notify_all()
        _remove_scratch_files(group.scratch_path)

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
        if self._wal_scratch_dir is not None:
            return self._register_scratch_group(count, timeout)
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
                # The snapshot point must come after that cleanup.  A pool
                # reclaim checkpoint is ordered ahead of group pinning too.
                while self._writers_out or self._maintaining:
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
                        self._bump_open_locked()
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
        if group.scratch_path is not None:
            self._release_scratch_group(group)
            return
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

    @property
    def peak_open_connections(self):
        """High-water mark of :attr:`open_connections` for this pool."""
        with self._cond:
            return self._open_peak

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


def _validate_scratch_dir(directory):
    """Validate an existing, usable caller-provided scratch directory."""
    try:
        path = os.fspath(directory)
    except TypeError as exc:
        raise ValueError("wal_scratch_dir must be a directory path") from exc
    if not os.path.isdir(path):
        raise ValueError(
            "wal_scratch_dir must be an existing usable directory: %r"
            % (directory,))
    probe = None
    try:
        # Actually create and remove a private entry: permission bits or a
        # read-only mount otherwise only fail later under pressure.
        probe = tempfile.mkdtemp(prefix=".pool-wal-probe-", dir=path)
        os.rmdir(probe)
    except OSError as exc:
        raise ValueError(
            "wal_scratch_dir is not writable/usable: %r" % (directory,)
        ) from exc
    return path


def _scratch_resource_bytes(base_path):
    """Total bytes of one snapshot plus its known SQLite sidecars."""
    total = 0
    for suffix in ("", "-wal", "-shm", "-journal"):
        try:
            total += os.path.getsize(base_path + suffix)
        except FileNotFoundError:
            pass
    return total


def _remove_scratch_files(base_path):
    """Remove exactly one pool-created snapshot and its sidecars.

    The pool only ever passes a path it created with ``mkstemp`` inside the
    caller-provided directory, so caller-pre-placed files are never touched.
    Missing files are not an error; other filesystem errors propagate.
    """
    if base_path is None:
        return
    for suffix in ("", "-wal", "-shm", "-journal"):
        try:
            os.unlink(base_path + suffix)
        except FileNotFoundError:
            pass


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
