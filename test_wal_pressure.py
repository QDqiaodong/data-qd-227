"""WAL soft-limit maintenance and write-borrow admission regressions.

Pure standard library, real temporary WAL SQLite databases.  Covers the
three acceptance areas:

1. measured ``-wal`` pressure, ``reclaim_wal`` status triples, lease-busy vs
   reader-busy classification, fresh re-admission of waiting writers and
   raw propagation of real SQLite maintenance failures;
2. the exact two-tenant dispatch scenario under a capacity-3 double read
   group without a scratch directory: committed callback payload survives,
   the group keeps its old view, maintenance reports ``reader_busy`` and
   dispatch raises :class:`PoolWalPressure` without touching dispatch state;
3. the same scenario with a scratch directory: the old group is served by
   one shared scratch snapshot, the main WAL is really truncated and
   dispatch commits; capacity peak and scratch directory accounting,
   pre-placed file survival and close/return cleanup are all checked.

Concurrency is driven by events/hooks, never by sleeps.
"""

import os
import sqlite3
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory

from dispatch import dispatch_wave, initialize_dispatch
from pool import (
    WAL_SCRATCH_TOTAL_MAX,
    ConnectionPool,
    PoolClosed,
    PoolTimeout,
    PoolWalPressure,
)
from quota import ReservationIndex
from test_dispatch import create_dispatch_db, dump_database, finish_backfill


LIMIT = 65536  # acceptance uses a 64 KiB soft limit (the accepted minimum)
PAYLOAD = b"W" * 200_000


def wal_size(path):
    try:
        return os.path.getsize(os.fspath(path) + "-wal")
    except FileNotFoundError:
        return 0


def make_wal_database(path):
    db = sqlite3.connect(path)
    try:
        mode = db.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        assert mode.lower() == "wal"
        db.execute("CREATE TABLE ledger(name TEXT PRIMARY KEY, amount INTEGER)")
        db.execute("INSERT INTO ledger VALUES ('detail', 10), ('balance', 10)")
        db.commit()
    finally:
        db.close()


def add_big_payload(pool, *, table="big_payload"):
    """Commit an unrelated large blob that reliably crosses 64 KiB of WAL."""
    with pool.borrow(timeout=5) as db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS %s ("
            "id INTEGER PRIMARY KEY, payload BLOB)" % table)
        db.execute(
            "INSERT INTO %s(payload) VALUES (?)" % table, (PAYLOAD,))
        db.commit()


class WalConfigValidationTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "cfg.db"
        make_wal_database(self.path)

    def test_accepted_limit_boundaries(self):
        for value in (65536, 16777216):
            pool = ConnectionPool(
                self.path, capacity=3, snapshot_groups=True,
                wal_soft_limit=value)
            try:
                self.assertEqual(pool.reclaim_wal()[0], "reclaimed")
            finally:
                pool.close()

    def test_invalid_limits(self):
        for value in (65535, 16777217, 0, -1, 1024, True, False,
                      65536.0, "65536"):
            with self.assertRaises(ValueError):
                ConnectionPool(
                    self.path, capacity=3, snapshot_groups=True,
                    wal_soft_limit=value)

    def test_limit_requires_snapshot_mode(self):
        with self.assertRaises(ValueError):
            ConnectionPool(
                self.path, capacity=3, wal_soft_limit=LIMIT)
        with self.assertRaises(ValueError):
            ConnectionPool(
                self.path, capacity=3, snapshot_groups=False,
                wal_soft_limit=LIMIT)

    def test_scratch_dir_requires_limit(self):
        scratch = self.root / "scratch"
        scratch.mkdir()
        with self.assertRaises(ValueError):
            ConnectionPool(
                self.path, capacity=3, snapshot_groups=True,
                wal_scratch_dir=scratch)

    def test_invalid_scratch_dirs(self):
        a_file = self.root / "a-file"
        a_file.write_text("not a directory")
        missing = self.root / "missing-dir"
        for bad in (missing, a_file):
            with self.assertRaises(ValueError):
                ConnectionPool(
                    self.path, capacity=3, snapshot_groups=True,
                    wal_soft_limit=LIMIT, wal_scratch_dir=bad)

    @unittest.skipIf(os.geteuid() == 0, "root bypasses directory permissions")
    def test_unwritable_scratch_dir_rejected(self):
        scratch = self.root / "ro-scratch"
        scratch.mkdir()
        scratch.chmod(0o500)
        self.addCleanup(lambda: scratch.chmod(0o700))
        with self.assertRaises(ValueError):
            ConnectionPool(
                self.path, capacity=3, snapshot_groups=True,
                wal_soft_limit=LIMIT, wal_scratch_dir=scratch)

    def test_maintenance_entry_requires_enabled_limit(self):
        plain = ConnectionPool(self.path, capacity=3)
        self.addCleanup(plain.close)
        snap = ConnectionPool(self.path, capacity=3, snapshot_groups=True)
        self.addCleanup(snap.close)
        for pool in (plain, snap):
            with self.assertRaises(ValueError):
                pool.reclaim_wal()

    def test_reclaim_after_close_raises_pool_closed(self):
        pool = ConnectionPool(
            self.path, capacity=3, snapshot_groups=True,
            wal_soft_limit=LIMIT)
        pool.close()
        with self.assertRaises(PoolClosed):
            pool.reclaim_wal()


class _CheckpointHookPool(ConnectionPool):
    """Records truncate checkpoints and can inject/block inside them."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.checkpoint_calls = 0
        self.sizes_at_checkpoint = []
        self.fail_checkpoint = False
        self.block_entered = None
        self.block_release = None

    def _run_truncate_checkpoint(self, db):
        self.checkpoint_calls += 1
        self.sizes_at_checkpoint.append(self._wal_length())
        if self.fail_checkpoint:
            raise sqlite3.OperationalError("injected maintenance failure")
        if self.block_entered is not None:
            self.block_entered.set()
            if not self.block_release.wait(5):
                raise RuntimeError("blocked checkpoint never released")
        return super()._run_truncate_checkpoint(db)


class _WaitingObserverPool(_CheckpointHookPool):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.started_waiting = threading.Event()

    def _wait(self, remaining):
        self.started_waiting.set()
        super()._wait(remaining)


class _FlakyStatPool(ConnectionPool):
    fail_next_stat = False

    def _wal_length(self):
        if type(self).fail_next_stat:
            type(self).fail_next_stat = False
            raise PermissionError("injected stat failure")
        return super()._wal_length()


class WalReclaimAdmissionTest(unittest.TestCase):
    """Acceptance item 1 on a real temporary WAL database."""

    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "pressure.db"
        make_wal_database(self.path)
        self.pool = self.make_pool()

    def make_pool(self, cls=ConnectionPool, **kwargs):
        pool = cls(
            self.path, capacity=3, snapshot_groups=True,
            wal_soft_limit=LIMIT, **kwargs)
        self.addCleanup(pool.close)
        # Deterministic zero starting point regardless of setup close hooks.
        self.assertEqual(pool.reclaim_wal(), ("reclaimed", 0, 0))
        return pool

    def test_empty_wal_reclaimed_zero_zero_without_open_pressure(self):
        before_open = self.pool.open_connections
        status, before, after = self.pool.reclaim_wal()
        self.assertEqual((status, before, after), ("reclaimed", 0, 0))
        # The no-work path need not open a maintenance connection.
        self.assertEqual(self.pool.open_connections, before_open)

    def test_leased_write_is_lease_busy_even_after_commit(self):
        observed = {}
        with self.pool.borrow(timeout=5) as db:
            db.execute(
                "CREATE TABLE big(id INTEGER PRIMARY KEY, b BLOB)")
            db.execute("INSERT INTO big(b) VALUES (?)", (PAYLOAD,))
            db.commit()
            # The granted lease is only an admission threshold: the same
            # checked-out connection may keep writing and committing after
            # its own transaction first pushed the WAL over the line.
            db.execute("INSERT INTO big(b) VALUES (?)", (PAYLOAD,))
            db.commit()
            # Commit succeeded but the lease is still checked out: the pool
            # must not seize the write slot just because the tx ended.
            actual = wal_size(self.path)
            self.assertGreaterEqual(actual, LIMIT)
            status, before, after = self.pool.reclaim_wal()
            observed["triple"] = (status, before, after)
            self.assertEqual(status, "lease_busy")
            self.assertEqual(before, after)
            self.assertEqual(before, actual)
            # The lease remains fully usable after the busy probe.
            self.assertEqual(
                db.execute("SELECT count(*) FROM big").fetchone(), (2,))
        self.assertEqual(
            observed["triple"], ("lease_busy", actual, actual))

    def test_reclaim_after_return_zeroes_wal_and_keeps_data_readable(self):
        with self.pool.borrow(timeout=5) as db:
            db.execute(
                "CREATE TABLE big(id INTEGER PRIMARY KEY, b BLOB)")
            db.execute("INSERT INTO big(b) VALUES (?)", (PAYLOAD,))
            db.commit()
            committed_bytes = wal_size(self.path)
        status, before, after = self.pool.reclaim_wal()
        self.assertEqual(status, "reclaimed")
        self.assertEqual(before, committed_bytes)
        self.assertEqual(after, 0)
        self.assertEqual(wal_size(self.path), 0)

        # Committed data is still there through the pool ...
        with self.pool.borrow(timeout=5) as db:
            self.assertEqual(
                db.execute("SELECT length(b) FROM big").fetchone(),
                (len(PAYLOAD),))
            self.assertEqual(
                db.execute(
                    "SELECT amount FROM ledger WHERE name='detail'"
                ).fetchone(),
                (10,))
        # ... and through a brand new independent SQLite connection.
        witness = sqlite3.connect(self.path)
        try:
            self.assertEqual(
                witness.execute("SELECT length(b) FROM big").fetchone(),
                (len(PAYLOAD),))
        finally:
            witness.close()

    def test_waiting_writer_rechecks_fresh_wal_after_previous_return(self):
        pool = self.make_pool(_WaitingObserverPool)

        def waiting_borrow():
            with pool.borrow(timeout=5) as db:
                granted_wal = pool._wal_length()
                db.execute("INSERT INTO ledger VALUES ('late', 77)")
                db.commit()
                return granted_wal

        with ThreadPoolExecutor(max_workers=1) as workers:
            with pool.borrow(timeout=5) as first:
                pending = workers.submit(waiting_borrow)
                self.assertTrue(pool.started_waiting.wait(5))
                # The queued writer measured nothing reusable: only now does
                # the holder push the WAL across the threshold.
                first.execute(
                    "CREATE TABLE big(id INTEGER PRIMARY KEY, b BLOB)")
                first.execute(
                    "INSERT INTO big(b) VALUES (?)", (PAYLOAD,))
                first.commit()
                self.assertGreaterEqual(wal_size(self.path), LIMIT)
            # First lease returned here; the waiter must re-measure, reclaim
            # for real and only then take the write slot.
            granted_wal = pending.result(timeout=10)

        self.assertEqual(granted_wal, 0)
        self.assertGreaterEqual(pool.checkpoint_calls, 1)
        self.assertGreaterEqual(pool.sizes_at_checkpoint[0], LIMIT)
        with sqlite3.connect(self.path) as witness:
            self.assertEqual(
                witness.execute(
                    "SELECT amount FROM ledger WHERE name='late'"
                ).fetchone(),
                (77,))

    def test_timeout_still_applies_to_busy_write_slot(self):
        with self.pool.borrow(timeout=5):
            with self.assertRaises(PoolTimeout):
                with self.pool.borrow(timeout=0):
                    self.fail("write slot must not be granted twice")

    def test_under_limit_group_does_not_block_writes(self):
        with self.pool.borrow_snapshot_group(2, timeout=5) as readers:
            readers[0].execute("SELECT count(*) FROM ledger").fetchall()
            for value in range(5):
                with self.pool.borrow(timeout=5) as db:
                    db.execute(
                        "INSERT INTO ledger VALUES (?, ?)",
                        ("k%d" % value, value))
                    db.commit()
            self.assertLess(wal_size(self.path), LIMIT)
            # The delivered group keeps its old three-row-or-two view.
            self.assertEqual(
                readers[1].execute(
                    "SELECT count(*) FROM ledger").fetchone(),
                (2,))

    def test_pressure_is_independent_exception_and_self_clears(self):
        with self.pool.borrow_snapshot_group(2, timeout=5) as readers:
            add_big_payload(self.pool)
            self.assertEqual(pool_reclaim(self.pool)[0], "reader_busy")
            with self.assertRaises(PoolWalPressure) as caught:
                with self.pool.borrow(timeout=5):
                    self.fail("no connection may be handed out under pressure")
            self.assertIsInstance(caught.exception, RuntimeError)
            self.assertNotIsInstance(caught.exception, PoolTimeout)
            # No lease/gate residue.
            self.assertEqual(self.pool._writers_out, 0)
            self.assertFalse(self.pool._maintaining)
            readers[0].execute("SELECT 1").fetchall()

        # No manual unlock/reclaim: after the old group exits, the next
        # borrow reclaims itself and proceeds.
        with self.pool.borrow(timeout=5) as db:
            self.assertEqual(db.execute("SELECT 42").fetchone(), (42,))
        self.assertLess(wal_size(self.path), LIMIT)

    def test_sqlite_maintenance_error_propagates_and_leaves_no_gate(self):
        pool = self.make_pool(_CheckpointHookPool)
        add_big_payload(pool)
        pool.fail_checkpoint = True
        with self.assertRaisesRegex(
                sqlite3.OperationalError, "injected maintenance failure"):
            pool.reclaim_wal()
        # Failure must not be reclassified as busy and must leave no residue.
        self.assertFalse(pool._maintaining)
        self.assertEqual(pool._writers_out, 0)
        self.assertIsNone(pool._group)
        self.assertLessEqual(pool.open_connections, 3)

        pool.fail_checkpoint = False
        status, before, after = pool.reclaim_wal()
        self.assertEqual(status, "reclaimed")
        self.assertGreaterEqual(before, LIMIT)
        self.assertEqual(after, 0)
        with pool.borrow(timeout=0) as db:
            self.assertEqual(db.execute("SELECT 1").fetchone(), (1,))

    def test_sqlite_error_during_admission_propagates_raw(self):
        pool = self.make_pool(_CheckpointHookPool)
        add_big_payload(pool)
        pool.fail_checkpoint = True
        with self.assertRaisesRegex(
                sqlite3.OperationalError, "injected maintenance failure"):
            with pool.borrow(timeout=5):
                self.fail("admission must not succeed when checkpoint fails")
        # The half-granted write lease was returned exactly once.
        self.assertEqual(pool._writers_out, 0)
        self.assertFalse(pool._maintaining)
        pool.fail_checkpoint = False
        with pool.borrow(timeout=5) as db:
            self.assertEqual(db.execute("SELECT 1").fetchone(), (1,))

    def test_file_stat_error_propagates_raw(self):
        pool = self.make_pool(_FlakyStatPool)
        add_big_payload(pool)
        _FlakyStatPool.fail_next_stat = True
        try:
            with self.assertRaises(PermissionError):
                pool.reclaim_wal()
        finally:
            _FlakyStatPool.fail_next_stat = False
        self.assertFalse(pool._maintaining)
        self.assertEqual(pool._writers_out, 0)
        status, _before, after = pool.reclaim_wal()
        self.assertEqual((status, after), ("reclaimed", 0))

    def test_maintenance_gate_orders_concurrent_borrows(self):
        pool = self.make_pool(_CheckpointHookPool)
        pool.block_entered = threading.Event()
        pool.block_release = threading.Event()
        add_big_payload(pool)

        with ThreadPoolExecutor(max_workers=1) as workers:
            reclaimed = workers.submit(pool.reclaim_wal)
            self.assertTrue(pool.block_entered.wait(5))
            # While the pool's own checkpoint runs, new write admission
            # waits on the maintenance gate and honors timeout=0.
            with self.assertRaises(PoolTimeout):
                with pool.borrow(timeout=0):
                    self.fail("writer admitted during maintenance")
            pool.block_release.set()
            status, _b, after = reclaimed.result(timeout=10)
            self.assertEqual((status, after), ("reclaimed", 0))

        with pool.borrow(timeout=5) as db:
            self.assertEqual(db.execute("SELECT 1").fetchone(), (1,))


def pool_reclaim(pool):
    return pool.reclaim_wal()


class DispatchPressureScenarioTest(unittest.TestCase):
    """Acceptance item 2: exact two-tenant dispatch scenario, no scratch."""

    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "dispatch.db"
        create_dispatch_db(self.path)
        self.pool = ConnectionPool(
            self.path, capacity=3, snapshot_groups=True,
            wal_soft_limit=LIMIT)
        self.addCleanup(self.pool.close)
        index = ReservationIndex(self.pool)
        index.initialize()
        initialize_dispatch(self.pool)
        finish_backfill(index, (1, 2, 3))
        status, _before, after = self.pool.reclaim_wal()
        self.assertEqual((status, after), ("reclaimed", 0))
        # An unrelated table with one small committed row supplies the
        # viewpoint left in WAL before the double group is opened.
        with self.pool.borrow(timeout=5) as db:
            db.execute(
                "CREATE TABLE wave_payload("
                "id INTEGER PRIMARY KEY, payload BLOB)")
            db.execute(
                "INSERT INTO wave_payload(payload) VALUES (?)",
                (b"baseline",))
            db.commit()
        self.assertLess(wal_size(self.path), LIMIT)
        self.workers = [
            (101, 1001, [1, 2]),
            (102, 1002, [1]),
        ]

    def _callback_commits_big_payload(self):
        with self.pool.borrow(timeout=5) as db:
            db.execute(
                "INSERT INTO wave_payload(payload) VALUES (?)", (PAYLOAD,))
            db.commit()

    def test_dispatch_pressure_then_recovery_after_group_exits(self):
        before_dump = dump_database(self.path)

        with self.pool.borrow_snapshot_group(2, timeout=5) as readers:
            # Early first query pins/uses the delivered snapshot.
            self.assertEqual(
                readers[0].execute(
                    "SELECT count(*) FROM dispatch_ready").fetchone(),
                (3,))

            with self.assertRaises(PoolWalPressure):
                dispatch_wave(
                    self.pool, self.workers,
                    before_commit=self._callback_commits_big_payload)

            # The callback's independent committed write survives ...
            witness = sqlite3.connect(self.path)
            try:
                self.assertEqual(
                    witness.execute(
                        "SELECT max(length(payload)) FROM wave_payload"
                    ).fetchone(),
                    (len(PAYLOAD),))
            finally:
                witness.close()

            # ... but dispatch state is byte-for-byte untouched.
            self.assertEqual(dump_database(self.path), before_dump)

            # Both group connections keep the old view, including the
            # connection whose very first query runs only after the callback
            # and the failed admission.
            self.assertEqual(
                readers[0].execute(
                    "SELECT job_id FROM dispatch_ready ORDER BY job_id"
                ).fetchall(),
                [(11,), (12,), (21,)])
            self.assertEqual(
                readers[1].execute(
                    "SELECT count(*) FROM job_attempts").fetchone(),
                (0,))
            # The pre-existing unrelated table stays at its baseline row;
            # the callback's big committed row is outside the old view.
            self.assertEqual(
                readers[1].execute(
                    "SELECT count(*) FROM wave_payload").fetchone(),
                (1,))
            self.assertEqual(
                readers[1].execute(
                    "SELECT count(*) FROM wave_payload "
                    "WHERE length(payload) > 1000").fetchone(),
                (0,))

            # Active maintenance reports the genuine reader obstruction with
            # real measured before/after lengths.
            before_stat = wal_size(self.path)
            status, before, after = self.pool.reclaim_wal()
            after_stat = wal_size(self.path)
            self.assertEqual(status, "reader_busy")
            self.assertEqual(before, before_stat)
            self.assertEqual(after, after_stat)
            self.assertGreaterEqual(before, LIMIT)
            self.assertGreater(after, 0)

        # Group exited; WITHOUT an explicit reclaim, redispatch self-reclaims
        # at admission and produces the required lexicographic matching.
        result = dispatch_wave(self.pool, self.workers)
        self.assertEqual(result, ("committed", [(11, 102), (21, 101)]))

        after_dump = dump_database(self.path)
        self.assertEqual(
            after_dump["attempts"],
            (
                (11, 1002, 1, "dispatched", None, None),
                (21, 1001, 1, "dispatched", None, None),
            ))
        self.assertEqual(
            after_dump["holds"],
            (
                (1, 101, 1),
                (1, 901, 1),
                (2, 201, 1),
                (2, 902, 1),
            ))
        self.assertEqual(
            after_dump["ready"], ((12, 1, 102, 2),))

        # A new group sees the attempts AND the callback's big payload.
        with self.pool.borrow_snapshot_group(2, timeout=5) as readers:
            self.assertEqual(
                readers[0].execute(
                    "SELECT job_id, epoch FROM job_attempts "
                    "ORDER BY job_id").fetchall(),
                [(11, 1002), (21, 1001)])
            self.assertEqual(
                readers[1].execute(
                    "SELECT max(length(payload)) FROM wave_payload"
                ).fetchone(),
                (len(PAYLOAD),))

        self.assertLessEqual(self.pool.peak_open_connections, 3)


class _ScratchOpenFailurePool(ConnectionPool):
    scratch_calls = 0
    fail_on_call = 2

    def _open_counted_scratch_connection_path(self, path):
        type(self).scratch_calls += 1
        if type(self).scratch_calls == self.fail_on_call:
            raise sqlite3.OperationalError("injected scratch open failure")
        return super()._open_counted_scratch_connection_path(path)


class _BackupFailureConnection(sqlite3.Connection):
    def backup(self, target, *args, **kwargs):
        raise sqlite3.OperationalError("injected backup failure")


class ScratchSnapshotTest(unittest.TestCase):
    """Acceptance item 3 plus scratch resource/lifecycle rules."""

    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.scratch = self.root / "scratch"
        self.scratch.mkdir()
        (self.scratch / "preplaced.txt").write_text("duty-file")
        self.path = self.root / "dispatch.db"

    def make_dispatch_world(self, cls=ConnectionPool, **pool_kwargs):
        create_dispatch_db(self.path)
        kwargs = dict(
            capacity=3, snapshot_groups=True, wal_soft_limit=LIMIT,
            wal_scratch_dir=self.scratch)
        kwargs.update(pool_kwargs)
        pool = cls(self.path, **kwargs)
        self.addCleanup(pool.close)
        index = ReservationIndex(pool)
        index.initialize()
        initialize_dispatch(pool)
        finish_backfill(index, (1, 2, 3))
        pool.reclaim_wal()
        return pool

    def scratch_snapshots(self):
        return [
            entry for entry in self.scratch.iterdir()
            if entry.is_file()
            and entry.name.startswith("pool-wal-snap-")
            and entry.name.endswith(".db")
        ]

    def scratch_resource_bytes(self, snapshot):
        total = 0
        for suffix in ("", "-wal", "-shm", "-journal"):
            side = Path(str(snapshot) + suffix)
            if side.is_file():
                total += side.stat().st_size
        return total

    def test_dispatch_continues_with_shared_snapshot_and_real_reclaim(self):
        pool = self.make_dispatch_world()
        workers = [(101, 1001, [1, 2]), (102, 1002, [1])]
        # Baseline unrelated committed row present at the group viewpoint.
        with pool.borrow(timeout=5) as db:
            db.execute(
                "CREATE TABLE wave_payload("
                "id INTEGER PRIMARY KEY, payload BLOB)")
            db.execute(
                "INSERT INTO wave_payload(payload) VALUES (?)",
                (b"baseline",))
            db.commit()

        def callback():
            with pool.borrow(timeout=5) as db:
                db.execute(
                    "INSERT INTO wave_payload(payload) VALUES (?)",
                    (PAYLOAD,))
                db.commit()

        with pool.borrow_snapshot_group(2, timeout=5) as readers:
            snap_files = self.scratch_snapshots()
            self.assertEqual(len(snap_files), 1)
            snapshot_path = snap_files[0]
            # Both connections share exactly one snapshot file.
            sources = {
                Path(db.execute("PRAGMA database_list").fetchone()[2])
                for db in readers
            }
            self.assertEqual(sources, {snapshot_path})

            # Scratch group does not pin the main WAL: active reclaim works
            # even while the delivered group is out.
            status, _b, after = pool.reclaim_wal()
            self.assertEqual((status, after), ("reclaimed", 0))

            result = dispatch_wave(
                pool, workers, before_commit=callback)
            self.assertEqual(
                result, ("committed", [(11, 102), (21, 101)]))

            # The over-threshold old WAL was really pressed below the limit.
            self.assertLess(wal_size(self.path), LIMIT)

            # Committed attempts are real ...
            witness = sqlite3.connect(self.path)
            try:
                self.assertEqual(
                    witness.execute(
                        "SELECT count(*) FROM job_attempts").fetchone(),
                    (2,))
            finally:
                witness.close()

            # ... while the old group, including its late first query, reads
            # only its framed old view from the scratch snapshot.
            self.assertEqual(
                readers[0].execute(
                    "SELECT job_id FROM dispatch_ready ORDER BY job_id"
                ).fetchall(),
                [(11,), (12,), (21,)])
            self.assertEqual(
                readers[1].execute(
                    "SELECT count(*) FROM job_attempts").fetchone(),
                (0,))
            self.assertEqual(
                readers[1].execute(
                    "SELECT count(*) FROM wave_payload").fetchone(),
                (1,))
            self.assertEqual(
                readers[1].execute(
                    "SELECT count(*) FROM wave_payload "
                    "WHERE length(payload) > 1000").fetchone(),
                (0,))

            total = self.scratch_resource_bytes(snapshot_path)
            self.assertGreater(total, 0)
            self.assertLessEqual(total, WAL_SCRATCH_TOTAL_MAX)
            # One source + the shared snapshot connections/transient backup
            # destination never exceed capacity 3.
            self.assertEqual(pool.peak_open_connections, 3)

        # Exit reclaims exactly this group's resource; caller file survives.
        self.assertEqual(self.scratch_snapshots(), [])
        self.assertEqual(
            (self.scratch / "preplaced.txt").read_text(), "duty-file")
        self.assertEqual(pool.peak_open_connections, 3)

    def test_close_keeps_delivered_scratch_group_queryable_then_cleans(self):
        setup = sqlite3.connect(self.path)
        setup.execute("PRAGMA journal_mode=WAL")
        setup.execute("CREATE TABLE t(x)")
        setup.execute("INSERT INTO t VALUES (1)")
        setup.commit()
        setup.close()

        pool = ConnectionPool(
            self.path, capacity=3, snapshot_groups=True,
            wal_soft_limit=LIMIT, wal_scratch_dir=self.scratch)
        self.addCleanup(pool.close)

        with pool.borrow_snapshot_group(2, timeout=5) as readers:
            self.assertEqual(
                readers[0].execute("SELECT count(*) FROM t").fetchone(),
                (1,))
            self.assertEqual(len(self.scratch_snapshots()), 1)
            pool.close()
            # Delivered borrows are not revoked by close().
            self.assertEqual(
                readers[1].execute("SELECT count(*) FROM t").fetchone(),
                (1,))
            # Resource is retained until the last delivered borrow returns.
            self.assertEqual(len(self.scratch_snapshots()), 1)
        # Returning the group after close cleans the resource and balances.
        self.assertEqual(self.scratch_snapshots(), [])
        self.assertEqual(pool.open_connections, 0)
        self.assertEqual(
            (self.scratch / "preplaced.txt").read_text(), "duty-file")
        with self.assertRaises(PoolClosed):
            with pool.borrow(timeout=0):
                pass

    def test_scratch_connection_open_failure_cleans_group_resource(self):
        _ScratchOpenFailurePool.scratch_calls = 0
        pool = self.make_dispatch_world(cls=_ScratchOpenFailurePool)
        with self.assertRaisesRegex(
                sqlite3.OperationalError, "injected scratch open failure"):
            with pool.borrow_snapshot_group(2, timeout=5):
                self.fail("group must not be delivered after setup failure")
        self.assertEqual(self.scratch_snapshots(), [])
        self.assertIsNone(pool._group)
        self.assertEqual(pool.open_connections, 0)
        # Pool is fully usable after the failed build.
        with pool.borrow(timeout=0) as db:
            db.execute("SELECT 1").fetchone()

    def test_backup_failure_cleans_snapshot_directory(self):
        create_dispatch_db(self.path)
        pool = ConnectionPool(
            self.path, capacity=3, snapshot_groups=True,
            wal_soft_limit=LIMIT, wal_scratch_dir=self.scratch)
        self.addCleanup(pool.close)

        def failing_connect(path, *args, **kwargs):
            return sqlite3.connect(
                path, *args, factory=_BackupFailureConnection, **kwargs)

        original_connect = pool._connect

        def connect_with_failing_backup():
            return failing_connect(pool._path_str)

        pool._connect = connect_with_failing_backup
        try:
            with self.assertRaisesRegex(
                    sqlite3.OperationalError, "injected backup failure"):
                with pool.borrow_snapshot_group(2, timeout=5):
                    self.fail("group must not be delivered")
        finally:
            pool._connect = original_connect
        self.assertEqual(self.scratch_snapshots(), [])
        self.assertIsNone(pool._group)
        self.assertEqual(pool.open_connections, 0)

    def test_scratch_cap_violation_rejects_and_removes_snapshot(self):
        import pool as pool_module
        original_cap = pool_module.WAL_SCRATCH_TOTAL_MAX
        pool_module.WAL_SCRATCH_TOTAL_MAX = 4096  # smaller than one page set
        self.addCleanup(
            setattr, pool_module, "WAL_SCRATCH_TOTAL_MAX", original_cap)
        pool = self.make_dispatch_world()
        with self.assertRaises(ValueError):
            with pool.borrow_snapshot_group(2, timeout=5):
                self.fail("oversized scratch snapshot must not be delivered")
        self.assertEqual(self.scratch_snapshots(), [])
        self.assertIsNone(pool._group)
        self.assertEqual(pool.open_connections, 0)

    def test_normal_exception_in_group_still_cleans_scratch(self):
        pool = self.make_dispatch_world()
        with self.assertRaisesRegex(RuntimeError, "boom"):
            with pool.borrow_snapshot_group(2, timeout=5):
                raise RuntimeError("boom")
        self.assertEqual(self.scratch_snapshots(), [])
        self.assertIsNone(pool._group)
        self.assertLessEqual(pool.open_connections, 3)


if __name__ == "__main__":
    unittest.main()
