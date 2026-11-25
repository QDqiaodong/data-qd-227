from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import threading
import time
import unittest

from pool import ConnectionPool, PoolClosed, PoolTimeout


def _amount(db, name):
    return db.execute(
        "SELECT amount FROM ledger WHERE name = ?", (name,)).fetchone()[0]


def _pair(db):
    """One connection query: a join returning (detail, balance)."""
    return db.execute(
        "SELECT d.amount, b.amount FROM ledger AS d JOIN ledger AS b "
        "ON d.name = 'detail' AND b.name = 'balance'").fetchone()


@contextmanager
def _tracked_connections():
    """Count every connection this process opens through ConnectionPool.

    The pool's own ``open_connections`` counter is asserted too, but this
    tracker observes the actual sqlite3 objects, so it also catches leaks the
    counter might paper over.
    """
    tracker = {"live": 0, "peak": 0, "opened": 0}
    lock = threading.Lock()

    class TrackedConnection(sqlite3.Connection):
        _pool_counted = True

        def close(self):
            with lock:
                if self._pool_counted:
                    self._pool_counted = False
                    tracker["live"] -= 1
            super().close()

    original = ConnectionPool._connect

    def connect(self):
        db = sqlite3.connect(
            self._path, check_same_thread=False, factory=TrackedConnection)
        db._pool_counted = True
        with lock:
            tracker["live"] += 1
            tracker["opened"] += 1
            tracker["peak"] = max(tracker["peak"], tracker["live"])
        return db

    ConnectionPool._connect = connect
    try:
        yield tracker
    finally:
        ConnectionPool._connect = original


class PoolTest(unittest.TestCase):
    def setUp(self):
        root = TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.pool = ConnectionPool(Path(root.name) / "local.db", capacity=1)
        self.addCleanup(self.pool.close)

    def test_committed_work_survives_exception_and_open_tx_is_reverted(self):
        with self.assertRaisesRegex(RuntimeError, "caller failure"):
            with self.pool.borrow() as db:
                db.execute("CREATE TABLE records(x)")
                db.execute("INSERT INTO records VALUES (1)")
                db.commit()
                db.execute("INSERT INTO records VALUES (2)")
                raise RuntimeError("caller failure")
        with self.pool.borrow() as reused:
            self.assertIs(reused, db)
            self.assertFalse(reused.in_transaction)
            self.assertEqual(reused.execute("SELECT x FROM records").fetchall(), [(1,)])

    def test_saturation_then_cross_thread_handoff(self):
        def try_borrow():
            with self.pool.borrow(timeout=.02):
                pass

        def acquire():
            with self.pool.borrow(timeout=2) as db:
                return id(db), db.execute("SELECT 8").fetchone()
        with ThreadPoolExecutor(max_workers=1) as workers:
            with self.pool.borrow() as one:
                with self.assertRaises(PoolTimeout):
                    workers.submit(try_borrow).result(timeout=2)
                waiting = workers.submit(acquire)
            self.assertEqual(waiting.result(timeout=3), (id(one), (8,)))

    def test_unusable_connection_is_discarded(self):
        with self.pool.borrow() as db:
            db.close()
        with self.pool.borrow() as replacement:
            self.assertIsNot(db, replacement)
            self.assertEqual(replacement.execute("SELECT 1").fetchone(), (1,))

    def test_close_does_not_revoke_outstanding_lease(self):
        with self.pool.borrow() as db:
            self.pool.close()
            with self.assertRaises(PoolClosed):
                with self.pool.borrow():
                    pass
            self.assertEqual(db.execute("SELECT 1").fetchone(), (1,))
        with self.assertRaises(sqlite3.ProgrammingError):
            db.execute("SELECT 1")


class SnapshotGroupTest(unittest.TestCase):
    """Regression tests for opt-in snapshot groups on a real WAL database."""

    def setUp(self):
        root = TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.root = Path(root.name)
        self.path = self.root / "ledger.db"
        db = sqlite3.connect(self.path)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute(
            "CREATE TABLE ledger(name TEXT PRIMARY KEY, amount INTEGER)")
        db.executemany(
            "INSERT INTO ledger VALUES (?, ?)",
            [("detail", 10), ("balance", 10)])
        db.commit()
        db.close()

    def make_pool(self, *, capacity=3, cls=ConnectionPool):
        pool = cls(self.path, capacity=capacity, snapshot_groups=True)
        self.addCleanup(pool.close)
        return pool

    # -- snapshot semantics ------------------------------------------------

    def test_one_group_sees_one_committed_snapshot_across_later_commit(self):
        pool = self.make_pool(capacity=3)
        first_read = threading.Event()
        release_first = threading.Event()

        def first_reader(readers):
            first_query = _pair(readers[0])
            first_read.set()
            if not release_first.wait(5):
                raise RuntimeError("first reader was never released")
            # Repeat query on the same connection, after the commit below.
            return first_query, _pair(readers[0])

        def second_reader(readers):
            # Its very first SQL runs only after the write below committed.
            return _pair(readers[1]), _amount(readers[1], "detail")

        with ThreadPoolExecutor(max_workers=2) as workers:
            with pool.borrow_snapshot_group(2, timeout=5) as readers:
                self.assertEqual(len(readers), 2)
                self.assertEqual(len({id(db) for db in readers}), 2)
                first = workers.submit(first_reader, readers)
                self.assertTrue(first_read.wait(5))

                with pool.borrow(timeout=5) as writer:
                    writer.execute("UPDATE ledger SET amount = 15")
                    writer.commit()

                second = workers.submit(second_reader, readers)
                self.assertEqual(
                    second.result(timeout=5), ((10, 10), 10))
                release_first.set()
                self.assertEqual(
                    first.result(timeout=5), ((10, 10), (10, 10)))
                # The group still reads its pinned version.
                self.assertEqual(_pair(readers[0]), (10, 10))

            # A fresh group on possibly reused connections sees the commit.
            with pool.borrow_snapshot_group(2, timeout=5) as readers:
                self.assertEqual(_pair(readers[0]), (15, 15))
                self.assertEqual(_pair(readers[1]), (15, 15))
                self.assertEqual(pool.open_connections, 3)

    def test_pending_group_orders_after_write_cleanup_and_before_late_writer(self):
        pool = _BlockingPinPool(self.path, capacity=3, snapshot_groups=True)
        self.addCleanup(pool.close)
        has_uncommitted = threading.Event()
        allow_return = threading.Event()
        late_admitted = threading.Event()

        def writer():
            with pool.borrow(timeout=5) as db:
                db.execute("UPDATE ledger SET amount = 20")
                db.commit()
                db.execute("UPDATE ledger SET amount = 99")
                has_uncommitted.set()
                if not allow_return.wait(5):
                    raise RuntimeError("writer was never released")

        def group_reader():
            with pool.borrow_snapshot_group(2, timeout=5) as readers:
                return _pair(readers[0]), _pair(readers[1])

        def late_writer():
            with pool.borrow(timeout=5) as db:
                late_admitted.set()
                db.execute("UPDATE ledger SET amount = 25")
                db.commit()

        with ThreadPoolExecutor(max_workers=3) as workers:
            w = workers.submit(writer)
            self.assertTrue(has_uncommitted.wait(5))
            g = workers.submit(group_reader)
            # The group request is now registered and waiting for the write.
            self.assertTrue(pool.wait_for_registration())
            with self.assertRaises(PoolTimeout):
                with pool.borrow(timeout=0):
                    pass
            allow_return.set()
            # The write returned (committing 20, rolling back 99) and the
            # group is now pinning its snapshot; a late writer still cannot
            # cross the pending gate.
            self.assertTrue(pool.pin_entered.wait(5))
            self.assertFalse(late_admitted.is_set())
            with self.assertRaises(PoolTimeout):
                with pool.borrow(timeout=0):
                    pass
            pool.pin_release.set()
            self.assertEqual(g.result(timeout=5), ((20, 20), (20, 20)))
            late = workers.submit(late_writer)
            late.result(timeout=5)
            self.assertTrue(late_admitted.is_set())

        with pool.borrow_snapshot_group(2, timeout=5) as readers:
            self.assertEqual(_pair(readers[0]), (25, 25))

    def test_zero_timeout_group_wait_fails_then_pool_stays_usable(self):
        pool = self.make_pool(capacity=3)
        with pool.borrow(timeout=5) as db:
            db.execute("UPDATE ledger SET amount = 20")
            db.commit()
            db.execute("UPDATE ledger SET amount = 99")
            with self.assertRaises(PoolTimeout):
                with pool.borrow_snapshot_group(2, timeout=0) as readers:
                    self.fail("group must not be delivered: %r" % (readers,))
        # Returning the writer rolled back (99, 99); the gate is gone.
        with pool.borrow(timeout=0) as db:
            db.execute("UPDATE ledger SET amount = 25")
            db.commit()
        with pool.borrow_snapshot_group(2, timeout=5) as readers:
            self.assertEqual(_pair(readers[0]), (25, 25))
            self.assertEqual(_amount(readers[1], "detail"), 25)
        self.assertLessEqual(pool.open_connections, 3)

    def test_write_leases_are_mutually_exclusive(self):
        pool = self.make_pool(capacity=3)
        entered = threading.Event()
        release = threading.Event()

        def writer():
            with pool.borrow(timeout=5) as db:
                entered.set()
                if not release.wait(5):
                    raise RuntimeError("writer was never released")
                db.execute("UPDATE ledger SET amount = 50")
                db.commit()

        with ThreadPoolExecutor(max_workers=1) as workers:
            pending = workers.submit(writer)
            self.assertTrue(entered.wait(5))
            with self.assertRaises(PoolTimeout):
                with pool.borrow(timeout=0):
                    pass
            release.set()
            pending.result(timeout=5)
        with pool.borrow_snapshot_group(2, timeout=5) as readers:
            self.assertEqual(_pair(readers[0]), (50, 50))

    # -- failure, close and admission --------------------------------------

    def test_partial_group_initialization_failure_is_reported_and_recovered(self):
        with _tracked_connections() as tracker:
            pool = _FailingPinPool(
                self.path, capacity=3, snapshot_groups=True, fail_at=2)
            self.addCleanup(pool.close)
            with self.assertRaisesRegex(
                    sqlite3.OperationalError, "injected"):
                with pool.borrow_snapshot_group(2, timeout=5) as readers:
                    self.fail("group must not be delivered: %r" % (readers,))
            self.assertEqual(pool.open_connections, 0)
            self.assertLessEqual(tracker["peak"], 3)
            pool.fail_at = -1
            # Gate, write slot and connection budget are all free again.
            with pool.borrow(timeout=0) as db:
                db.execute("UPDATE ledger SET amount = 30")
                db.commit()
            with pool.borrow_snapshot_group(2, timeout=5) as readers:
                self.assertEqual(_pair(readers[0]), (30, 30))
                self.assertEqual(len(readers), 2)
            self.assertLessEqual(pool.open_connections, 3)
            self.assertLessEqual(tracker["peak"], 3)

    def test_connection_open_failure_during_group_setup_is_clean(self):
        with _tracked_connections() as tracker:
            pool = _FailingConnectPool(
                self.path, capacity=3, snapshot_groups=True, fail_at=2)
            self.addCleanup(pool.close)
            with self.assertRaisesRegex(sqlite3.OperationalError, "injected"):
                with pool.borrow_snapshot_group(2, timeout=5) as readers:
                    self.fail("group must not be delivered: %r" % (readers,))
            self.assertEqual(pool.open_connections, 0)
            self.assertEqual(tracker["live"], 0)
            pool.fail_at = -1
            with pool.borrow_snapshot_group(2, timeout=5) as readers:
                self.assertEqual(_pair(readers[0]), (10, 10))
            self.assertLessEqual(tracker["peak"], 3)

    def test_close_during_pinning_does_not_deliver_group(self):
        pool = _BlockingPinPool(self.path, capacity=3, snapshot_groups=True)
        self.addCleanup(pool.close)
        outcome = []

        def group():
            try:
                with pool.borrow_snapshot_group(2, timeout=5) as readers:
                    outcome.append(readers)
            except PoolClosed:
                outcome.append(None)

        with ThreadPoolExecutor(max_workers=1) as workers:
            pending = workers.submit(group)
            self.assertTrue(pool.pin_entered.wait(5))
            pool.close()
            pool.pin_release.set()
            pending.result(timeout=5)
        self.assertEqual(outcome, [None])
        self.assertEqual(pool.open_connections, 0)

    def test_second_group_rejected_while_first_is_still_pending(self):
        pool = _GatePool(self.path, capacity=3, snapshot_groups=True)
        self.addCleanup(pool.close)
        entered = threading.Event()
        release = threading.Event()
        delivered = threading.Event()

        def writer():
            with pool.borrow(timeout=5) as db:
                db.execute("UPDATE ledger SET amount = 20")
                db.commit()
                entered.set()
                if not release.wait(5):
                    raise RuntimeError("writer was never released")

        def group():
            with pool.borrow_snapshot_group(2, timeout=5) as readers:
                delivered.set()
                return _pair(readers[0])

        with ThreadPoolExecutor(max_workers=2) as workers:
            pending = workers.submit(writer)
            self.assertTrue(entered.wait(5))
            first = workers.submit(group)
            self.assertTrue(pool.wait_for_registration())
            # While the first request is registered but not delivered, a
            # second one is refused immediately.
            with self.assertRaises(PoolTimeout):
                with pool.borrow_snapshot_group(2, timeout=5):
                    pass
            self.assertFalse(delivered.is_set())
            release.set()
            pending.result(timeout=5)
            self.assertEqual(first.result(timeout=5), (20, 20))

    def test_upper_capacity_bound(self):
        with _tracked_connections() as tracker:
            pool = ConnectionPool(
                self.path, capacity=16, snapshot_groups=True)
            self.addCleanup(pool.close)
            with pool.borrow_snapshot_group(15, timeout=5) as readers:
                self.assertEqual(len(readers), 15)
                self.assertEqual(len({id(db) for db in readers}), 15)
                with pool.borrow(timeout=5) as writer:
                    writer.execute("UPDATE ledger SET amount = 15")
                    writer.commit()
                    self.assertEqual(pool.open_connections, 16)
                    self.assertLessEqual(tracker["peak"], 16)
                self.assertEqual(_pair(readers[0]), (10, 10))
            self.assertLessEqual(tracker["peak"], 16)

    def test_close_wakes_waiting_writer(self):
        pool = _ObservingPool(
            self.path, capacity=3, snapshot_groups=True)
        self.addCleanup(pool.close)
        entered = threading.Event()
        release = threading.Event()
        outcome = []

        def holder():
            with pool.borrow(timeout=5) as db:
                entered.set()
                if not release.wait(5):
                    raise RuntimeError("writer was never released")
                db.execute("UPDATE ledger SET amount = 15")
                db.commit()
            outcome.append("holder-returned")

        def waiter():
            try:
                with pool.borrow(timeout=5):
                    outcome.append("waiter-granted")
            except PoolClosed:
                outcome.append("waiter-closed")

        with ThreadPoolExecutor(max_workers=2) as workers:
            first = workers.submit(holder)
            self.assertTrue(entered.wait(5))
            pool.waited.clear()
            second = workers.submit(waiter)
            self.assertTrue(pool.waited.wait(5))
            pool.close()
            second.result(timeout=5)
            release.set()
            first.result(timeout=5)
        self.assertEqual(outcome, ["waiter-closed", "holder-returned"])
        self.assertEqual(pool.open_connections, 0)

    def test_close_keeps_delivered_group_readable_then_discards(self):
        pool = self.make_pool(capacity=3)
        with pool.borrow_snapshot_group(2, timeout=5) as readers:
            pool.close()
            self.assertEqual(_pair(readers[0]), (10, 10))
            self.assertEqual(_pair(readers[1]), (10, 10))
            with self.assertRaises(PoolClosed):
                with pool.borrow():
                    pass
        self.assertEqual(pool.open_connections, 0)

    def test_close_wakes_waiting_group_without_delivering_it(self):
        pool = _GatePool(self.path, capacity=3, snapshot_groups=True)
        self.addCleanup(pool.close)
        entered = threading.Event()
        release = threading.Event()
        outcome = []

        def writer():
            with pool.borrow(timeout=5) as db:
                db.execute("UPDATE ledger SET amount = 99")
                entered.set()
                if not release.wait(5):
                    raise RuntimeError("writer was never released")

        def group():
            try:
                with pool.borrow_snapshot_group(2, timeout=5) as readers:
                    outcome.append(readers)
            except PoolClosed:
                outcome.append(None)

        with ThreadPoolExecutor(max_workers=2) as workers:
            pending = workers.submit(writer)
            self.assertTrue(entered.wait(5))
            waiting = workers.submit(group)
            self.assertTrue(pool.wait_for_registration())
            pool.close()
            waiting.result(timeout=5)
            release.set()
            pending.result(timeout=5)
        self.assertEqual(outcome, [None])
        self.assertEqual(pool.open_connections, 0)

    def test_exception_in_group_body_restores_pool(self):
        pool = self.make_pool(capacity=3)
        with self.assertRaisesRegex(RuntimeError, "job failed"):
            with pool.borrow_snapshot_group(2, timeout=5):
                raise RuntimeError("job failed")
        self.assertEqual(pool.open_connections, 2)
        self.assertFalse(any(db.in_transaction for db in pool._idle))
        with pool.borrow(timeout=0) as db:
            db.execute("UPDATE ledger SET amount = 40")
            db.commit()
        with pool.borrow_snapshot_group(2, timeout=5) as readers:
            self.assertEqual(_pair(readers[0]), (40, 40))
        self.assertLessEqual(pool.open_connections, 3)

    def test_only_one_group_request_at_a_time(self):
        pool = self.make_pool(capacity=3)
        with ThreadPoolExecutor(max_workers=1) as workers:
            with pool.borrow_snapshot_group(2, timeout=5) as readers:
                def competing_group():
                    with pool.borrow_snapshot_group(2, timeout=5):
                        pass

                blocked = workers.submit(competing_group)
                with self.assertRaises(PoolTimeout):
                    blocked.result(timeout=5)
                # The existing group and the write slot are unaffected.
                with pool.borrow(timeout=5) as writer:
                    writer.execute("UPDATE ledger SET amount = 15")
                    writer.commit()
                self.assertEqual(_pair(readers[0]), (10, 10))
                self.assertEqual(_pair(readers[1]), (10, 10))

    def test_capacity_peak_and_no_private_database_copy(self):
        with _tracked_connections() as tracker:
            pool = self.make_pool(capacity=3)
            with pool.borrow_snapshot_group(2, timeout=5) as readers:
                with pool.borrow(timeout=5) as writer:
                    writer.execute("UPDATE ledger SET amount = 15")
                    writer.commit()
                    self.assertEqual(pool.open_connections, 3)
                    # Full capacity: no fourth connection may be opened.
                    with self.assertRaises(PoolTimeout):
                        with pool.borrow(timeout=0):
                            pass
                    self.assertEqual(_pair(readers[0]), (10, 10))
                self.assertEqual(pool.open_connections, 3)
            self.assertEqual(tracker["peak"], 3)
            self.assertLessEqual(tracker["peak"], 3)
            # Group connections all point at the one main database file.
            with pool.borrow_snapshot_group(2, timeout=5) as readers:
                sources = {
                    tuple(db.execute("PRAGMA database_list").fetchone()[1:3])
                    for db in readers
                }
                self.assertEqual(sources, {("main", str(self.path))})
            self.assertLessEqual(pool.open_connections, 3)

    # -- validation and WAL requirement ------------------------------------

    def test_snapshot_mode_rejects_non_wal_without_changing_mode(self):
        plain = self.root / "plain.db"
        db = sqlite3.connect(plain)
        db.execute("CREATE TABLE t(x)")
        db.commit()
        db.close()
        with self.assertRaisesRegex(ValueError, "WAL"):
            ConnectionPool(plain, capacity=3, snapshot_groups=True)
        db = sqlite3.connect(plain)
        self.assertEqual(db.execute("PRAGMA journal_mode").fetchone()[0],
                         "delete")
        db.close()
        # Opting into WAL is the caller's job, then the pool accepts it.
        db = sqlite3.connect(plain)
        self.assertEqual(db.execute("PRAGMA journal_mode=WAL").fetchone()[0],
                         "wal")
        db.close()
        ConnectionPool(plain, capacity=3, snapshot_groups=True).close()

    def test_snapshot_mode_requires_an_existing_local_file(self):
        with self.assertRaises(ValueError):
            ConnectionPool(
                self.root / "missing.db", capacity=3, snapshot_groups=True)
        with self.assertRaises(ValueError):
            ConnectionPool(
                self.root, capacity=3, snapshot_groups=True)

    def test_invalid_capacity_count_and_timeout(self):
        for capacity in (0, 2, 17, True, 3.0, "3"):
            with self.assertRaises(ValueError):
                ConnectionPool(
                    self.path, capacity=capacity, snapshot_groups=True)
        pool = self.make_pool(capacity=3)
        for count in (1, 3, 0, 4, True, 2.0, "2", None):
            with self.assertRaises(ValueError):
                pool.borrow_snapshot_group(count)
        for timeout in (-1, float("nan"), float("inf"), None, "1"):
            with self.assertRaises(ValueError):
                pool.borrow_snapshot_group(2, timeout=timeout)
            with self.assertRaises(ValueError):
                with pool.borrow(timeout=timeout):
                    pass

    def test_group_entry_rejected_on_original_pool(self):
        pool = ConnectionPool(self.path, capacity=3)
        self.addCleanup(pool.close)
        with self.assertRaises(ValueError):
            pool.borrow_snapshot_group(2)
        # The original pool keeps its original, non-exclusive borrowing.
        with pool.borrow(timeout=0) as one, pool.borrow(timeout=0) as two:
            self.assertIsNot(one, two)
            self.assertEqual(one.execute("SELECT 1").fetchone(), (1,))


class _ObservingPool(ConnectionPool):
    """Pool that signals when a caller starts waiting for admission."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.waited = threading.Event()

    def _wait(self, remaining):
        self.waited.set()
        super()._wait(remaining)


class _GatePool(ConnectionPool):
    """Test pool with a synchronization point on group registration."""

    def wait_for_registration(self, timeout=5):
        """Block until a group request is registered and pending.

        Uses the pool's own condition rather than sleeps or bare events, so
        the caller knows the gate is really in place when this returns.
        """
        deadline = time.monotonic() + timeout
        with self._cond:
            while not (self._group is not None
                       and self._group.state == "pending"):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(remaining)
            return True


class _BlockingPinPool(_GatePool):
    """Test pool whose snapshot pinning can be held at a known point."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pin_entered = threading.Event()
        self.pin_release = threading.Event()

    def _pin_snapshot(self, db):
        self.pin_entered.set()
        if not self.pin_release.wait(5):
            raise RuntimeError("pin was never released")
        super()._pin_snapshot(db)


class _FailingConnectPool(ConnectionPool):
    """Test pool that fails while opening the Nth group connection."""

    def __init__(self, *args, fail_at=2, **kwargs):
        super().__init__(*args, **kwargs)
        self.connect_calls = 0
        self.fail_at = fail_at

    def _connect(self):
        self.connect_calls += 1
        if self.connect_calls == self.fail_at:
            raise sqlite3.OperationalError("injected connect failure")
        return super()._connect()


class _FailingPinPool(_GatePool):
    """Test pool that fails on the Nth snapshot pin."""

    def __init__(self, *args, fail_at=2, **kwargs):
        super().__init__(*args, **kwargs)
        self.pin_calls = 0
        self.fail_at = fail_at

    def _pin_snapshot(self, db):
        self.pin_calls += 1
        if self.pin_calls == self.fail_at:
            raise sqlite3.OperationalError("injected pin failure")
        super()._pin_snapshot(db)


if __name__ == "__main__":
    unittest.main()
