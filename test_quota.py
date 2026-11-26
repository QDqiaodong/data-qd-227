"""Tests for the tenant reservation index and quota write guard.

Everything runs against real temporary WAL SQLite databases and the existing
``ConnectionPool``; no mocks replace SQLite.  Concurrency cases use injected
connection hooks as explicit synchronization points, never sleeps.
"""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import random
import sqlite3
from tempfile import TemporaryDirectory
import threading
import unittest

from pool import ConnectionPool
from quota import (
    ADVANCE_MAX,
    SLOT_MAX,
    TENANT_MAX,
    UNITS_MAX,
    ReservationIndex,
)


def make_database(path, tenants, holds=()):
    """Create the legacy schema in WAL mode with seed rows."""
    db = sqlite3.connect(path)
    try:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute(
            "CREATE TABLE quota_tenants ("
            "tenant_id INTEGER PRIMARY KEY, max_units INTEGER NOT NULL)")
        db.execute(
            "CREATE TABLE quota_holds ("
            "tenant_id INTEGER NOT NULL, slot INTEGER NOT NULL, "
            "units INTEGER NOT NULL, PRIMARY KEY(tenant_id, slot))")
        db.executemany(
            "INSERT INTO quota_tenants VALUES (?, ?)", list(tenants))
        db.executemany(
            "INSERT INTO quota_holds VALUES (?, ?, ?)", list(holds))
        db.commit()
    finally:
        db.close()


def raw_total(path, tenant_id):
    """Independent ground-truth total straight from the raw hold table."""
    db = sqlite3.connect(path)
    try:
        row = db.execute(
            "SELECT COALESCE(SUM(units), 0) FROM quota_holds "
            "WHERE tenant_id = ?", (tenant_id,)).fetchone()
        return row[0]
    finally:
        db.close()


def raw_holds(path, tenant_id):
    db = sqlite3.connect(path)
    try:
        return set(db.execute(
            "SELECT slot, units FROM quota_holds WHERE tenant_id = ?",
            (tenant_id,)).fetchall())
    finally:
        db.close()


def backfill(index, tenant_id):
    covered = index.status(tenant_id)[0]
    while covered < SLOT_MAX:
        covered, _ = index.advance(tenant_id, ADVANCE_MAX)
    return covered


class _HookPool(ConnectionPool):
    """Pool whose connections expose deterministic test synchronization.

    ``block_on`` makes the first matching ``execute`` signal ``inside`` and
    wait on ``release``; ``raise_on`` makes every matching ``execute`` raise
    an ``OperationalError`` so SQLite write failures can be exercised without
    touching the component code.  ``statements`` records every SQL string.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.inside = threading.Event()
        self.release = threading.Event()
        self.block_on = None
        self.raise_on = None
        self.raised = 0
        self.statements = []

    def _connect(self):
        pool = self

        class HookConnection(sqlite3.Connection):
            def execute(self, sql, parameters=()):
                pool.statements.append(sql)
                if pool.raise_on is not None and pool.raise_on in sql:
                    pool.raised += 1
                    raise sqlite3.OperationalError("injected write failure")
                if (pool.block_on is not None and pool.block_on in sql
                        and not pool.inside.is_set()):
                    pool.inside.set()
                    if not pool.release.wait(5):
                        raise RuntimeError("hook was never released")
                return super().execute(sql, parameters)

        return sqlite3.connect(
            self._path, check_same_thread=False, factory=HookConnection)


class ReservationIndexTest(unittest.TestCase):
    def setUp(self):
        root = TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.path = Path(root.name) / "quota.db"
        make_database(
            self.path,
            tenants=[(1, 10), (2, 20)],
            holds=[(1, 1, 5), (1, 2, 2), (1, SLOT_MAX, 4), (2, 9, 7)])
        self.pool = ConnectionPool(self.path, capacity=4)
        self.addCleanup(self.pool.close)
        self.index = ReservationIndex(self.pool)
        self.index.initialize()

    def test_required_scenario_partial_backfill_rules_then_exact_quota(self):
        # Only slot 1 is covered: partial totals must never be exposed.
        self.assertEqual(self.index.advance(1, 1), (1, 5))
        self.assertEqual(self.index.status(1), (1, None))

        # Decreases and deletions (also on still-uncovered slots) are always
        # allowed, even while the tenant is not ready.
        self.assertEqual(self.index.set_hold(1, 1, 3), "applied")
        self.assertEqual(self.index.set_hold(1, SLOT_MAX, 0), "applied")
        self.assertEqual(raw_holds(self.path, 1), {(1, 3), (2, 2)})

        # The real total (5) is already below the limit (10), but net
        # increases stay forbidden until the tenant is fully covered.
        self.assertEqual(self.index.set_hold(1, 2, 3), "not_ready")
        self.assertEqual(self.index.set_hold(1, 3, 1), "not_ready")
        self.assertEqual(raw_holds(self.path, 1), {(1, 3), (2, 2)})
        self.assertEqual(self.index.status(1), (1, None))

        # Finish the backfill; the deleted far slot must not come back and
        # the covered-slot decrease must not be lost: exact total is 5.
        self.assertEqual(backfill(self.index, 1), SLOT_MAX)
        self.assertEqual(self.index.status(1), (SLOT_MAX, 5))
        self.assertEqual(raw_total(self.path, 1), 5)

        # Exactly 5 free units: occupying them reaches the limit.
        self.assertEqual(self.index.set_hold(1, 7, 5), "applied")
        self.assertEqual(self.index.status(1), (SLOT_MAX, 10))
        # One more unit must be rejected without changing either table.
        self.assertEqual(self.index.set_hold(1, 8, 1), "limit_exceeded")
        self.assertEqual(raw_holds(self.path, 1),
                         {(1, 3), (2, 2), (7, 5)})
        self.assertEqual(self.index.status(1), (SLOT_MAX, 10))

        # Tenant 2 is independently still not ready.
        self.assertEqual(self.index.status(2), (0, None))
        self.assertEqual(self.index.set_hold(2, 1, 1), "not_ready")
        self.assertEqual(raw_holds(self.path, 2), {(9, 7)})

    def test_same_value_deletion_noop_and_covered_slot_delta_merge(self):
        self.assertEqual(self.index.advance(1, 2), (2, 7))
        # Same value on an existing row: allowed, no movement.
        self.assertEqual(self.index.set_hold(1, 2, 2), "applied")
        # Deleting a missing row is a documented no-op.
        self.assertEqual(self.index.set_hold(1, 3, 0), "applied")
        # Decrease a covered slot: the running total follows immediately.
        self.assertEqual(self.index.set_hold(1, 1, 1), "applied")
        with self.pool.borrow() as db:
            used = db.execute(
                "SELECT used_units FROM quota_index WHERE tenant_id = 1"
            ).fetchone()[0]
        self.assertEqual(used, 3)
        # An uncovered slot reduced before it is scanned: the scan must see
        # the new value, never the legacy one.
        self.assertEqual(self.index.set_hold(1, SLOT_MAX, 1), "applied")
        self.assertEqual(raw_holds(self.path, 1),
                         {(1, 1), (2, 2), (SLOT_MAX, 1)})
        backfill(self.index, 1)
        self.assertEqual(self.index.status(1), (SLOT_MAX, 4))
        self.assertEqual(raw_total(self.path, 1), 4)

    def test_uncovered_deletion_is_not_resurrected_by_later_scan(self):
        self.index.advance(1, 1)
        # Delete a slot far ahead of the prefix while it is still uncovered.
        self.assertEqual(self.index.set_hold(1, SLOT_MAX, 0), "applied")
        backfill(self.index, 1)
        self.assertEqual(self.index.status(1), (SLOT_MAX, 7))
        self.assertNotIn((SLOT_MAX, 4), raw_holds(self.path, 1))

    def test_over_quota_legacy_total_allows_releases_blocks_increases(self):
        path3 = self.path.parent / "over.db"
        make_database(path3, tenants=[(3, 3)], holds=[(3, 1, 5), (3, 2, 5)])
        pool = ConnectionPool(path3, capacity=2)
        self.addCleanup(pool.close)
        index = ReservationIndex(pool)
        index.initialize()
        backfill(index, 3)
        self.assertEqual(index.status(3), (SLOT_MAX, 10))
        # Already over the limit: releases and same values still go through.
        self.assertEqual(index.set_hold(3, 1, 5), "applied")
        self.assertEqual(index.set_hold(3, 1, 4), "applied")
        self.assertEqual(index.set_hold(3, 2, 0), "applied")
        self.assertEqual(index.status(3), (SLOT_MAX, 4))
        # Still over the limit of 3: even one unit cannot be admitted.
        self.assertEqual(index.set_hold(3, 3, 1), "limit_exceeded")
        self.assertEqual(raw_total(path3, 3), 4)
        # Release enough headroom, then increases fill it exactly and the
        # next unit is rejected.
        self.assertEqual(index.set_hold(3, 1, 1), "applied")
        self.assertEqual(index.status(3), (SLOT_MAX, 1))
        self.assertEqual(index.set_hold(3, 3, 1), "applied")
        self.assertEqual(index.set_hold(3, 4, 1), "applied")
        self.assertEqual(index.set_hold(3, 5, 1), "limit_exceeded")
        self.assertEqual(raw_total(path3, 3), 3)

    def test_advance_only_touches_the_new_range_of_this_tenant(self):
        pool = _HookPool(self.path, capacity=4)
        self.addCleanup(pool.close)
        index = ReservationIndex(pool)
        index.initialize()
        covered, used = index.advance(1, 3)
        self.assertEqual((covered, used), (3, 7))
        hold_queries = [
            sql for sql in pool.statements if "quota_holds" in sql]
        # Exactly one raw-hold statement per step, bounded by tenant and slot.
        self.assertEqual(len(hold_queries), 1)
        sql = hold_queries[0]
        self.assertIn("tenant_id = ?", sql)
        self.assertIn("slot > ?", sql)
        self.assertIn("slot <= ?", sql)
        self.assertIn("SUM(units)", sql)
        # Tenant 2's hold at slot 9 and tenant 1's slot 4096 stay excluded.
        self.assertEqual(used, 7)

    def test_advance_is_idempotent_after_completion_and_never_overshoots(self):
        self.index.advance(1, 1)
        # Drive to exactly 4095, then take the precise final single slot.
        covered = 1
        while covered < SLOT_MAX - 1:
            step = min(ADVANCE_MAX, SLOT_MAX - 1 - covered)
            covered, _ = self.index.advance(1, step)
        self.assertEqual(covered, SLOT_MAX - 1)
        self.assertEqual(self.index.status(1), (SLOT_MAX - 1, None))
        final_covered, final_used = self.index.advance(1, 1)
        self.assertEqual((final_covered, final_used),
                         (SLOT_MAX, raw_total(self.path, 1)))
        # Further calls are side-effect free reads and cannot exceed 4096.
        again = self.index.advance(1, ADVANCE_MAX)
        self.assertEqual(again, (SLOT_MAX, raw_total(self.path, 1)))
        with self.pool.borrow() as db:
            stored = db.execute(
                "SELECT covered_slots FROM quota_index WHERE tenant_id = 1"
            ).fetchone()[0]
        self.assertEqual(stored, SLOT_MAX)

    def test_initialize_is_idempotent_and_preserves_progress(self):
        self.index.advance(1, 10)
        self.index.initialize()
        self.index.initialize()
        self.assertEqual(self.index.status(1), (10, None))
        with self.pool.borrow() as db:
            rows = db.execute(
                "SELECT tenant_id, covered_slots, used_units, ready "
                "FROM quota_index ORDER BY tenant_id").fetchall()
        self.assertEqual(rows, [(1, 10, 7, 0), (2, 0, 0, 0)])

    def test_borrowed_connections_are_returned_without_open_transactions(self):
        self.index.set_hold(1, 1, 4)
        self.index.advance(2, 8)
        self.index.status(1)
        for db in self.pool._idle:
            self.assertFalse(db.in_transaction)


class RestartTest(unittest.TestCase):
    def test_pool_reopen_keeps_committed_prefix_without_double_counting(self):
        root = TemporaryDirectory()
        self.addCleanup(root.cleanup)
        path = Path(root.name) / "quota.db"
        holds = [(1, slot, slot % 7 + 1) for slot in range(1, 4097, 37)]
        holds += [(2, 5, 6), (2, 100, 9)]
        make_database(path, tenants=[(1, 1_000_000), (2, 50)], holds=holds)
        truth_1 = raw_total(path, 1)
        truth_2 = raw_total(path, 2)

        pool = ConnectionPool(path, capacity=3)
        index = ReservationIndex(pool)
        index.initialize()
        for _ in range(3):
            index.advance(1, 500)
        index.advance(2, 50)
        self.assertEqual(index.status(1), (1500, None))
        self.assertEqual(index.status(2), (50, None))
        pool.close()

        # Reopen with a brand-new pool and component instance, then
        # re-initialize: committed backfill state must survive untouched.
        pool = ConnectionPool(path, capacity=3)
        self.addCleanup(pool.close)
        index = ReservationIndex(pool)
        index.initialize()
        self.assertEqual(index.status(1), (1500, None))
        self.assertEqual(index.status(2), (50, None))
        backfill(index, 1)
        backfill(index, 2)
        self.assertEqual(index.status(1), (SLOT_MAX, truth_1))
        self.assertEqual(index.status(2), (SLOT_MAX, truth_2))
        # The index total is exactly the raw ground truth, no duplicates.
        self.assertEqual(raw_total(path, 1), truth_1)
        self.assertEqual(raw_total(path, 2), truth_2)


class ConcurrencyTest(unittest.TestCase):
    def setUp(self):
        root = TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.path = Path(root.name) / "quota.db"
        # Tenant 1 starts ready with exactly one free unit.
        make_database(self.path, tenants=[(1, 10), (2, 10)],
                      holds=[(1, 1, 9)])
        self.pool = _HookPool(self.path, capacity=4)
        self.addCleanup(self.pool.close)
        self.index = ReservationIndex(self.pool)
        self.index.initialize()
        backfill(self.index, 1)
        backfill(self.index, 2)
        self.assertEqual(self.index.status(1), (SLOT_MAX, 9))

    def test_two_contending_increases_admit_at_most_one(self):
        # Hold writer A inside its transaction at the raw INSERT.
        self.pool.block_on = "INSERT INTO quota_holds"
        with ThreadPoolExecutor(max_workers=2) as workers:
            first = workers.submit(self.index.set_hold, 1, 10, 1)
            self.assertTrue(self.pool.inside.wait(5))
            second = workers.submit(self.index.set_hold, 1, 11, 1)
            # B cannot finish while A holds the component's write serialization.
            self.assertFalse(second.done())
            self.pool.release.set()
            outcomes = {first.result(5), second.result(5)}
        self.assertEqual(outcomes, {"applied", "limit_exceeded"})
        self.assertEqual(self.index.status(1), (SLOT_MAX, 10))
        rows = raw_holds(self.path, 1)
        self.assertEqual(len(rows), 2)  # the original row plus one new row
        self.assertEqual(sum(units for _, units in rows), 10)

    def test_sqlite_write_failure_propagates_and_headroom_is_preserved(self):
        # A write that should pass is rejected by SQLite itself.
        self.pool.raise_on = "INSERT INTO quota_holds"
        with self.assertRaises(sqlite3.Error):
            self.index.set_hold(1, 12, 1)
        self.assertGreaterEqual(self.pool.raised, 1)
        # Nothing was applied: raw hold, index total and headroom are intact.
        self.assertEqual(raw_holds(self.path, 1), {(1, 9)})
        self.assertEqual(self.index.status(1), (SLOT_MAX, 9))

        # After the failure the same connection class still serves a legal
        # write and the preserved headroom is usable.
        self.pool.raise_on = None
        self.assertEqual(self.index.set_hold(1, 12, 1), "applied")
        self.assertEqual(self.index.status(1), (SLOT_MAX, 10))
        self.assertEqual(raw_total(self.path, 1), 10)
        # Headroom is now genuinely exhausted, not silently corrupted.
        self.assertEqual(self.index.set_hold(1, 13, 1), "limit_exceeded")
        # A decrease still works after the injected failure path.
        self.assertEqual(self.index.set_hold(1, 1, 4), "applied")
        self.assertEqual(self.index.status(1), (SLOT_MAX, 5))

    def test_final_backfill_step_orders_against_business_write(self):
        # A fresh tenant parked at 4095.
        path = self.path.parent / "boundary.db"
        make_database(path, tenants=[(3, 10)], holds=[(3, SLOT_MAX, 3)])
        pool = _HookPool(path, capacity=3)
        self.addCleanup(pool.close)
        index = ReservationIndex(pool)
        index.initialize()
        covered = 0
        while covered < SLOT_MAX - 1:
            step = min(ADVANCE_MAX, SLOT_MAX - 1 - covered)
            covered, _ = index.advance(3, step)
        self.assertEqual(covered, SLOT_MAX - 1)

        # Ordering A: the backfill commits readiness first; the waiting
        # business increase is then admitted against the exact total.
        pool.block_on = "UPDATE quota_index"
        with ThreadPoolExecutor(max_workers=2) as workers:
            finishing = workers.submit(index.advance, 3, 1)
            self.assertTrue(pool.inside.wait(5))
            waiting_write = workers.submit(index.set_hold, 3, 7, 1)
            self.assertFalse(waiting_write.done())
            pool.release.set()
            self.assertEqual(finishing.result(5), (SLOT_MAX, 3))
            self.assertEqual(waiting_write.result(5), "applied")
        self.assertEqual(index.status(3), (SLOT_MAX, 4))
        self.assertEqual(raw_total(path, 3), 4)

        # Ordering B on a fresh tenant: a business increase that commits
        # before readiness follows the not-ready rule.
        path_b = self.path.parent / "boundary_b.db"
        make_database(path_b, tenants=[(4, 10)], holds=[(4, SLOT_MAX, 3)])
        pool_b = ConnectionPool(path_b, capacity=3)
        self.addCleanup(pool_b.close)
        index_b = ReservationIndex(pool_b)
        index_b.initialize()
        covered = 0
        while covered < SLOT_MAX - 1:
            step = min(ADVANCE_MAX, SLOT_MAX - 1 - covered)
            covered, _ = index_b.advance(4, step)
        self.assertEqual(index_b.set_hold(4, 7, 1), "not_ready")
        self.assertEqual(raw_holds(path_b, 4), {(SLOT_MAX, 3)})
        index_b.advance(4, 1)
        self.assertEqual(index_b.set_hold(4, 7, 1), "applied")
        self.assertEqual(index_b.status(4), (SLOT_MAX, 4))


class SnapshotGroupQuotaTest(unittest.TestCase):
    def test_quota_writes_commit_while_a_delivered_group_keeps_its_view(self):
        root = TemporaryDirectory()
        self.addCleanup(root.cleanup)
        path = Path(root.name) / "quota.db"
        make_database(path, tenants=[(1, 10)], holds=[(1, 1, 2)])
        pool = ConnectionPool(path, capacity=3, snapshot_groups=True)
        self.addCleanup(pool.close)
        index = ReservationIndex(pool)
        # Initialize before any read group is opened.
        index.initialize()
        backfill(index, 1)
        self.assertEqual(index.status(1), (SLOT_MAX, 2))

        with pool.borrow_snapshot_group(2, timeout=5) as readers:
            # Both group connections observe the pre-write view.
            for reader in readers:
                self.assertEqual(reader.execute(
                    "SELECT units FROM quota_holds "
                    "WHERE tenant_id = 1 AND slot = 1").fetchone()[0], 2)
                self.assertIsNone(reader.execute(
                    "SELECT units FROM quota_holds "
                    "WHERE tenant_id = 1 AND slot = 5").fetchone())

            # A quota-allowed write actually commits while the group is live;
            # it needs a third connection, exercising the full capacity.
            self.assertEqual(index.set_hold(1, 1, 5), "applied")
            self.assertEqual(index.set_hold(1, 5, 3), "applied")
            self.assertEqual(pool.open_connections, 3)
            # A rejected write changes nothing and also does not disturb
            # the pinned snapshot.
            self.assertEqual(index.set_hold(1, 6, 99), "limit_exceeded")

            for reader in readers:
                self.assertEqual(reader.execute(
                    "SELECT units FROM quota_holds "
                    "WHERE tenant_id = 1 AND slot = 1").fetchone()[0], 2)
                self.assertIsNone(reader.execute(
                    "SELECT units FROM quota_holds "
                    "WHERE tenant_id = 1 AND slot = 5").fetchone())

        # A later group on reused connections sees the committed new state.
        with pool.borrow_snapshot_group(2, timeout=5) as readers:
            for reader in readers:
                self.assertEqual(reader.execute(
                    "SELECT units FROM quota_holds "
                    "WHERE tenant_id = 1 AND slot = 1").fetchone()[0], 5)
                self.assertEqual(reader.execute(
                    "SELECT units FROM quota_holds "
                    "WHERE tenant_id = 1 AND slot = 5").fetchone()[0], 3)
        self.assertEqual(index.status(1), (SLOT_MAX, 8))
        self.assertLessEqual(pool.open_connections, 3)


class InterleavingInvariantTest(unittest.TestCase):
    """Backfill vs. business writes: every committed state stays consistent."""

    def setUp(self):
        root = TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.path = Path(root.name) / "quota.db"
        rng = random.Random(4242)
        holds = [(1, slot, rng.randint(1, 9))
                 for slot in rng.sample(range(1, SLOT_MAX + 1), 300)]
        make_database(self.path, tenants=[(1, 10_000)], holds=holds)
        self.truth = dict((slot, units) for _, slot, units in holds)
        self.pool = ConnectionPool(self.path, capacity=3)
        self.addCleanup(self.pool.close)
        self.index = ReservationIndex(self.pool)
        self.index.initialize()

    def _checkpoint(self):
        """Invariant: used_units equals the raw sum over covered slots only."""
        with self.pool.borrow() as db:
            covered, used, ready = db.execute(
                "SELECT covered_slots, used_units, ready "
                "FROM quota_index WHERE tenant_id = 1").fetchone()
        prefix_sum = sum(units for slot, units in self.truth.items()
                         if slot <= covered)
        self.assertEqual(used, prefix_sum)
        self.assertEqual(bool(ready), covered == SLOT_MAX)
        return covered

    def test_single_threaded_interleavings_keep_prefix_invariant(self):
        rng = random.Random(7)
        covered = 0
        while covered < SLOT_MAX:
            action = rng.randrange(4)
            if action == 0:
                covered, _ = self.index.advance(
                    1, rng.randint(1, ADVANCE_MAX))
            elif action == 1:
                slot = rng.randint(1, SLOT_MAX)
                units = rng.randint(0, 20)
                verdict = self.index.set_hold(1, slot, units)
                if verdict == "applied":
                    if units:
                        self.truth[slot] = units
                    else:
                        self.truth.pop(slot, None)
            elif action == 2:
                # Always-allowed changes: decreases and deletions on rows
                # that currently exist.
                if self.truth:
                    slot = rng.choice(list(self.truth))
                    new_value = rng.randint(0, self.truth[slot] - 1)
                    self.assertEqual(
                        self.index.set_hold(1, slot, new_value), "applied")
                    if new_value:
                        self.truth[slot] = new_value
                    else:
                        self.truth.pop(slot, None)
            covered = self._checkpoint()
        covered, used = self.index.status(1)
        self.assertEqual((covered, used),
                         (SLOT_MAX, sum(self.truth.values())))
        self.assertEqual(used, raw_total(self.path, 1))

    def test_backfill_worker_concurrent_with_business_writers(self):
        stop = threading.Event()
        errors = []

        def backfiller():
            try:
                while not stop.is_set():
                    covered, _ = self.index.advance(1, 17)
                    if covered == SLOT_MAX:
                        return
            except BaseException as exc:  # pragma: no cover - diagnostic
                errors.append(exc)

        def writer(seed):
            rng = random.Random(seed)
            try:
                for _ in range(400):
                    slot = rng.randint(1, SLOT_MAX)
                    if rng.random() < 0.6:
                        # Releases: always legal regardless of readiness.
                        current = self.truth.get(slot, 0)
                        units = rng.randint(0, current)
                        verdict = self.index.set_hold(1, slot, units)
                        if verdict == "applied":
                            if units:
                                self.truth[slot] = units
                            else:
                                self.truth.pop(slot, None)
                    else:
                        units = rng.randint(1, 30)
                        verdict = self.index.set_hold(1, slot, units)
                        if verdict == "applied":
                            self.truth[slot] = units
            except BaseException as exc:  # pragma: no cover - diagnostic
                errors.append(exc)

        with ThreadPoolExecutor(max_workers=3) as workers:
            futures = [workers.submit(backfiller)]
            futures += [workers.submit(writer, 100 + n) for n in range(2)]
            for future in futures:
                future.result(10)
            stop.set()
        self.assertEqual(errors, [])
        covered, used = self.index.status(1)
        self.assertEqual(covered, SLOT_MAX)
        self.assertEqual(used, raw_total(self.path, 1))


class RejectionAtomicityTest(unittest.TestCase):
    def setUp(self):
        root = TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.path = Path(root.name) / "quota.db"
        make_database(self.path, tenants=[(1, 10)],
                      holds=[(1, 1, 4), (1, 2, 3)])
        self.pool = ConnectionPool(self.path, capacity=2)
        self.addCleanup(self.pool.close)
        self.index = ReservationIndex(self.pool)
        self.index.initialize()

    def _snapshot(self):
        db = sqlite3.connect(self.path)
        try:
            return (
                set(db.execute(
                    "SELECT tenant_id, slot, units FROM quota_holds")
                    .fetchall()),
                list(db.execute(
                    "SELECT tenant_id, covered_slots, used_units, ready "
                    "FROM quota_index").fetchall()))
        finally:
            db.close()

    def test_rejections_change_neither_table_at_any_backfill_stage(self):
        for advance in (0, 1, 2000, SLOT_MAX):
            covered = self.index.status(1)[0]
            while covered < min(advance, SLOT_MAX):
                covered, _ = self.index.advance(1, ADVANCE_MAX)
            before = self._snapshot()
            ready = covered == SLOT_MAX
            # Net increases on an existing slot and on a fresh slot.
            # Total is 7 under limit 10: +4 reaches the limit, +5 exceeds.
            self.assertEqual(
                self.index.set_hold(1, 1, 12),
                "limit_exceeded" if ready else "not_ready")
            self.assertEqual(
                self.index.set_hold(1, 99, 5),
                "limit_exceeded" if ready else "not_ready")
            self.assertEqual(self._snapshot(), before)
        # Final ready state still matches the untouched legacy total.
        self.assertEqual(self.index.status(1), (SLOT_MAX, 7))


class ValidationTest(unittest.TestCase):
    def setUp(self):
        root = TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.path = Path(root.name) / "quota.db"
        # Only tenants 1 and 2 exist; in-range but unconfigured ids are
        # still unknown tenants.
        make_database(self.path, tenants=[(1, 10), (2, 10)],
                      holds=[(1, 1, 1)])
        self.pool = ConnectionPool(self.path, capacity=2)
        self.addCleanup(self.pool.close)
        self.index = ReservationIndex(self.pool)
        self.index.initialize()

    def _assert_value_error(self, fn):
        with self.assertRaises(ValueError):
            fn()

    def test_rejects_bad_types_and_ranges(self):
        bad_int = [True, False, 1.0, "1", None, 1j]
        for bad in bad_int:
            self._assert_value_error(lambda: self.index.advance(bad, 1))
            self._assert_value_error(lambda: self.index.status(bad))
            self._assert_value_error(
                lambda: self.index.set_hold(bad, 1, 1))
        for bad_tenant in (0, TENANT_MAX + 1, -1):
            self._assert_value_error(
                lambda t=bad_tenant: self.index.advance(t, 1))
            self._assert_value_error(
                lambda t=bad_tenant: self.index.status(t))
            self._assert_value_error(
                lambda t=bad_tenant: self.index.set_hold(t, 1, 1))
        for bad_slots in (0, ADVANCE_MAX + 1, -1, True, 2.0):
            self._assert_value_error(
                lambda s=bad_slots: self.index.advance(1, s))
        for bad_slot in (0, SLOT_MAX + 1, -1, True, 1.0):
            self._assert_value_error(
                lambda s=bad_slot: self.index.set_hold(1, s, 1))
        for bad_units in (-1, UNITS_MAX + 1, True, 1.0, "1"):
            self._assert_value_error(
                lambda u=bad_units: self.index.set_hold(1, 1, u))

    def test_unknown_but_in_range_tenant_is_value_error(self):
        self._assert_value_error(lambda: self.index.advance(3, 1))
        self._assert_value_error(lambda: self.index.status(3))
        self._assert_value_error(
            lambda: self.index.set_hold(3, 1, 1))

    def test_validation_runs_before_any_database_change(self):
        with self.assertRaises(ValueError):
            self.index.set_hold(1, 1, UNITS_MAX + 1)
        self.assertEqual(raw_holds(self.path, 1), {(1, 1)})
        for db in self.pool._idle:
            self.assertFalse(db.in_transaction)


if __name__ == "__main__":
    unittest.main()
