"""Active-attempt roster handoff regressions (``projection.py``).

Pure standard library, real temporary WAL SQLite databases, driven through the
existing :class:`pool.ConnectionPool`, ``dispatch_wave`` and quota components.
Concurrency/interleaving is built from explicit statements and commits - no
sleeps are used to create races.

Covered:

* enable / validation semantics: online limits, non-boolean integers,
  re-enabling after a pool reopen keeps acknowledgement progress and tickets,
  a different limit is rejected, both handoff entries reject when disabled;
* the first ticket is always ``"full"`` (even when empty), excludes uncertain /
  finished / retryable states, keeps ids ascending, and pins its content across
  later commits and a close/reopen;
* dirty tracking: only active-roster changes count, the same job collapses into
  one budget item, inactive-only updates and same-content updates record
  nothing, rolled-back writes leave nothing, reconcile-style inactive writes
  are never blocked;
* the acceptance scenario with capacity 1 / dirty_limit 2 for both overflow
  policies (defer with ``sqlite3.IntegrityError`` + ``projection_backlog`` and
  full-batch rollback, and commit with a forced full roster next);
* the no-backlog failure path keeps the original SQLite error, all four
  business tables and creates no ghost ticket;
* ``ROSTER_MAX`` bounds and an independent monitor dictionary reconciled against
  the real active rows in the database.
"""

import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from dispatch import dispatch_wave, initialize_dispatch
from pool import ConnectionPool
from projection import (
    DIRTY_LIMIT_MAX,
    DIRTY_LIMIT_MIN,
    ROSTER_MAX,
    ack_projection,
    enable_projection,
    next_projection,
)
from quota import ReservationIndex
from test_dispatch import (
    FailurePool,
    create_dispatch_db,
    dump_database,
    finish_backfill,
)


def apply_rows(roster, rows, kind="patch"):
    """Apply one ticket's rows to an independent monitor dictionary.

    A ``"full"`` ticket replaces the monitor's whole roster; a ``"patch"``
    only touches the listed jobs, with ``entry=None`` removing one.
    """
    if kind == "full":
        roster.clear()
        for job_id, entry in rows:
            if entry is not None:
                roster[job_id] = entry
        return roster
    for job_id, entry in rows:
        if entry is None:
            roster.pop(job_id, None)
        else:
            roster[job_id] = entry
    return roster


def active_rows(path):
    """Ground truth: current active attempts straight from the database."""
    db = sqlite3.connect(path)
    try:
        rows = db.execute(
            "SELECT job_id, epoch, fence, state FROM job_attempts "
            "WHERE state IN ('dispatched', 'running') ORDER BY job_id"
        ).fetchall()
    finally:
        db.close()
    return {job_id: (epoch, fence, state)
            for job_id, epoch, fence, state in rows}


class ProjectionWorld(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "projection.db"

    def make_world(self, *, capacity=1, attempts=(), candidates=(),
                   tenants=((1, 9), (2, 9), (3, 9)), holds=(),
                   pool_cls=ConnectionPool, backfill=(1, 2, 3),
                   dirty_limit=2, overflow=None):
        create_dispatch_db(
            self.path,
            tenants=tenants,
            holds=holds,
            candidates=candidates,
            attempts=attempts)
        pool = pool_cls(self.path, capacity=capacity)
        self.addCleanup(pool.close)
        self.pool = pool
        index = ReservationIndex(pool)
        index.initialize()
        initialize_dispatch(pool)
        finish_backfill(index, backfill)
        kwargs = {"dirty_limit": dirty_limit}
        if overflow is not None:
            kwargs["overflow"] = overflow
        enable_projection(pool, **kwargs)
        return pool

    def pull_full(self, pool=None):
        ticket = next_projection(pool or self.pool)
        self.assertIsNotNone(ticket)
        self.assertEqual(ticket[1], "full")
        return ticket


class EnableAndValidationTest(ProjectionWorld):
    def test_entries_reject_before_enabling(self):
        create_dispatch_db(self.path)
        with ConnectionPool(self.path, capacity=1) as pool:
            with self.assertRaises(ValueError):
                next_projection(pool)
            with self.assertRaises(ValueError):
                ack_projection(pool, 1)

    def test_dirty_limit_bounds_and_types(self):
        create_dispatch_db(self.path)
        with ConnectionPool(self.path, capacity=1) as pool:
            for bad in (DIRTY_LIMIT_MIN - 1, DIRTY_LIMIT_MAX + 1, 0, -5,
                        True, False, 2.0, "2", None):
                with self.assertRaises(ValueError):
                    enable_projection(pool, dirty_limit=bad)
            # Valid boundaries are accepted.
            enable_projection(pool, dirty_limit=DIRTY_LIMIT_MIN)
            with self.assertRaises(ValueError):
                enable_projection(pool, dirty_limit=3)

    def test_invalid_overflow_is_rejected(self):
        create_dispatch_db(self.path)
        with ConnectionPool(self.path, capacity=1) as pool:
            for bad in ("abort", "defer ", "", 1, True, 0.5):
                with self.assertRaises(ValueError):
                    enable_projection(pool, dirty_limit=2, overflow=bad)
        # The persisted policy cannot be flipped online.
        other = self.root / "policy.db"
        create_dispatch_db(other)
        with ConnectionPool(other, capacity=1) as pool:
            enable_projection(pool, dirty_limit=2, overflow="commit")
            with self.assertRaises(ValueError):
                enable_projection(pool, dirty_limit=2, overflow="defer")
            # Re-confirming the same policy (or omitting it) is accepted.
            enable_projection(pool, dirty_limit=2, overflow="commit")
            enable_projection(pool, dirty_limit=2)

    def test_enable_requires_job_attempts_table(self):
        path = self.root / "bare.db"
        db = sqlite3.connect(path)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("CREATE TABLE unrelated (x INTEGER)")
        db.commit()
        db.close()
        with ConnectionPool(path, capacity=1) as pool:
            with self.assertRaises(ValueError):
                enable_projection(pool, dirty_limit=2)
            # No component tables were left behind on the early rejection.
            with pool.borrow() as dbc:
                self.assertIsNone(dbc.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' "
                    "AND name = 'projection_state'").fetchone())

    def test_reopen_keeps_progress_and_rejects_different_limit(self):
        pool = self.make_world(attempts=(
            (30, 7, 1, "dispatched", None, None),
            (40, 8, 1, "running", None, None),
        ))
        first = next_projection(pool)
        self.assertEqual(first[0], 1)
        self.assertEqual(first[1], "full")
        self.assertEqual(first[2], [
            (30, (7, 1, "dispatched")),
            (40, (8, 1, "running")),
        ])
        pool.close()

        with ConnectionPool(self.path, capacity=1) as pool2:
            enable_projection(pool2, dirty_limit=2)
            # Unconfirmed ticket survives the restart unchanged.
            self.assertEqual(next_projection(pool2), first)
            with self.assertRaises(ValueError):
                enable_projection(pool2, dirty_limit=3)
            self.assertTrue(ack_projection(pool2, first[0]))

    def test_empty_first_full_and_ack_of_foreign_tickets(self):
        pool = self.make_world(attempts=(
            (50, 9, 1, "uncertain", None, None),
            (51, 9, 1, "finished", None, None),
            (52, 9, 1, "retryable", None, None),
        ))
        first = next_projection(pool)
        self.assertEqual(first, (1, "full", []))

        # While ticket 1 is unconfirmed a pull returns it unchanged.
        self.assertEqual(next_projection(pool), first)

        self.assertTrue(ack_projection(pool, 1))
        # Now nothing is pending and the roster did not change.
        self.assertIsNone(next_projection(pool))
        # Non-current but legal positive tickets simply return False.
        for other in (2, 999, 10 ** 9):
            self.assertFalse(ack_projection(pool, other))
        # Still acknowledged state; nothing to advance.
        self.assertFalse(ack_projection(pool, 1))
        self.assertIsNone(next_projection(pool))

    def test_fresh_pool_over_enabled_database_can_pull_without_reenable(self):
        pool = self.make_world(attempts=(
            (30, 7, 1, "dispatched", None, None),
        ))
        first = next_projection(pool)
        pool.close()
        # A brand new pool over the already-enabled database works directly.
        with ConnectionPool(self.path, capacity=1) as fresh:
            self.assertEqual(next_projection(fresh), first)
            self.assertTrue(ack_projection(fresh, first[0]))
            self.assertIsNone(next_projection(fresh))

    def test_ack_validation(self):
        pool = self.make_world()
        for bad in (0, -1, True, False, 1.0, "1", None):
            with self.assertRaises(ValueError):
                ack_projection(pool, bad)


class FirstFullAndPinTest(ProjectionWorld):
    def test_first_full_includes_preexisting_active_and_excludes_inactive(self):
        pool = self.make_world(attempts=(
            (30, 7, 1, "dispatched", None, None),
            (40, 8, 1, "running", None, None),
            (50, 9, 1, "uncertain", None, None),
            (51, 9, 1, "finished", "ok", b"x"),
            (52, 9, 1, "retryable", None, None),
            (60, 9, 1, "dispatched", None, None),
        ))
        ticket = next_projection(pool)
        self.assertEqual(ticket[0], 1)
        self.assertEqual(ticket[1], "full")
        self.assertEqual(ticket[2], [
            (30, (7, 1, "dispatched")),
            (40, (8, 1, "running")),
            (60, (9, 1, "dispatched")),
        ])

    def test_unconfirmed_ticket_is_pinned_against_later_commits(self):
        pool = self.make_world(attempts=(
            (30, 7, 1, "dispatched", None, None),
        ))
        first = next_projection(pool)
        # A later commit changes the roster.
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'running' WHERE job_id = 30")
            db.execute(
                "INSERT INTO job_attempts "
                "(job_id, epoch, fence, state, outcome, receipt) "
                "VALUES (41, 8, 1, 'dispatched', NULL, NULL)")
            db.commit()
        # Pulling again returns the original content, not the newer state.
        self.assertEqual(next_projection(pool), first)

        self.assertTrue(ack_projection(pool, first[0]))
        patch = next_projection(pool)
        self.assertEqual(patch[1], "patch")
        self.assertEqual(patch[2], [
            (30, (7, 1, "running")),
            (41, (8, 1, "dispatched")),
        ])
        self.assertTrue(ack_projection(pool, patch[0]))
        self.assertIsNone(next_projection(pool))

    def test_calls_do_not_hold_the_only_connection(self):
        pool = self.make_world(capacity=1)
        next_projection(pool)
        with pool.borrow(timeout=0):
            pass
        ack_projection(pool, 1)
        with pool.borrow(timeout=0):
            pass


class DirtyTrackingTest(ProjectionWorld):
    def test_same_job_updates_collapse_and_inactive_updates_do_not_count(self):
        pool = self.make_world(attempts=(
            (30, 7, 1, "dispatched", None, None),
            (40, 8, 1, "dispatched", None, None),
            (50, 9, 1, "uncertain", None, None),
        ))
        first = self.pull_full(pool)
        self.assertTrue(ack_projection(pool, first[0]))

        # Repeated updates of the same active job consume one budget item.
        for state in ("running", "dispatched", "running"):
            with pool.borrow() as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    "UPDATE job_attempts SET state = ? WHERE job_id = 30",
                    (state,))
                db.commit()
        # Updating the already inactive 50 never enters the roster changes.
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'finished' WHERE job_id = 50")
            db.commit()

        with pool.borrow() as db:
            dirty = db.execute(
                "SELECT job_id FROM projection_dirty ORDER BY job_id").fetchall()
        self.assertEqual(dirty, [(30,)])
        patch = next_projection(pool)
        self.assertEqual(patch[2], [(30, (7, 1, "running"))])

    def test_rolled_back_and_same_content_writes_create_no_ticket(self):
        pool = self.make_world(attempts=(
            (30, 7, 1, "dispatched", None, None),
        ))
        first = self.pull_full(pool)
        self.assertTrue(ack_projection(pool, first[0]))

        # Roll an active change back.
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'running' WHERE job_id = 30")
            db.rollback()
        # Commit a write that does not change the roster entry content.
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET outcome = 'pending', "
                "receipt = X'00' WHERE job_id = 30")
            db.commit()

        self.assertIsNone(next_projection(pool))

    def test_inactive_and_reconcile_style_writes_are_not_blocked(self):
        pool = self.make_world(attempts=(
            (30, 7, 1, "dispatched", None, None),
            (40, 8, 1, "dispatched", None, None),
            (50, 9, 1, "uncertain", None, None),
        ), dirty_limit=2)
        first = self.pull_full(pool)
        self.assertTrue(ack_projection(pool, first[0]))

        # Fill the pending budget with 30 and 40.
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'running' WHERE job_id = 30")
            db.execute(
                "UPDATE job_attempts SET state = 'running' WHERE job_id = 40")
            db.commit()

        # Reconcile-style writes on uncertain rows (and their deletion) do not
        # change the active roster and therefore are never backlog-blocked.
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'finished', outcome = 'ok', "
                "receipt = X'01' WHERE job_id = 50")
            db.execute("DELETE FROM job_attempts WHERE job_id = 50")
            db.commit()
        # The reconcile-style writes committed (they were not rejected); the
        # patch still contains only the two active changes.
        patch = next_projection(pool)
        self.assertEqual(patch[1], "patch")
        self.assertEqual(patch[2], [
            (30, (7, 1, "running")),
            (40, (8, 1, "running")),
        ])

    def test_state_and_identity_change_across_commits_without_statement_limit(self):
        pool = self.make_world(attempts=(
            (30, 7, 1, "dispatched", None, None),
            (40, 8, 1, "dispatched", None, None),
        ), dirty_limit=2)
        first = self.pull_full(pool)
        self.assertTrue(ack_projection(pool, first[0]))

        # Commit one: change state of the same active job.
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'running' WHERE job_id = 30")
            db.commit()
        # Commit two: new attempt identity (epoch/fence) of the same job.
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET epoch = 11, fence = 2 WHERE job_id = 30")
            db.commit()
        # Delete another active job.
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("DELETE FROM job_attempts WHERE job_id = 40")
            db.commit()

        patch = next_projection(pool)
        self.assertEqual(patch[1], "patch")
        self.assertEqual(patch[2], [
            (30, (11, 2, "running")),
            (40, None),
        ])

    def test_reverting_to_original_value_may_keep_the_item(self):
        pool = self.make_world(attempts=(
            (30, 7, 1, "dispatched", None, None),
        ))
        first = self.pull_full(pool)
        self.assertTrue(ack_projection(pool, first[0]))
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'running' WHERE job_id = 30")
            db.commit()
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'dispatched' WHERE job_id = 30")
            db.commit()
        patch = next_projection(pool)
        # Keeping the item is allowed; it simply re-delivers the original value,
        # which the monitor applies as a no-op.
        self.assertEqual(patch[2], [(30, (7, 1, "dispatched"))])

    def test_pull_and_ack_leave_no_open_transaction(self):
        pool = self.make_world(attempts=(
            (30, 7, 1, "dispatched", None, None),
        ), capacity=1)
        first = next_projection(pool)
        with pool.borrow(timeout=0) as db:
            self.assertFalse(db.in_transaction)
        self.assertTrue(ack_projection(pool, first[0]))
        with pool.borrow(timeout=0) as db:
            self.assertFalse(db.in_transaction)

    def test_patch_query_plan_uses_primary_key_lookups(self):
        pool = self.make_world(attempts=(
            (30, 7, 1, "dispatched", None, None),
            (40, 8, 1, "dispatched", None, None),
        ))
        first = self.pull_full(pool)
        self.assertTrue(ack_projection(pool, first[0]))
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'running' WHERE job_id = 30")
            db.commit()
        with pool.borrow() as db:
            plan = "\n".join(
                row[3] for row in db.execute(
                    "EXPLAIN QUERY PLAN "
                    "SELECT d.job_id, a.epoch, a.fence, a.state "
                    "FROM projection_dirty AS d "
                    "LEFT JOIN job_attempts AS a ON a.job_id = d.job_id "
                    "ORDER BY d.job_id"))
        # The attempt side is reached by INTEGER PRIMARY KEY (rowid) lookup,
        # never by a full scan of job_attempts.
        self.assertIn("SEARCH", plan)
        self.assertIn("INTEGER PRIMARY KEY", plan)
        self.assertNotIn("SCAN job_attempts", plan)

    def test_patch_does_not_scan_the_attempt_table(self):
        # The patch resolves only the bounded dirty set by primary key; with a
        # large unrelated attempt table that still has to be a point lookup.
        attempts = [(job_id, 1, 1, "finished", None, None)
                    for job_id in range(1, 500)]
        pool = self.make_world(attempts=attempts)
        first = self.pull_full(pool)
        self.assertTrue(ack_projection(pool, first[0]))
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "INSERT INTO job_attempts "
                "(job_id, epoch, fence, state, outcome, receipt) "
                "VALUES (900, 5, 1, 'dispatched', NULL, NULL)")
            db.commit()
        patch = next_projection(pool)
        self.assertEqual(patch[2], [(900, (5, 1, "dispatched"))])


class DeferOverflowAcceptanceTest(ProjectionWorld):
    """Acceptance scenario 1, defer policy, capacity 1 / dirty_limit 2."""

    def setUp(self):
        super().setUp()
        self.pool = self.make_world(
            attempts=(
                (30, 7, 1, "dispatched", None, None),
                (40, 8, 1, "dispatched", None, None),
                (50, 9, 1, "uncertain", None, None),
            ),
            candidates=(),
            overflow="defer",
            dirty_limit=2)

    def _add_candidates(self):
        with self.pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "INSERT INTO dispatch_ready VALUES (13, 2, 205, 1)")
            db.execute(
                "INSERT INTO dispatch_ready VALUES (27, 1, 101, 1)")
            db.commit()

    def test_backlog_defers_dispatch_then_retry_succeeds(self):
        pool = self.pool
        first = next_projection(pool)
        self.assertEqual(first[1], "full")
        self.assertEqual(first[2], [
            (30, (7, 1, "dispatched")),
            (40, (8, 1, "dispatched")),
        ])

        # Caller marks 30 running and 40 uncertain via the original borrow
        # path, in one real commit; the two fill the budget exactly.
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'running' WHERE job_id = 30")
            db.execute(
                "UPDATE job_attempts SET state = 'uncertain' WHERE job_id = 40")
            db.commit()
        # Updating the inactive 50 cannot be rejected.
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'retryable' WHERE job_id = 50")
            db.commit()

        self._add_candidates()
        before_dispatch = dump_database(self.path)

        # The over-limit dispatch must stop its whole batch.
        workers = [(101, 1001, [1]), (102, 1002, [1])]
        with self.assertRaises(sqlite3.IntegrityError) as raised:
            dispatch_wave(pool, workers)
        self.assertIn("projection_backlog", str(raised.exception))
        self.assertEqual(dump_database(self.path), before_dispatch)

        # Close/reopen: the unconfirmed ticket must be byte-identical.
        pool.close()
        with ConnectionPool(self.path, capacity=1) as reopened:
            enable_projection(reopened, dirty_limit=2)
            self.assertEqual(next_projection(reopened), first)
            self.assertTrue(ack_projection(reopened, first[0]))

            patch = next_projection(reopened)
            self.assertEqual(patch[0], 2)
            self.assertEqual(patch[1], "patch")
            self.assertEqual(patch[2], [
                (30, (7, 1, "running")),
                (40, None),
            ])
            # Pulling (not yet acking) the patch frees the backlog; the same
            # dispatch now really allocates 13 and 27.
            result = dispatch_wave(reopened, workers)
            self.assertEqual(result, ("committed", [(13, 101), (27, 102)]))
            self.assertFalse(ack_projection(reopened, first[0]))

            # The committed dispatch stays pending behind the unconfirmed
            # patch; pulling still returns that patch.
            self.assertEqual(next_projection(reopened), patch)
            self.assertTrue(ack_projection(reopened, patch[0]))
            after = next_projection(reopened)
            self.assertEqual(after[1], "patch")
            self.assertEqual(after[2], [
                (13, (1001, 1, "dispatched")),
                (27, (1002, 1, "dispatched")),
            ])

            roster = {}
            for ticket in (first, patch, after):
                apply_rows(roster, ticket[2], ticket[1])
            self.assertEqual(roster, active_rows(self.path))
            self.assertFalse(ack_projection(reopened, first[0]))

    def test_retry_allowed_even_with_active_dirty_budget(self):
        # After acking the patch and pulling it, two more active inserts fill
        # the budget again without rejecting the retried dispatch.
        pool = self.pool
        first = next_projection(pool)
        self.assertTrue(ack_projection(pool, first[0]))
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'running' WHERE job_id = 30")
            db.execute(
                "UPDATE job_attempts SET state = 'uncertain' WHERE job_id = 40")
            db.commit()
        self._add_candidates()
        patch = next_projection(pool)
        self.assertEqual(patch[1], "patch")
        # Not acknowledged yet, but the pending budget is free.
        result = dispatch_wave(pool, [(101, 1001, [1]), (102, 1002, [1])])
        self.assertEqual(result, ("committed", [(13, 101), (27, 102)]))
        with pool.borrow() as db:
            dirty = db.execute(
                "SELECT job_id FROM projection_dirty ORDER BY job_id").fetchall()
        self.assertEqual(dirty, [(13,), (27,)])
        self.assertTrue(ack_projection(pool, patch[0]))
        follow = next_projection(pool)
        self.assertEqual(follow[2], [
            (13, (1001, 1, "dispatched")),
            (27, (1002, 1, "dispatched")),
        ])


class CommitOverflowAcceptanceTest(ProjectionWorld):
    """Acceptance scenario 1, commit policy, capacity 1 / dirty_limit 2."""

    def setUp(self):
        super().setUp()
        self.pool = self.make_world(
            attempts=(
                (30, 7, 1, "dispatched", None, None),
                (40, 8, 1, "dispatched", None, None),
                (50, 9, 1, "uncertain", None, None),
            ),
            candidates=(),
            overflow="commit",
            dirty_limit=2)

    def test_overflow_commits_and_next_ticket_is_a_full(self):
        pool = self.pool
        first = next_projection(pool)  # stays unconfirmed across the overflow

        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'running' WHERE job_id = 30")
            db.execute(
                "UPDATE job_attempts SET state = 'uncertain' WHERE job_id = 40")
            db.commit()

        # The over-limit dispatch commits and allocates 13 / 27.
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT INTO dispatch_ready VALUES (13, 2, 205, 1)")
            db.execute("INSERT INTO dispatch_ready VALUES (27, 1, 101, 1)")
            db.commit()
        workers = [(101, 1001, [1]), (102, 1002, [1])]
        result = dispatch_wave(pool, workers)
        self.assertEqual(result, ("committed", [(13, 101), (27, 102)]))

        # The old delivered ticket must not be rewritten with current data.
        self.assertEqual(next_projection(pool), first)
        pool.close()
        with ConnectionPool(self.path, capacity=1) as reopened:
            enable_projection(reopened, dirty_limit=2)
            self.assertEqual(next_projection(reopened), first)
            self.assertTrue(ack_projection(reopened, first[0]))

            full = next_projection(reopened)
            self.assertEqual(full[0], 2)
            self.assertEqual(full[1], "full")
            self.assertEqual(full[2], [
                (13, (1001, 1, "dispatched")),
                (27, (1002, 1, "dispatched")),
                (30, (7, 1, "running")),
            ])

            # While the full is unconfirmed an active job exits.
            with reopened.borrow() as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    "UPDATE job_attempts SET state = 'finished' WHERE job_id = 30")
                db.commit()
            # Pulling again yields the same full, not the newer state.
            self.assertEqual(next_projection(reopened), full)

            self.assertTrue(ack_projection(reopened, full[0]))
            removal = next_projection(reopened)
            self.assertEqual(removal[1], "patch")
            self.assertEqual(removal[2], [(30, None)])

            roster = {}
            apply_rows(roster, first[2], "full")
            apply_rows(roster, full[2], "full")
            apply_rows(roster, removal[2], "patch")
            self.assertEqual(roster, active_rows(self.path))
            # A late acknowledgement of the old ticket does not advance the
            # new ticket.
            self.assertFalse(ack_projection(reopened, first[0]))
            self.assertFalse(ack_projection(reopened, 1))

    def test_unconfirmed_old_ticket_is_not_replaced_by_the_full(self):
        pool = self.pool
        first = next_projection(pool)  # remains unconfirmed
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'running' WHERE job_id = 30")
            db.execute(
                "UPDATE job_attempts SET state = 'uncertain' WHERE job_id = 40")
            db.commit()
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT INTO dispatch_ready VALUES (13, 2, 205, 1)")
            db.execute("INSERT INTO dispatch_ready VALUES (27, 1, 101, 1)")
            db.commit()
        result = dispatch_wave(pool, [(101, 1001, [1]), (102, 1002, [1])])
        self.assertEqual(result[0], "committed")

        # The pending full cannot replace the delivered-but-unconfirmed initial
        # ticket; pulls keep returning it unchanged.
        self.assertEqual(next_projection(pool), first)
        self.assertTrue(ack_projection(pool, first[0]))
        full = next_projection(pool)
        self.assertEqual(full[1], "full")
        self.assertIn((13, (1001, 1, "dispatched")), full[2])
        self.assertIn((27, (1002, 1, "dispatched")), full[2])
        # Old ticket number now stale.
        self.assertFalse(ack_projection(pool, first[0]))

    def test_post_full_changes_still_enter_later_batches(self):
        pool = self.pool
        first = next_projection(pool)
        self.assertTrue(ack_projection(pool, first[0]))
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'running' WHERE job_id = 30")
            db.execute(
                "UPDATE job_attempts SET state = 'uncertain' WHERE job_id = 40")
            db.commit()
        # Force a full via a real overflow without touching dispatch.
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "INSERT INTO job_attempts "
                "(job_id, epoch, fence, state, outcome, receipt) "
                "VALUES (60, 1, 1, 'dispatched', NULL, NULL)")
            db.commit()
        full = next_projection(pool)
        self.assertEqual(full[1], "full")
        self.assertTrue(ack_projection(pool, full[0]))

        # A change right after the full was delivered reaches the next patch.
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'finished' WHERE job_id = 60")
            db.commit()
        patch = next_projection(pool)
        self.assertEqual(patch[1], "patch")
        self.assertEqual(patch[2], [(60, None)])


class NoBacklogFailureTest(ProjectionWorld):
    def test_original_error_and_no_ghost_ticket(self):
        attempts = ((30, 7, 1, "dispatched", None, None),)
        pool = self.make_world(
            pool_cls=FailurePool,
            attempts=attempts,
            candidates=((11, 1, 101, 1), (12, 1, 102, 1)),
            tenants=((1, 4),),
            backfill=(1,))
        first = next_projection(pool)
        self.assertTrue(ack_projection(pool, first[0]))
        self.assertIsNone(next_projection(pool))

        before = dump_database(self.path)
        pool.fail_on_attempt_number = 2
        pool.attempt_inserts = 0
        with self.assertRaisesRegex(
                sqlite3.OperationalError, "injected second attempt failure"):
            dispatch_wave(pool, [(101, 1001, [1]), (102, 1002, [1])])
        self.assertEqual(dump_database(self.path), before)

        # The rolled back attempt inserts left no dirty rows and no ghost
        # ticket; the ticket counter did not advance behind our back.
        self.assertIsNone(next_projection(pool))

        # A later genuine change receives the next ticket number (2).
        pool.fail_on_attempt_number = None
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'running' WHERE job_id = 30")
            db.commit()
        change = next_projection(pool)
        self.assertEqual(change[0], 2)
        self.assertEqual(change[1], "patch")
        self.assertEqual(change[2], [(30, (7, 1, "running"))])


class InterleavingTest(ProjectionWorld):
    def test_business_commit_with_monitor_pull_in_sequence_is_consistent(self):
        # A monitor applying the ticket and a business commit interleave; the
        # ticket still returns exactly its fetch-point content, then the next
        # pull sees the committed change.
        pool = self.make_world(attempts=(
            (30, 7, 1, "dispatched", None, None),
            (40, 8, 1, "dispatched", None, None),
        ), capacity=1)
        first = next_projection(pool)
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'running' WHERE job_id = 30")
            db.commit()
        # Content pinned even though the commit landed between fetch points.
        self.assertEqual(next_projection(pool), first)
        self.assertTrue(ack_projection(pool, first[0]))
        patch = next_projection(pool)
        self.assertEqual(patch[2], [(30, (7, 1, "running"))])

    def test_many_alternating_updates_keep_roster_consistent(self):
        pool = self.make_world(attempts=(
            (job_id, 7, 1, "dispatched", None, None)
            for job_id in (10, 20, 30)
        ), dirty_limit=2)
        roster = {}
        ticket = next_projection(pool)
        apply_rows(roster, ticket[2], ticket[1])
        ack_projection(pool, ticket[0])

        # Each step changes exactly one job so the budget never overflows.
        steps = [
            ("UPDATE job_attempts SET state='running' WHERE job_id=10"),
            ("UPDATE job_attempts SET state='running' WHERE job_id=20"),
            ("UPDATE job_attempts SET state='finished' WHERE job_id=30"),
            ("INSERT INTO job_attempts "
             "(job_id, epoch, fence, state, outcome, receipt) "
             "VALUES (40, 9, 1, 'dispatched', NULL, NULL)"),
            ("UPDATE job_attempts SET fence=2 WHERE job_id=40"),
            ("DELETE FROM job_attempts WHERE job_id=10"),
        ]
        for statement in steps:
            with pool.borrow() as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute(statement)
                db.commit()
            ticket = next_projection(pool)
            self.assertIsNotNone(ticket)
            apply_rows(roster, ticket[2], ticket[1])
            self.assertTrue(ack_projection(pool, ticket[0]))
            self.assertEqual(roster, active_rows(self.path))


class ResourceReleaseTest(ProjectionWorld):
    def test_pull_raises_then_pool_still_borrowable(self):
        pool = self.make_world(capacity=1)
        next_projection(pool)
        # Not enabled on a fresh pool over a different database.
        other = self.root / "other.db"
        create_dispatch_db(other)
        with ConnectionPool(other, capacity=1) as other_pool:
            with self.assertRaises(ValueError):
                next_projection(other_pool)
            with other_pool.borrow(timeout=0):
                pass
            with self.assertRaises(ValueError):
                ack_projection(other_pool, 1)
            with other_pool.borrow(timeout=0):
                pass


class ExplicitInterleaveTest(ProjectionWorld):
    """Interleavings built from explicit commits, never from timing/sleeps."""

    def test_commit_between_pull_and_ack_is_not_mixed_into_old_ticket(self):
        pool = self.make_world(
            attempts=(
                (30, 7, 1, "dispatched", None, None),
            ),
            capacity=1,
            dirty_limit=4)
        first = next_projection(pool)
        # Between pull and ack a new active job commits.
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "INSERT INTO job_attempts "
                "(job_id, epoch, fence, state, outcome, receipt) "
                "VALUES (60, 9, 1, 'dispatched', NULL, NULL)")
            db.commit()
        # Acking the already-fetched ticket does not retroactively include 60.
        self.assertTrue(ack_projection(pool, first[0]))
        patch = next_projection(pool)
        self.assertEqual(patch[2], [(60, (9, 1, "dispatched"))])

    def test_multiple_commits_then_one_patch_carries_final_states(self):
        pool = self.make_world(
            attempts=(
                (30, 7, 1, "dispatched", None, None),
                (40, 8, 1, "dispatched", None, None),
            ),
            capacity=1,
            dirty_limit=4)
        roster = {}
        first = next_projection(pool)
        apply_rows(roster, first[2], first[1])
        self.assertTrue(ack_projection(pool, first[0]))

        # 30 changes state in one commit, then its identity in another, then
        # exits; 40 is deleted.  All of these happened before any pull.
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'running' WHERE job_id = 30")
            db.commit()
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET epoch = 12, fence = 7 WHERE job_id = 30")
            db.commit()
        with pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE job_attempts SET state = 'finished' WHERE job_id = 30")
            db.execute("DELETE FROM job_attempts WHERE job_id = 40")
            db.commit()

        patch = next_projection(pool)
        self.assertEqual(patch[2], [(30, None), (40, None)])
        apply_rows(roster, patch[2], patch[1])
        self.assertEqual(roster, active_rows(self.path))


class BoundsTest(ProjectionWorld):
    def test_roster_over_the_limit_is_rejected(self):
        attempts = [(job_id, 1, 1, "dispatched", None, None)
                    for job_id in range(1, ROSTER_MAX + 2)]
        pool = self.make_world(attempts=attempts)
        with self.assertRaises(ValueError):
            next_projection(pool)

    def test_largest_roster_is_one_full_ticket(self):
        attempts = [(job_id, 1, 1, "dispatched", None, None)
                    for job_id in range(1, ROSTER_MAX + 1)]
        pool = self.make_world(attempts=attempts)
        ticket = next_projection(pool)
        self.assertEqual(ticket[1], "full")
        self.assertEqual(len(ticket[2]), ROSTER_MAX)
        self.assertEqual(ticket[2][0][0], 1)
        self.assertEqual(ticket[2][-1][0], ROSTER_MAX)
        self.assertEqual(ack_projection(pool, ticket[0]), True)


class OriginalBehaviourTest(ProjectionWorld):
    def test_dispatch_unchanged_without_projection(self):
        create_dispatch_db(self.path)
        with ConnectionPool(self.path, capacity=1) as pool:
            index = ReservationIndex(pool)
            index.initialize()
            initialize_dispatch(pool)
            finish_backfill(index, (1, 2, 3))
            self.assertEqual(
                dispatch_wave(pool, [(101, 1001, [1, 2]),
                                     (102, 1002, [1])]),
                ("committed", [(11, 102), (21, 101)]))
            # No projection tables are created while disabled.
            with pool.borrow() as db:
                self.assertIsNone(db.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type = 'table' AND name = 'projection_state'"
                ).fetchone())


if __name__ == "__main__":
    unittest.main(verbosity=2)
