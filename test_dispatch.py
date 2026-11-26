"""批次派发组件的真实 WAL SQLite 测试。

全部仅使用标准库；每个用例都在独立的临时目录中建立真实 WAL 库和现有
``ConnectionPool``。竞争由 ``before_commit`` 回调中的实际提交确定性制造，
不使用睡眠。
"""

from contextlib import contextmanager
from pathlib import Path
import random
import sqlite3
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

from dispatch import dispatch_wave, initialize_dispatch, solve_match
from pool import ConnectionPool
from quota import ADVANCE_MAX, SLOT_MAX, ReservationIndex


PROJECT_DIR = Path(__file__).resolve().parent


def create_dispatch_db(path, *, tenants=((1, 2), (2, 2), (3, 5)),
                       holds=((1, 901, 1), (2, 902, 1)),
                       candidates=(
                           (11, 1, 101, 1),
                           (12, 1, 102, 2),
                           (21, 2, 201, 2),
                       ),
                       attempts=()):
    """建立调用方契约中的完整调度库。"""
    db = sqlite3.connect(path)
    try:
        mode = db.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        assert mode.lower() == "wal"
        db.execute(
            "CREATE TABLE quota_tenants ("
            "tenant_id INTEGER PRIMARY KEY, max_units INTEGER NOT NULL)")
        db.execute(
            "CREATE TABLE quota_holds ("
            "tenant_id INTEGER NOT NULL, slot INTEGER NOT NULL, "
            "units INTEGER NOT NULL, PRIMARY KEY(tenant_id, slot))")
        db.execute(
            "CREATE TABLE job_attempts ("
            "job_id INTEGER PRIMARY KEY, epoch INTEGER NOT NULL, "
            "fence INTEGER NOT NULL, state TEXT NOT NULL, "
            "outcome TEXT, receipt BLOB)")
        db.execute(
            "CREATE TABLE dispatch_ready ("
            "job_id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL, "
            "slot INTEGER NOT NULL, kind INTEGER NOT NULL, "
            "UNIQUE(tenant_id, slot))")
        db.executemany("INSERT INTO quota_tenants VALUES (?, ?)", tenants)
        db.executemany("INSERT INTO quota_holds VALUES (?, ?, ?)", holds)
        db.executemany(
            "INSERT INTO dispatch_ready VALUES (?, ?, ?, ?)", candidates)
        db.executemany(
            "INSERT INTO job_attempts "
            "(job_id, epoch, fence, state, outcome, receipt) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [(job_id, epoch, fence, state, outcome, receipt)
             for job_id, epoch, fence, state, outcome, receipt in attempts])
        db.commit()
    finally:
        db.close()


def finish_backfill(index, tenant_ids):
    for tenant_id in tenant_ids:
        while index.status(tenant_id)[0] < SLOT_MAX:
            index.advance(tenant_id, ADVANCE_MAX)


def dump_database(path):
    """从独立连接读取地面真值，供整批回滚前后精确比较。"""
    db = sqlite3.connect(path)
    try:
        return {
            "ready": tuple(db.execute(
                "SELECT job_id, tenant_id, slot, kind "
                "FROM dispatch_ready ORDER BY job_id").fetchall()),
            "holds": tuple(db.execute(
                "SELECT tenant_id, slot, units FROM quota_holds "
                "ORDER BY tenant_id, slot").fetchall()),
            "index": tuple(db.execute(
                "SELECT tenant_id, covered_slots, used_units, ready "
                "FROM quota_index ORDER BY tenant_id").fetchall()),
            "attempts": tuple(db.execute(
                "SELECT job_id, epoch, fence, state, outcome, receipt "
                "FROM job_attempts ORDER BY job_id").fetchall()),
        }
    finally:
        db.close()


class CountingPool(ConnectionPool):
    """记录 dispatch 自身和测试回调的借用次数。"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.borrow_count = 0

    @contextmanager
    def borrow(self, *args, **kwargs):
        self.borrow_count += 1
        with super().borrow(*args, **kwargs) as db:
            yield db


class FailurePool(ConnectionPool):
    """在第 N 次 ``INSERT INTO job_attempts`` 时注入 SQLite 错误。"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.attempt_inserts = 0
        self.fail_on_attempt_number = None

    def _connect(self):
        pool = self

        class HookConnection(sqlite3.Connection):
            def execute(self, sql, parameters=()):
                if "INSERT INTO job_attempts" in sql:
                    pool.attempt_inserts += 1
                    if (pool.fail_on_attempt_number is not None
                            and pool.attempt_inserts
                            == pool.fail_on_attempt_number):
                        raise sqlite3.OperationalError(
                            "injected second attempt failure")
                return super().execute(sql, parameters)

        return sqlite3.connect(
            self._path,
            check_same_thread=False,
            factory=HookConnection)


class DispatchWorldTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "dispatch.db"

    def make_world(self, *, pool_cls=CountingPool, capacity=1,
                   snapshot_groups=False, backfill=(1, 2, 3), **db_kwargs):
        create_dispatch_db(self.path, **db_kwargs)
        pool_kwargs = {"capacity": capacity}
        if snapshot_groups:
            pool_kwargs["snapshot_groups"] = True
        pool = pool_cls(self.path, **pool_kwargs)
        self.addCleanup(pool.close)
        index = ReservationIndex(pool)
        index.initialize()
        initialize_dispatch(pool)
        finish_backfill(index, backfill)
        if isinstance(pool, CountingPool):
            pool.borrow_count = 0
        return pool, index

    def test_required_two_tenant_batch_beats_job_order_dispatch(self):
        pool, index = self.make_world()
        workers = [
            (101, 1001, [1, 2]),
            (102, 1002, [1]),
        ]

        self.assertEqual(index.status(1), (SLOT_MAX, 1))
        self.assertEqual(index.status(2), (SLOT_MAX, 1))
        pool.borrow_count = 0
        result = dispatch_wave(pool, workers)
        borrows_for_wave = pool.borrow_count

        self.assertEqual(result, ("committed", [(11, 102), (21, 101)]))
        self.assertEqual(borrows_for_wave, 2)

        db = sqlite3.connect(self.path)
        try:
            attempts = db.execute(
                "SELECT job_id, epoch, fence, state, outcome, receipt "
                "FROM job_attempts ORDER BY job_id").fetchall()
            self.assertEqual(attempts, [
                (11, 1002, 1, "dispatched", None, None),
                (21, 1001, 1, "dispatched", None, None),
            ])
            holds = db.execute(
                "SELECT tenant_id, slot, units FROM quota_holds "
                "ORDER BY tenant_id, slot").fetchall()
            self.assertEqual(holds, [
                (1, 101, 1),
                (1, 901, 1),
                (2, 201, 1),
                (2, 902, 1),
            ])
            remaining = db.execute(
                "SELECT job_id, tenant_id, slot, kind "
                "FROM dispatch_ready ORDER BY job_id").fetchall()
            self.assertEqual(remaining, [(12, 1, 102, 2)])
            margins = dict(db.execute(
                "SELECT t.tenant_id, t.max_units - i.used_units "
                "FROM quota_tenants t JOIN quota_index i "
                "USING(tenant_id) WHERE t.tenant_id IN (1,2)").fetchall())
            self.assertEqual(margins, {1: 0, 2: 0})
        finally:
            db.close()

        self.assertEqual(index.status(1), (SLOT_MAX, 2))
        self.assertEqual(index.status(2), (SLOT_MAX, 2))

    def test_equal_cardinality_ties_use_lexicographic_pair_sequence(self):
        # 故意打乱候选物理插入顺序和 worker 输入顺序。
        pool, _index = self.make_world(
            tenants=((1, 10),),
            holds=(),
            candidates=(
                (2, 1, 12, 7),
                (1, 1, 11, 7),
            ),
            backfill=(1,))
        workers = [
            (20, 2, [7]),
            (10, 1, [7]),
        ]
        self.assertEqual(
            dispatch_wave(pool, workers),
            ("committed", [(1, 10), (2, 20)]))

    def test_lex_choice_prefers_lower_job_then_lower_worker(self):
        pool, _index = self.make_world(
            tenants=((1, 1),),
            holds=(),
            candidates=(
                (2, 1, 12, 9),
                (1, 1, 11, 9),
            ),
            backfill=(1,))
        self.assertEqual(
            dispatch_wave(pool, [
                (2, 2, [9]),
                (1, 1, [9]),
            ]),
            ("committed", [(1, 1)]))

    def test_not_ready_or_occupied_candidate_cannot_start(self):
        # Tenant 1 不完成回填；tenant 2 就绪但 21 号号位已被占用。
        pool, index = self.make_world(
            tenants=((1, 10), (2, 5)),
            holds=(),
            candidates=(
                (11, 1, 101, 1),
                (21, 2, 201, 2),
            ),
            backfill=(2,))
        self.assertEqual(index.set_hold(2, 201, 1), "applied")

        calls = []

        def before_commit():
            calls.append(True)

        result = dispatch_wave(
            pool, [(101, 1, [1, 2])], before_commit=before_commit)
        self.assertEqual(result, ("committed", []))
        self.assertEqual(len(calls), 1)

        db = sqlite3.connect(self.path)
        try:
            self.assertEqual(
                [row[0] for row in db.execute(
                    "SELECT job_id FROM dispatch_ready ORDER BY job_id")],
                [11, 21])
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM job_attempts").fetchone()[0],
                0)
        finally:
            db.close()

    def test_tenant_batch_capacity_limits_starts(self):
        pool, index = self.make_world(
            tenants=((1, 1),),
            holds=(),
            candidates=(
                (1, 1, 101, 1),
                (2, 1, 102, 1),
            ),
            backfill=(1,))
        result = dispatch_wave(pool, [
            (2, 2, [1]),
            (1, 1, [1]),
        ])
        self.assertEqual(result, ("committed", [(1, 1)]))
        self.assertEqual(index.status(1)[1], 1)

    def test_stale_insert_of_lower_supported_job_forces_retry(self):
        pool, _index = self.make_world()
        workers = [(101, 1001, [1, 2]), (102, 1002, [1])]
        before = dump_database(self.path)

        def insert_in_capacity_one_pool():
            # 若 dispatch 仍持有唯一连接，这里会立即 PoolTimeout。
            with pool.borrow(timeout=0) as db:
                db.execute(
                    "INSERT INTO dispatch_ready "
                    "(job_id, tenant_id, slot, kind) "
                    "VALUES (5, 2, 205, 2)")
                db.commit()

        result = dispatch_wave(
            pool, workers, before_commit=insert_in_capacity_one_pool)
        self.assertEqual(result, ("stale", []))

        after_stale = dump_database(self.path)
        self.assertEqual(
            after_stale["ready"],
            ((5, 2, 205, 2),) + before["ready"])
        self.assertEqual(after_stale["holds"], before["holds"])
        self.assertEqual(after_stale["index"], before["index"])
        self.assertEqual(after_stale["attempts"], ())

        # 回调自己借过一次，dispatch 仍恰好借两次。
        self.assertEqual(pool.borrow_count, 3)

        self.assertEqual(
            dispatch_wave(pool, workers),
            ("committed", [(5, 101), (11, 102)]))

    def test_stale_quota_consumption_forces_retry(self):
        pool, index = self.make_world()
        workers = [(101, 1001, [1, 2]), (102, 1002, [1])]

        def consume_tenant_one_headroom():
            self.assertEqual(index.set_hold(1, 999, 1), "applied")

        result = dispatch_wave(
            pool, workers, before_commit=consume_tenant_one_headroom)
        self.assertEqual(result, ("stale", []))

        db = sqlite3.connect(self.path)
        try:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM job_attempts").fetchone()[0],
                0)
            self.assertEqual(
                [row[0] for row in db.execute(
                    "SELECT job_id FROM dispatch_ready ORDER BY job_id")],
                [11, 12, 21])
            self.assertIsNotNone(db.execute(
                "SELECT 1 FROM quota_holds WHERE tenant_id=1 AND slot=999"
            ).fetchone())
        finally:
            db.close()

        self.assertEqual(
            dispatch_wave(pool, workers),
            ("committed", [(21, 101)]))

    def test_unrelated_kind_and_tenant_change_does_not_stale(self):
        pool, index = self.make_world()
        workers = [(101, 1001, [1, 2]), (102, 1002, [1])]

        def add_unrelated_candidate_and_hold():
            with pool.borrow(timeout=0) as db:
                db.execute(
                    "INSERT INTO dispatch_ready "
                    "(job_id, tenant_id, slot, kind) "
                    "VALUES (33, 3, 301, 3)")
                db.commit()
            self.assertEqual(index.set_hold(3, 302, 1), "applied")

        result = dispatch_wave(
            pool, workers, before_commit=add_unrelated_candidate_and_hold)
        self.assertEqual(result, ("committed", [(11, 102), (21, 101)]))

        db = sqlite3.connect(self.path)
        try:
            self.assertEqual(
                db.execute(
                    "SELECT job_id, tenant_id, slot, kind "
                    "FROM dispatch_ready WHERE job_id=33").fetchone(),
                (33, 3, 301, 3))
            self.assertEqual(
                db.execute(
                    "SELECT units FROM quota_holds "
                    "WHERE tenant_id=3 AND slot=302").fetchone(),
                (1,))
        finally:
            db.close()

    def test_rolled_back_callback_write_does_not_stale(self):
        pool, _index = self.make_world()
        workers = [(101, 1001, [1, 2]), (102, 1002, [1])]

        def rolled_back_insert():
            with pool.borrow(timeout=0) as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    "INSERT INTO dispatch_ready "
                    "VALUES (5, 2, 205, 2)")
                db.rollback()

        self.assertEqual(
            dispatch_wave(pool, workers, before_commit=rolled_back_insert),
            ("committed", [(11, 102), (21, 101)]))
        db = sqlite3.connect(self.path)
        try:
            self.assertIsNone(db.execute(
                "SELECT 1 FROM dispatch_ready WHERE job_id=5").fetchone())
        finally:
            db.close()

    def test_callback_exception_propagates_without_dispatch_commit(self):
        pool, _index = self.make_world()
        calls = 0

        class Boom(RuntimeError):
            pass

        def callback():
            nonlocal calls
            calls += 1
            raise Boom("callback failed")

        with self.assertRaises(Boom):
            dispatch_wave(
                pool,
                [(101, 1, [1])],
                before_commit=callback)
        self.assertEqual(calls, 1)
        # 池连接没有被遗留占用：容量 1 仍可立即借用。
        with pool.borrow(timeout=0) as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM job_attempts").fetchone()[0],
                0)

    def test_empty_batch_still_invokes_callback_and_can_be_stale(self):
        pool, _index = self.make_world(candidates=())
        calls = 0

        def callback():
            nonlocal calls
            calls += 1

        self.assertEqual(
            dispatch_wave(pool, [(1, 1, [1])], before_commit=callback),
            ("committed", []))
        self.assertEqual(calls, 1)

        def insert_after_match():
            with pool.borrow(timeout=0) as db:
                db.execute(
                    "INSERT INTO dispatch_ready VALUES (1, 1, 1, 1)")
                db.commit()

        # Tenant 1 was backfilled with max_units=2 and seed hold 1, so it has
        # exactly one unit of headroom.
        self.assertEqual(
            dispatch_wave(
                pool, [(1, 1, [1])], before_commit=insert_after_match),
            ("stale", []))


class ValidationTest(unittest.TestCase):
    def assert_value_error(self, workers):
        with self.assertRaises(ValueError):
            dispatch_wave(object(), workers)

    def test_workers_shape_ranges_duplicates_and_bools(self):
        good = (1, 1, [1])
        self.assert_value_error([])
        self.assert_value_error([good] * 17)
        self.assert_value_error(object())
        self.assert_value_error([(1, 1)])
        self.assert_value_error([(1, 1, [1], 4)])
        self.assert_value_error([(True, 1, [1])])
        self.assert_value_error([(1, True, [1])])
        self.assert_value_error([(0, 1, [1])])
        self.assert_value_error([(2147483648, 1, [1])])
        self.assert_value_error([(1, 0, [1])])
        self.assert_value_error([(1, 2147483648, [1])])
        self.assert_value_error([(1, 1, [])])
        self.assert_value_error([(1, 1, list(range(1, 34)))])
        self.assert_value_error([(1, 1, [0])])
        self.assert_value_error([(1, 1, [33])])
        self.assert_value_error([(1, 1, [True])])
        self.assert_value_error([(1, 1, [1, 1])])
        self.assert_value_error([
            (1, 1, [1]),
            (1, 2, [2]),
        ])
        self.assert_value_error([(1, 1, "12")])

    def test_bad_before_commit_is_value_error(self):
        with self.assertRaises(ValueError):
            dispatch_wave(None, [(1, 1, [1])], before_commit=123)


class SolverPropertyTest(unittest.TestCase):
    @staticmethod
    def brute_force(candidates, occupied, headrooms, workers):
        worker_ids = sorted(w[0] for w in workers)
        kind_by_worker = {wid: set(kinds) for wid, kinds in workers}
        best = None

        def visit(index, used_workers, tenant_used, chosen):
            nonlocal best
            if index == len(candidates):
                value = tuple(chosen)
                if (best is None
                        or len(value) > len(best)
                        or (len(value) == len(best) and value < best)):
                    best = value
                return

            job_id, tenant_id, slot, kind = candidates[index]
            visit(index + 1, used_workers, tenant_used, chosen)

            if (tenant_id, slot) in occupied:
                return
            if tenant_used.get(tenant_id, 0) >= headrooms.get(tenant_id, 0):
                return

            for position, worker_id in enumerate(worker_ids):
                if position in used_workers:
                    continue
                if kind not in kind_by_worker[worker_id]:
                    continue
                used_workers.add(position)
                tenant_used[tenant_id] = tenant_used.get(tenant_id, 0) + 1
                chosen.append((job_id, worker_id))
                visit(index + 1, used_workers, tenant_used, chosen)
                chosen.pop()
                tenant_used[tenant_id] -= 1
                used_workers.remove(position)

        visit(0, set(), {}, [])
        return list(best or ())

    def test_random_small_cases_match_brute_force(self):
        rng = random.Random(20261126)
        for case in range(300):
            tenant_count = rng.randint(1, 3)
            headrooms = {
                tenant_id: rng.randint(0, 2)
                for tenant_id in range(1, tenant_count + 1)
            }
            job_count = rng.randint(0, 8)
            job_ids = rng.sample(range(1, 25), job_count)
            candidates = []
            used_slots = set()
            for job_id in job_ids:
                tenant_id = rng.randint(1, tenant_count)
                while True:
                    slot = rng.randint(1, 8)
                    key = (tenant_id, slot)
                    if key not in used_slots:
                        used_slots.add(key)
                        break
                candidates.append(
                    (job_id, tenant_id, slot, rng.randint(1, 4)))
            candidates.sort()

            occupied = {
                (tenant_id, slot)
                for tenant_id, slot in rng.sample(
                    list(used_slots), rng.randint(0, len(used_slots)))
            } if used_slots else set()

            worker_count = rng.randint(1, 5)
            worker_ids = rng.sample(range(1, 30), worker_count)
            workers = []
            for worker_id in worker_ids:
                kinds = rng.sample(range(1, 5), rng.randint(1, 4))
                workers.append((worker_id, set(kinds)))

            expected = self.brute_force(
                candidates, occupied, headrooms, workers)
            actual = solve_match(
                candidates, occupied, headrooms, workers)
            self.assertEqual(
                actual, expected,
                msg="case=%r candidates=%r occupied=%r caps=%r workers=%r"
                    % (case, candidates, occupied, headrooms, workers))


class FailureAndSnapshotTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "snapshot.db"
        create_dispatch_db(self.path)
        self.pool = FailurePool(
            self.path, capacity=3, snapshot_groups=True)
        self.addCleanup(self.pool.close)
        self.index = ReservationIndex(self.pool)
        self.index.initialize()
        initialize_dispatch(self.pool)
        finish_backfill(self.index, (1, 2, 3))

    def test_second_attempt_failure_rolls_back_and_snapshot_visibility(self):
        workers = [(101, 1001, [1, 2]), (102, 1002, [1])]
        before = dump_database(self.path)

        self.pool.fail_on_attempt_number = 2
        with self.assertRaisesRegex(
                sqlite3.OperationalError, "injected second attempt"):
            dispatch_wave(self.pool, workers)

        after_failure = dump_database(self.path)
        self.assertEqual(after_failure, before)

        # 移除故障后，在一个仍打开的旧读组期间成功提交。
        self.pool.fail_on_attempt_number = None
        self.pool.attempt_inserts = 0
        with self.pool.borrow_snapshot_group(2, timeout=1) as readers:
            old_ready = readers[0].execute(
                "SELECT COUNT(*) FROM dispatch_ready").fetchone()[0]
            old_attempts = readers[1].execute(
                "SELECT COUNT(*) FROM job_attempts").fetchone()[0]
            self.assertEqual((old_ready, old_attempts), (3, 0))

            result = dispatch_wave(self.pool, workers)
            self.assertEqual(
                result, ("committed", [(11, 102), (21, 101)]))

            # 已交付读组继续钉住旧快照，读不到实际提交。
            self.assertEqual(
                readers[0].execute(
                    "SELECT COUNT(*) FROM dispatch_ready").fetchone()[0],
                3)
            self.assertEqual(
                readers[1].execute(
                    "SELECT COUNT(*) FROM job_attempts").fetchone()[0],
                0)

        # 退出后新开的读组取景于提交之后。
        with self.pool.borrow_snapshot_group(2, timeout=1) as readers:
            self.assertEqual(
                readers[0].execute(
                    "SELECT COUNT(*) FROM dispatch_ready").fetchone()[0],
                1)
            self.assertEqual(
                readers[1].execute(
                    "SELECT COUNT(*) FROM job_attempts").fetchone()[0],
                2)

        # 故障后池仍可继续服务，配额索引也已在成功批次中更新。
        self.assertEqual(self.index.status(1)[1], 2)
        self.assertEqual(self.index.status(2)[1], 2)


class InitializeDispatchTest(unittest.TestCase):
    def test_idempotent_and_keeps_existing_candidates(self):
        with TemporaryDirectory() as temp:
            path = Path(temp) / "init.db"
            create_dispatch_db(path)
            with ConnectionPool(path, capacity=1) as pool:
                initialize_dispatch(pool)
                initialize_dispatch(pool)
                with pool.borrow() as db:
                    count = db.execute(
                        "SELECT COUNT(*) FROM dispatch_ready").fetchone()[0]
                self.assertEqual(count, 3)

    def test_can_create_missing_dispatch_table(self):
        with TemporaryDirectory() as temp:
            path = Path(temp) / "missing.db"
            db = sqlite3.connect(path)
            db.execute("PRAGMA journal_mode=WAL")
            db.execute(
                "CREATE TABLE quota_tenants "
                "(tenant_id INTEGER PRIMARY KEY, max_units INTEGER NOT NULL)")
            db.execute(
                "CREATE TABLE quota_holds ("
                "tenant_id INTEGER NOT NULL, slot INTEGER NOT NULL, "
                "units INTEGER NOT NULL, PRIMARY KEY(tenant_id, slot))")
            db.execute(
                "CREATE TABLE job_attempts ("
                "job_id INTEGER PRIMARY KEY, epoch INTEGER NOT NULL, "
                "fence INTEGER NOT NULL, state TEXT NOT NULL, "
                "outcome TEXT, receipt BLOB)")
            db.commit()
            db.close()

            with ConnectionPool(path, capacity=1) as pool:
                initialize_dispatch(pool)
                initialize_dispatch(pool)
                with pool.borrow() as db:
                    db.execute(
                        "INSERT INTO dispatch_ready VALUES (1,1,1,1)")
                    db.commit()


class RegressionSuiteTest(unittest.TestCase):
    def test_original_suites_and_demo_keep_running(self):
        suite = subprocess.run(
            [sys.executable, "-m", "unittest",
             "test_pool", "test_quota", "test_reconcile"],
            cwd=PROJECT_DIR, capture_output=True, text=True, timeout=240)
        self.assertEqual(
            suite.returncode, 0, msg=suite.stdout + suite.stderr)

        demo = subprocess.run(
            [sys.executable, "demo.py"],
            cwd=PROJECT_DIR, capture_output=True, text=True, timeout=60)
        self.assertEqual(demo.returncode, 0, msg=demo.stderr)
        self.assertIn("capacity: busy", demo.stdout)
        self.assertIn("reused:", demo.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
