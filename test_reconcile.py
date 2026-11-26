"""退役执行器对账组件的标准库测试。

全部使用真实临时 WAL SQLite 数据库与现有的 ``ConnectionPool``，不 mock
SQLite。并发场景用可控迭代点/注入钩子做确定性同步，不靠休眠碰撞。

* ``ReconcileSemanticsTest``：片段只补登、封存才置重试、身份精确匹配、
  跨世代隔离、重放零改动，以及各类整次拒绝（含序号 1、1、3 冒充完整
  范围、同序号异内容、身份复用、读取中断、超限与参数校验）。
* ``OnlineSchedulingTest``：流消费途中在线换尝试/结束尝试、SQLite 批量
  写失败整批回滚且池仍可用、双读快照组与对账写的视图关系，并回归原有
  测试套件与 demo。
* ``FullScaleMemoryTest``：在 96 MiB 虚拟地址空间上限的独立子进程里，
  单次迭代生成 120000 条乱序满规模回执（含末尾精确重送），绝大多数身份
  已归档；正常与错误路径都不得残留临时资源。
"""

from pathlib import Path
import os
import sqlite3
import struct
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

from pool import ConnectionPool
from reconcile import (
    ID_MAX,
    PAYLOAD_MAX,
    ROW_LIMIT,
    SEQ_MAX,
    reconcile_epoch,
)

PROJECT_DIR = Path(__file__).resolve().parent


def make_scheduler_db(path, rows, *, epochs=(1, 2)):
    """建 WAL 调度库并预填 ``job_attempts``。"""
    db = sqlite3.connect(path)
    try:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute(
            "CREATE TABLE job_attempts ("
            "job_id INTEGER PRIMARY KEY, epoch INTEGER NOT NULL, "
            "fence INTEGER NOT NULL, state TEXT NOT NULL, "
            "outcome TEXT, receipt BLOB)")
        db.executemany(
            "INSERT INTO job_attempts "
            "(job_id, epoch, fence, state, outcome, receipt) "
            "VALUES (?, ?, ?, ?, ?, ?)", list(rows))
        db.commit()
    finally:
        db.close()


def fetch_attempt(path, job_id):
    db = sqlite3.connect(path)
    try:
        return db.execute(
            "SELECT epoch, fence, state, outcome, receipt "
            "FROM job_attempts WHERE job_id = ?", (job_id,)).fetchone()
    finally:
        db.close()


def scratch_files(scratch_dir):
    return [name for name in os.listdir(scratch_dir)
            if name.startswith("reconcile-")]


class HookPool(ConnectionPool):
    """带确定性钩子的池：可在指定 SQL 上阻塞或令其失败。"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
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
                return super().execute(sql, parameters)

        return sqlite3.connect(
            self._path, check_same_thread=False, factory=HookConnection)


class ReconcileSemanticsTest(unittest.TestCase):
    def setUp(self):
        root = TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.root = Path(root.name)
        self.path = self.root / "scheduler.db"
        self.scratch = self.root / "scratch"
        self.scratch.mkdir()
        # 世代 7：job1/2/4/5 未知，job3 已正常完成；世代 8 另有一行未知，
        # 用于验证世代过滤。
        self.seed = [
            (1, 7, 10, "uncertain", None, None),
            (2, 7, 20, "uncertain", None, None),
            (3, 7, 30, "finished", "ok", b"x" * 3),
            (4, 7, 40, "uncertain", None, None),
            (5, 7, 50, "uncertain", None, None),
            (6, 8, 10, "uncertain", None, None),
        ]
        make_scheduler_db(self.path, self.seed)
        self.pool = ConnectionPool(self.path, capacity=2)
        self.addCleanup(self.pool.close)

    def test_fragment_then_sealed_then_replay(self):
        fragment = [
            (1, 1, 10, "ok", b"a" * 4),
            (2, 3, 30, "failed", b"b" * 5),   # 命中已完成行：不改
            (3, 5, 49, "ok", b"c" * 6),       # 旧令牌：当前 fence=50，未命中
            (1, 1, 10, "ok", b"a" * 4),       # 精确重送
        ]
        # 片段：只补登命中的未知尝试，缺失绝不触发重试。
        self.assertEqual(
            reconcile_epoch(
                self.pool, 7, iter(fragment),
                sealed_count=None, scratch_dir=self.scratch),
            (1, 0))
        self.assertEqual(
            fetch_attempt(self.path, 1),
            (7, 10, "finished", "ok", b"a" * 4))
        self.assertEqual(fetch_attempt(self.path, 2)[2], "uncertain")
        self.assertEqual(
            fetch_attempt(self.path, 3),
            (7, 30, "finished", "ok", b"x" * 3))
        self.assertEqual(fetch_attempt(self.path, 4)[2], "uncertain")
        self.assertEqual(fetch_attempt(self.path, 5)[2], "uncertain")
        self.assertEqual(fetch_attempt(self.path, 6)[2], "uncertain")
        self.assertEqual(scratch_files(self.scratch), [])

        # 同样的记录放进已证明完整的封存日志（序号 1..4），未命中的旧
        # 尝试这才交回重试调度。
        sealed = [
            (4, 2, 20, "failed", b"d" * 7),
            (1, 1, 10, "ok", b"a" * 4),
            (3, 5, 49, "ok", b"c" * 6),
            (2, 3, 30, "failed", b"b" * 5),
        ]
        finished, retryable = reconcile_epoch(
            self.pool, 7, iter(sealed),
            sealed_count=4, scratch_dir=self.scratch)
        self.assertEqual((finished, retryable), (1, 2))
        self.assertEqual(
            fetch_attempt(self.path, 2),
            (7, 20, "finished", "failed", b"d" * 7))
        # 旧令牌回执不能完成新尝试；封存完整且身份是对账前的旧身份 → 重试。
        row5 = fetch_attempt(self.path, 5)
        self.assertEqual(row5[:3], (7, 50, "retryable"))
        self.assertIsNone(row5[3])
        self.assertIsNone(row5[4])
        # 完全没有回执的未知尝试同样置重试，两列保持 NULL。
        row4 = fetch_attempt(self.path, 4)
        self.assertEqual(row4[:3], (7, 40, "retryable"))
        self.assertIsNone(row4[4])
        # 其他世代不动。
        self.assertEqual(fetch_attempt(self.path, 6)[2], "uncertain")
        self.assertEqual(scratch_files(self.scratch), [])

        # 重放：没有仍为 uncertain 的行，零改动、零异常。
        again = reconcile_epoch(
            self.pool, 7, iter(sealed),
            sealed_count=4, scratch_dir=self.scratch)
        self.assertEqual(again, (0, 0))
        self.assertEqual(
            fetch_attempt(self.path, 3),
            (7, 30, "finished", "ok", b"x" * 3))
        self.assertEqual(scratch_files(self.scratch), [])

    def _fresh_database(self):
        path = self.root / ("fresh-%d.db" % len(os.listdir(self.root)))
        make_scheduler_db(path, self.seed)
        return path

    def _assert_rejected_untouched(self, receipts, **kwargs):
        path = self._fresh_database()
        pool = ConnectionPool(path, capacity=2)
        self.addCleanup(pool.close)
        with self.assertRaises(ValueError):
            reconcile_epoch(
                pool, 7, receipts, scratch_dir=self.scratch, **kwargs)
        db = sqlite3.connect(path)
        try:
            self.assertEqual(
                db.execute(
                    "SELECT job_id, epoch, fence, state, outcome, receipt "
                    "FROM job_attempts ORDER BY job_id").fetchall(),
                self.seed)
        finally:
            db.close()
        self.assertEqual(scratch_files(self.scratch), [])

    def test_seqs_one_one_three_cannot_claim_full_coverage(self):
        # 恰好三行但序号是 1、1、3：distinct 只有 {1,3}，封存声明失败。
        rows = [
            (1, 1, 10, "ok", b"p"),
            (1, 1, 10, "ok", b"p"),
            (3, 2, 20, "ok", b"q"),
        ]
        self._assert_rejected_untouched(iter(rows), sealed_count=3)

    def test_late_redelivery_with_different_content_is_rejected(self):
        rows = [
            (1, 1, 10, "ok", b"p"),
            (2, 2, 20, "ok", b"q"),
            (1, 1, 10, "failed", b"p"),  # 同序号，其余四项不再精确相同
        ]
        self._assert_rejected_untouched(iter(rows), sealed_count=None)
        self._assert_rejected_untouched(iter(rows), sealed_count=2)

    def test_identity_reuse_across_seqs_is_rejected_even_when_archived(self):
        # 库中根本没有 job 999（已归档身份），日志自身的冲突仍必须拦截。
        rows = [
            (1, 999, 777, "ok", b"p"),
            (2, 999, 777, "failed", b"q"),
        ]
        self._assert_rejected_untouched(iter(rows), sealed_count=None)

    def test_iterator_error_propagates_and_leaves_nothing(self):
        class Boom(Exception):
            pass

        def broken_stream():
            yield (1, 1, 10, "ok", b"p")
            yield (2, 2, 20, "ok", b"q")
            raise Boom("stream broke")

        path = self._fresh_database()
        pool = ConnectionPool(path, capacity=2)
        self.addCleanup(pool.close)
        with self.assertRaises(Boom):
            reconcile_epoch(
                pool, 7, broken_stream(),
                sealed_count=None, scratch_dir=self.scratch)
        db = sqlite3.connect(path)
        try:
            self.assertEqual(
                db.execute(
                    "SELECT job_id, epoch, fence, state, outcome, receipt "
                    "FROM job_attempts ORDER BY job_id").fetchall(),
                self.seed)
        finally:
            db.close()
        self.assertEqual(scratch_files(self.scratch), [])

    def test_sealed_hole_extra_and_count_mismatch_reject_whole_batch(self):
        # 洞：声明 4 个序号，缺 2。
        self._assert_rejected_untouched(
            iter([(1, 1, 10, "ok", b"p"), (3, 3, 30, "ok", b"q"),
                  (4, 4, 40, "ok", b"r")]),
            sealed_count=4)
        # 多余：声明 2 个序号却来了 3 个不同序号。
        self._assert_rejected_untouched(
            iter([(1, 1, 10, "ok", b"p"), (2, 2, 20, "ok", b"q"),
                  (3, 3, 30, "ok", b"r")]),
            sealed_count=2)
        # 越界序号本身在摄入期即拒绝。
        self._assert_rejected_untouched(
            iter([(SEQ_MAX + 1, 1, 10, "ok", b"p")]),
            sealed_count=None)

    def test_row_limit_is_enforced_including_redeliveries(self):
        def too_many():
            yield (1, 1, 10, "ok", b"p")
            for _ in range(ROW_LIMIT):  # 连同首行共 160001 行
                yield (1, 1, 10, "ok", b"p")

        self._assert_rejected_untouched(
            too_many(), sealed_count=None)

    def test_parameter_and_row_validation(self):
        good = (1, 1, 10, "ok", b"p")

        def expect_value_error(fn):
            with self.assertRaises(ValueError):
                fn()

        # 顶层参数：布尔冒充整数、越界、错误类型、坏 scratch 目录。
        expect_value_error(lambda: reconcile_epoch(
            self.pool, True, [good], sealed_count=None,
            scratch_dir=self.scratch))
        expect_value_error(lambda: reconcile_epoch(
            self.pool, 0, [good], sealed_count=None,
            scratch_dir=self.scratch))
        expect_value_error(lambda: reconcile_epoch(
            self.pool, ID_MAX + 1, [good], sealed_count=None,
            scratch_dir=self.scratch))
        expect_value_error(lambda: reconcile_epoch(
            self.pool, 7, [good], sealed_count=True,
            scratch_dir=self.scratch))
        expect_value_error(lambda: reconcile_epoch(
            self.pool, 7, [good], sealed_count=-1,
            scratch_dir=self.scratch))
        expect_value_error(lambda: reconcile_epoch(
            self.pool, 7, [good], sealed_count=SEQ_MAX + 1,
            scratch_dir=self.scratch))
        expect_value_error(lambda: reconcile_epoch(
            self.pool, 7, [good], sealed_count=None,
            scratch_dir=self.root / "does-not-exist"))
        expect_value_error(lambda: reconcile_epoch(
            self.pool, 7, None, sealed_count=None,
            scratch_dir=self.scratch))

        # 行结构与字段。
        bad_rows = [
            (1, 1, 10, "ok"),                              # 缺字段
            (1, 1, 10, "ok", b"p", "extra"),              # 多字段
            (True, 1, 10, "ok", b"p"),                    # 布尔 seq
            (1, False, 10, "ok", b"p"),                   # 布尔 job_id
            (1, 1, 1.0, "ok", b"p"),                      # 小数 fence
            (0, 1, 10, "ok", b"p"),                       # seq 越界
            (1, ID_MAX + 1, 10, "ok", b"p"),              # job 越界
            (1, 1, 0, "ok", b"p"),                        # fence 越界
            (1, 1, 10, "maybe", b"p"),                    # outcome 非法
            (1, 1, 10, "ok", "p"),                        # payload 非 bytes
            (1, 1, 10, "ok", b""),                        # payload 空
            (1, 1, 10, "ok", b"x" * (PAYLOAD_MAX + 1)),   # payload 超长
        ]
        for bad in bad_rows:
            self._assert_rejected_untouched(
                iter([bad]), sealed_count=None)

    def test_sealed_count_zero_retries_every_uncertain_old_attempt(self):
        # 空流 + sealed_count=0：完整空日志，所有旧未知尝试都可重试。
        result = reconcile_epoch(
            self.pool, 7, iter([]),
            sealed_count=0, scratch_dir=self.scratch)
        self.assertEqual(result, (0, 4))
        for job_id in (1, 2, 4, 5):
            row = fetch_attempt(self.path, job_id)
            self.assertEqual(row[2], "retryable")
            self.assertIsNone(row[3])
            self.assertIsNone(row[4])
        # 已完成行与其他世代不受影响。
        self.assertEqual(fetch_attempt(self.path, 3)[2], "finished")
        self.assertEqual(fetch_attempt(self.path, 6)[2], "uncertain")

    def test_max_payload_is_stored_byte_for_byte(self):
        payload = bytes(range(256)) * 4  # 恰好 1024 字节
        self.assertEqual(len(payload), PAYLOAD_MAX)
        reconcile_epoch(
            self.pool, 7, iter([(1, 1, 10, "failed", payload)]),
            sealed_count=None, scratch_dir=self.scratch)
        self.assertEqual(
            fetch_attempt(self.path, 1),
            (7, 10, "finished", "failed", payload))


class OnlineSchedulingTest(unittest.TestCase):
    def setUp(self):
        root = TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.root = Path(root.name)
        self.scratch = self.root / "scratch"
        self.scratch.mkdir()

    def _open(self, path, *, snapshot=False, capacity=2, hook=False):
        cls = HookPool if hook else ConnectionPool
        return cls(path, capacity=capacity, snapshot_groups=snapshot)

    def test_online_commits_during_stream_are_preserved(self):
        path = self.root / "online.db"
        make_scheduler_db(path, [
            (1, 7, 11, "uncertain", None, None),  # 流途中换 fence
            (2, 7, 22, "uncertain", None, None),  # 流途中正常结束
            (3, 7, 33, "uncertain", None, None),  # 无任何回执，封存 → 重试
            (4, 7, 44, "uncertain", None, None),  # 正常命中补登
        ])
        pool = self._open(path, capacity=1)  # 容量 1：持连接就会堵住在线写
        self.addCleanup(pool.close)

        def replace_attempt():
            # 对账此时不得持有业务连接：容量 1 的池里在线写必须立刻借到。
            with pool.borrow() as db:
                db.execute(
                    "UPDATE job_attempts SET fence = 111 "
                    "WHERE job_id = 1 AND epoch = 7")
                db.commit()
            self.assertFalse(pool._idle[0].in_transaction)

        def finish_attempt():
            with pool.borrow() as db:
                db.execute(
                    "UPDATE job_attempts "
                    "SET state = 'finished', outcome = 'ok', "
                    "receipt = ? WHERE job_id = 2",
                    (b"online",))
                db.commit()

        hooks = {1: replace_attempt, 2: finish_attempt}

        class HookedStream:
            def __init__(self, rows):
                self.rows = rows

            def __iter__(self):
                for row in self.rows:
                    yield row
                    hook = hooks.get(row[0])
                    if hook is not None:
                        hook()

        # 乱序投递，确保钩子在流中途触发。job1 的回执匹配旧 fence=11，
        # 投递后在线把它换成 fence=111；job2 同理，投递后被在线结束。
        # 序号恰好覆盖 1..4，封存完整；job3 没有任何回执。
        stream = HookedStream([
            (1, 1, 11, "ok", b"r1"),
            (2, 2, 22, "ok", b"r2"),
            (4, 4, 44, "failed", b"r4"),
            (3, 999, 999, "ok", b"archived"),  # 已归档身份，仍须校验
        ])
        result = reconcile_epoch(
            pool, 7, stream, sealed_count=4, scratch_dir=self.scratch)
        # 只有 job4 补登；只有 job3（流期间身份未动且无回执）置重试。
        self.assertEqual(result, (1, 1))

        # 换上的新尝试身份不在对账前快照里：保持 uncertain，绝不被误重试。
        self.assertEqual(
            fetch_attempt(path, 1),
            (7, 111, "uncertain", None, None))
        # 在线完成的状态原样保留。
        self.assertEqual(
            fetch_attempt(path, 2),
            (7, 22, "finished", "ok", b"online"))
        self.assertEqual(
            fetch_attempt(path, 3)[:3], (7, 33, "retryable"))
        self.assertEqual(
            fetch_attempt(path, 4),
            (7, 44, "finished", "failed", b"r4"))
        self.assertEqual(scratch_files(self.scratch), [])

    def test_sqlite_failure_mid_batch_rolls_back_everything(self):
        path = self.root / "fail.db"
        make_scheduler_db(path, [
            (1, 7, 11, "uncertain", None, None),  # 先命中补登
            (2, 7, 22, "uncertain", None, None),  # 后置重试时注入失败
        ])
        pool = self._open(path, hook=True)
        self.addCleanup(pool.close)
        pool.raise_on = "SET state = 'retryable'"

        # seq2 绑定一个已归档身份：封存完整，但对当前两行都是未命中路径。
        stream = iter([
            (1, 1, 11, "ok", b"r1"),
            (2, 999, 999, "failed", b"archived"),
        ])
        with self.assertRaises(sqlite3.Error):
            reconcile_epoch(
                pool, 7, stream,
                sealed_count=2, scratch_dir=self.scratch)
        self.assertGreaterEqual(pool.raised, 1)
        # 整批未生效：连先执行的补登也回滚。
        self.assertEqual(fetch_attempt(path, 1)[2], "uncertain")
        self.assertEqual(fetch_attempt(path, 2)[2], "uncertain")
        self.assertEqual(scratch_files(self.scratch), [])

        # 池仍然可用：用完整空日志把两行置重试，验证连接与库都健康。
        pool.raise_on = None
        self.assertEqual(
            reconcile_epoch(
                pool, 7, iter([]),
                sealed_count=0, scratch_dir=self.scratch),
            (0, 2))
        self.assertEqual(fetch_attempt(path, 1)[2], "retryable")
        self.assertEqual(fetch_attempt(path, 2)[2], "retryable")

    def test_snapshot_group_keeps_old_view_until_it_exits(self):
        path = self.root / "snap.db"
        make_scheduler_db(path, [
            (1, 7, 11, "uncertain", None, None),
            (2, 7, 22, "uncertain", None, None),
        ])
        pool = self._open(path, snapshot=True, capacity=3)
        self.addCleanup(pool.close)

        with pool.borrow_snapshot_group(2, timeout=5) as readers:
            for reader in readers:
                self.assertEqual(reader.execute(
                    "SELECT state FROM job_attempts "
                    "WHERE job_id = 1").fetchone()[0], "uncertain")

            # 组存活期间对账写借用第三条连接提交；封存日志只覆盖 job1，
            # job2 没有终态回执 → retryable。
            result = reconcile_epoch(
                pool, 7, iter([(1, 1, 11, "ok", b"r1")]),
                sealed_count=1, scratch_dir=self.scratch)
            self.assertEqual(result, (1, 1))
            self.assertEqual(pool.open_connections, 3)

            # 已交付的读组仍钉在旧视图。
            for reader in readers:
                self.assertEqual(reader.execute(
                    "SELECT state FROM job_attempts "
                    "WHERE job_id = 1").fetchone()[0], "uncertain")

        # 退出后的新组读到已提交的对账结果。
        with pool.borrow_snapshot_group(2, timeout=5) as readers:
            states = dict(
                readers[0].execute(
                    "SELECT job_id, state FROM job_attempts "
                    "WHERE epoch = 7 ORDER BY job_id").fetchall())
        self.assertEqual(states, {1: "finished", 2: "retryable"})
        self.assertEqual(
            fetch_attempt(path, 1),
            (7, 11, "finished", "ok", b"r1"))
        row2 = fetch_attempt(path, 2)
        self.assertEqual(row2[:3], (7, 22, "retryable"))
        self.assertIsNone(row2[4])
        self.assertEqual(scratch_files(self.scratch), [])

    def test_original_suites_and_demo_still_run(self):
        # 项目自有可信代码，按 README 的方式在项目目录下运行（-I 会剥离
        # cwd，反而找不到同目录模块）。
        suite = subprocess.run(
            [sys.executable, "-m", "unittest",
             "test_pool", "test_quota"],
            cwd=PROJECT_DIR, capture_output=True, text=True, timeout=180)
        self.assertEqual(
            suite.returncode, 0,
            msg=suite.stdout + suite.stderr)
        demo = subprocess.run(
            [sys.executable, "demo.py"],
            cwd=PROJECT_DIR, capture_output=True, text=True, timeout=60)
        self.assertEqual(demo.returncode, 0, msg=demo.stderr)
        self.assertIn("capacity: busy", demo.stdout)
        self.assertIn("reused:", demo.stdout)


# ----------------------------------------------------------------------
# 96 MiB 满规模测试：独立子进程，对该进程设置 RLIMIT_AS。
# ----------------------------------------------------------------------

_FULLSCALE_WORKER = r'''
import os
import resource
import struct
import sys

project_dir, mode, db_path, scratch_dir = sys.argv[1:5]
sys.path.insert(1, project_dir)

# 先收紧地址空间，再导入组件：96 MiB 虚拟地址上限。
LIMIT = 96 * 1024 * 1024
resource.setrlimit(resource.RLIMIT_AS, (LIMIT, LIMIT))

from pool import ConnectionPool
from reconcile import reconcile_epoch

EPOCH = 1
N = 120000
STEP = 7919  # 与 120000 互质，乘法遍历生成完整置换，不物化列表

# 少量现存作业的精确身份；其余序号全部对应已归档（库中不存在）的作业。
LIVE = {
    1: (1, 1001),    # 现存 uncertain，命中补登
    2: (2, 1002),    # 现存 uncertain，命中补登
    3: (3, 1003),    # 现存 uncertain，命中补登
    6: (6, 1006),    # 现存已 finished：重放不改
    7: (7, 9007),    # 旧令牌回执：当前 fence=1007，未命中 → retryable
}


def payload(seq):
    return struct.pack(">I", seq) * 256  # 恰好 1024 字节


def stream():
    first = None
    for i in range(N):
        seq = (i * STEP) % N + 1
        if seq in LIVE:
            job_id, fence = LIVE[seq]
        else:
            job_id, fence = seq + 100000, seq
        row = (seq, job_id, fence,
               "ok" if seq % 2 else "failed", payload(seq))
        if seq == 1:
            first = row
        yield row
    # 末尾精确重送一个（现存身份的）回执：应按一次计算。
    yield first


pool = ConnectionPool(db_path, capacity=2)
try:
    if mode == "ok":
        result = reconcile_epoch(
            pool, EPOCH, stream(),
            sealed_count=N, scratch_dir=scratch_dir)
        print("RESULT", result[0], result[1])
    else:
        try:
            reconcile_epoch(
                pool, EPOCH, stream(),
                sealed_count=N - 1, scratch_dir=scratch_dir)
        except ValueError as exc:
            print("REJECTED", type(exc).__name__)
        else:
            raise SystemExit("error path did not reject the broken seal")
finally:
    pool.close()

peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print("PEAK_KB", peak)
if peak >= 96 * 1024:
    raise SystemExit("peak address space exceeded the 96 MiB budget")
'''


class FullScaleMemoryTest(unittest.TestCase):
    def _seed_full_scale_db(self, path):
        make_scheduler_db(path, [
            (1, 1, 1001, "uncertain", None, None),
            (2, 1, 1002, "uncertain", None, None),
            (3, 1, 1003, "uncertain", None, None),
            (4, 1, 1004, "uncertain", None, None),
            (5, 1, 1005, "uncertain", None, None),
            (6, 1, 1006, "finished", "ok", b"kept"),
            (7, 1, 1007, "uncertain", None, None),
            (100, 2, 2002, "uncertain", None, None),  # 其他世代
        ])

    def _run_worker(self, mode, db_path, scratch_dir):
        return subprocess.run(
            [sys.executable, "-I", "-c", _FULLSCALE_WORKER,
             str(PROJECT_DIR), mode, str(db_path), str(scratch_dir)],
            capture_output=True, text=True, timeout=300)

    def test_full_scale_under_96mib_normal_and_error_paths(self):
        if not sys.platform.startswith("linux"):
            self.skipTest("RLIMIT_AS address-space test is Linux-specific")
        # 在独立子进程里探测能否收紧地址空间：直接在本进程 setrlimit 会
        # 压低硬上限且无法恢复。
        probe = subprocess.run(
            [sys.executable, "-c",
             "import resource;"
             "resource.setrlimit(resource.RLIMIT_AS, (%d, %d))"
             % (96 * 1024 * 1024, 96 * 1024 * 1024)])
        if probe.returncode != 0:
            self.skipTest("sandbox forbids lowering RLIMIT_AS")

        with TemporaryDirectory() as root:
            root = Path(root)
            ok_db = root / "ok.db"
            err_db = root / "err.db"
            ok_scratch = root / "scratch-ok"
            err_scratch = root / "scratch-err"
            ok_scratch.mkdir()
            err_scratch.mkdir()
            self._seed_full_scale_db(ok_db)
            self._seed_full_scale_db(err_db)

            ok = self._run_worker("ok", ok_db, ok_scratch)
            self.assertEqual(ok.returncode, 0, msg=ok.stderr)
            self.assertIn("RESULT 3 3", ok.stdout)
            self.assertIn("PEAK_KB", ok.stdout)

            # 少量现存作业按精确身份补登；重放回执不重复计数。
            self.assertEqual(
                fetch_attempt(ok_db, 1),
                (1, 1001, "finished", "ok", struct.pack(">I", 1) * 256))
            self.assertEqual(
                fetch_attempt(ok_db, 2),
                (1, 1002, "finished", "failed",
                 struct.pack(">I", 2) * 256))
            self.assertEqual(
                fetch_attempt(ok_db, 3)[2], "finished")
            # 无回执的旧未知尝试与旧令牌未命中都置重试，两列仍为 NULL。
            for job_id in (4, 5, 7):
                row = fetch_attempt(ok_db, job_id)
                self.assertEqual(row[2], "retryable")
                self.assertIsNone(row[3])
                self.assertIsNone(row[4])
            # 已完成行不被重放改写；其他世代不动。
            self.assertEqual(
                fetch_attempt(ok_db, 6),
                (1, 1006, "finished", "ok", b"kept"))
            self.assertEqual(
                fetch_attempt(ok_db, 100)[2], "uncertain")
            # 正常路径不残留任何临时资源。
            self.assertEqual(os.listdir(ok_scratch), [])

            # 错误路径：封存声明与实际 distinct 数不符，整次拒绝。
            err = self._run_worker("error", err_db, err_scratch)
            self.assertEqual(err.returncode, 0, msg=err.stderr)
            self.assertIn("REJECTED ValueError", err.stdout)
            db = sqlite3.connect(err_db)
            try:
                # 整次拒绝：所有未知行原样保留，没有任何重试/补登。
                self.assertEqual(
                    db.execute(
                        "SELECT count(*) FROM job_attempts "
                        "WHERE state = 'uncertain'").fetchone()[0],
                    7)
                self.assertEqual(
                    db.execute(
                        "SELECT count(*) FROM job_attempts "
                        "WHERE state = 'retryable'").fetchone()[0],
                    0)
                self.assertEqual(
                    db.execute(
                        "SELECT count(*) FROM job_attempts "
                        "WHERE state = 'finished'").fetchone()[0],
                    1)
                self.assertEqual(
                    db.execute(
                        "SELECT receipt FROM job_attempts "
                        "WHERE job_id = 1").fetchone()[0],
                    None)
            finally:
                db.close()
            # 错误路径同样不残留临时资源。
            self.assertEqual(os.listdir(err_scratch), [])


if __name__ == "__main__":
    unittest.main()
