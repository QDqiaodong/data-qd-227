"""退役执行器回执对账组件。

执行器世代退役后，调度库里可能还留着 ``state='uncertain'`` 的尝试：它的终态
回执也许已经写进执行器的顺序回执日志，只是没来得及回传。本组件消费一次回执
流，把"确有回执"的尝试补登为 ``finished``，并且只在日志**完整性可证**时，
才把没有终态回执的旧尝试交回 ``retryable`` 调度。

它修改的是真实调度资格，不是对账报表；不执行作业，也不碰配额与回填规则。

两阶段结构
==========

1. 准备阶段（不占用业务池连接、不写业务库）：

   * 先用一次极短的自动提交读，记下该世代对账开始时库中现存的全部尝试身份
     ``(job_id, fence)``（最多 4096 行，驻留内存）。它只用于界定"旧尝试"的
     范围，读取结束立即归还连接，随后慢流再慢也不会堵住在线调度。
   * 在调用方提供的隔离目录 ``scratch_dir`` 里建一个**组件私有**的临时
     SQLite 库做落盘校验：120000 条、每条 1024 字节的乱序流也不需要进进程
     内存。逐条做类型/越界校验、同序号精确重送判定和
     ``(job_id, fence)`` 身份冲突判定；身份冲突即使对应已归档（库中已不
     存在）的尝试也照样拦截。
   * ``sealed_count`` 非空时，在落盘索引上证明有效结果恰好覆盖序号
     ``1..sealed_count``（允许精确重送，无洞、无越界、无多余序号）。

   任一步失败都在进入写事务前抛 ``ValueError``，业务库一行未改；迭代器
   自身抛出的异常原样传播。

2. 收尾写事务（借用现有 ``ConnectionPool``，单条短事务、整批提交）：

   只枚举该世代、**此刻事务内**仍为 ``uncertain`` 的当前行，因此流校验期间
   正常调度的提交永远不会被启动时的旧状态覆盖：

   * 回执的 ``(job_id, fence)`` 与当前行精确匹配 → ``finished``，写入回执
     的 ``outcome`` 与原始 ``payload`` 字节。
   * 未命中且封存完整性已证明 → 仅当该尝试身份在对账开始时就已存在（属于
     "旧尝试"）才置 ``retryable``，``outcome``/``receipt`` 保持 NULL；流
     处理途中才换上来的新尝试身份原样保留在线提交的新状态。
   * 片段日志（``sealed_count=None``）的未命中行一律保持原状。

   SQLite 操作失败即回滚并传播，不存在部分生效；临时资源在正常或异常
   返回后都不残留。这里的 ``retryable`` 只表示没有这次尝试的终态回执，
   不承诺作业外部副作用只发生一次。
"""

import os
import sqlite3
import tempfile

ID_MIN = 1
ID_MAX = 2147483647
SEQ_MIN = 1
SEQ_MAX = 120000
ROW_LIMIT = 160000
PAYLOAD_MIN = 1
PAYLOAD_MAX = 1024
OUTCOMES = frozenset(("ok", "failed"))
# 每积累这么多条新身份落一次盘，避免单事务过大；与内存占用无关，只影响
# scratch 文件的 fsync 节奏（journal 已关闭，提交本身很轻）。
_COMMIT_EVERY = 2000
_SEALED_SCAN_CHUNK = 1024


def _require_int(value, name):
    # ``bool`` 是 ``int`` 的子类，契约明确不接受布尔值冒充整数。
    if type(value) is not int:
        raise ValueError("%s must be an integer" % name)
    return value


def _validate_epoch(epoch):
    _require_int(epoch, "epoch")
    if not ID_MIN <= epoch <= ID_MAX:
        raise ValueError("epoch out of range")


def _validate_sealed_count(sealed_count):
    """返回是否为封存模式；``None`` 表示仅有片段。"""
    if sealed_count is None:
        return False
    _require_int(sealed_count, "sealed_count")
    if not 0 <= sealed_count <= SEQ_MAX:
        raise ValueError("sealed_count out of range")
    return True


def _validate_scratch_dir(scratch_dir):
    try:
        scratch = os.fspath(scratch_dir)
    except TypeError as exc:
        raise ValueError("scratch_dir must be a directory path") from exc
    if not os.path.isdir(scratch):
        raise ValueError("scratch_dir must be an existing directory")
    return scratch


def _validate_row(row, epoch):
    """校验一行五元组，返回规范化后的五元组。

    ``epoch`` 仅用于错误信息；流中的行按契约均属于入参世代，五元组本身
    不带世代字段。
    """
    if not isinstance(row, (tuple, list)) or len(row) != 5:
        raise ValueError(
            "each receipt must be a five-element tuple "
            "(seq, job_id, fence, outcome, payload)")
    seq, job_id, fence, outcome, payload = row

    _require_int(seq, "seq")
    _require_int(job_id, "job_id")
    _require_int(fence, "fence")
    if not SEQ_MIN <= seq <= SEQ_MAX:
        raise ValueError("seq out of range in epoch %s" % epoch)
    if not ID_MIN <= job_id <= ID_MAX:
        raise ValueError("job_id out of range in epoch %s" % epoch)
    if not ID_MIN <= fence <= ID_MAX:
        raise ValueError("fence out of range in epoch %s" % epoch)

    if type(outcome) is not str or outcome not in OUTCOMES:
        raise ValueError("outcome must be 'ok' or 'failed'")
    if type(payload) is not bytes or not (
            PAYLOAD_MIN <= len(payload) <= PAYLOAD_MAX):
        raise ValueError(
            "payload must be %d..%d raw bytes" % (PAYLOAD_MIN, PAYLOAD_MAX))

    return seq, job_id, fence, outcome, payload


def _open_scratch(scratch_dir, epoch):
    """在隔离目录里创建本组件私有的临时 scratch 库，返回 ``(连接, 路径)``。"""
    fd, path = tempfile.mkstemp(
        prefix="reconcile-epoch%s-" % epoch, suffix=".db", dir=scratch_dir)
    # 只需要唯一文件名；句柄关掉后交给 sqlite3 重新打开。
    os.close(fd)
    sdb = sqlite3.connect(path)
    # 落盘但不走 WAL/回滚日志：数据只服务于本次调用，崩溃后整文件删除，
    # 不需要崩溃恢复能力；小 cache + 临时段落盘，把进程内存压到最低。
    sdb.execute("PRAGMA journal_mode=OFF")
    sdb.execute("PRAGMA synchronous=OFF")
    sdb.execute("PRAGMA cache_size=-256")
    sdb.execute("PRAGMA temp_store=FILE")
    sdb.execute(
        "CREATE TABLE receipts ("
        "seq INTEGER NOT NULL, job_id INTEGER NOT NULL, "
        "fence INTEGER NOT NULL, outcome TEXT NOT NULL, payload BLOB NOT NULL)")
    # 序号唯一：同序号只可能是精确重送。
    sdb.execute("CREATE UNIQUE INDEX idx_receipts_seq ON receipts(seq)")
    # 尝试身份唯一：同一 (job_id, fence) 不得绑定两个序号，已归档身份同样
    # 受此约束。
    sdb.execute(
        "CREATE UNIQUE INDEX idx_receipts_identity "
        "ON receipts(job_id, fence)")
    return sdb, path


def _remove_scratch(path):
    """只清理本次创建的临时文件及其已知旁挂文件。"""
    for suffix in ("", "-wal", "-shm", "-journal"):
        try:
            os.unlink(path + suffix)
        except FileNotFoundError:
            pass


def _ingest(sdb, receipts, epoch):
    """顺序消费流并落盘校验，返回 ``(读入行数, 不同序号数)``。

    迭代器自身的异常不在此捕获，按契约原样传播；非法回执抛 ``ValueError``。
    """
    try:
        iterator = iter(receipts)
    except TypeError as exc:
        raise ValueError("receipts must be iterable") from exc

    total_rows = 0
    distinct = 0
    pending = 0
    for row in iterator:
        if total_rows >= ROW_LIMIT:
            raise ValueError(
                "receipt stream exceeds the %d-row limit" % ROW_LIMIT)
        total_rows += 1
        seq, job_id, fence, outcome, payload = _validate_row(row, epoch)

        previous = sdb.execute(
            "SELECT job_id, fence, outcome, payload FROM receipts "
            "WHERE seq = ?", (seq,)).fetchone()
        if previous is not None:
            # 必须四项逐字节/逐字符精确相同，不能用摘要判定。
            if tuple(previous) != (job_id, fence, outcome, payload):
                raise ValueError(
                    "seq %s was redelivered with different content" % seq)
            # 精确重送：只算一次，不再落盘、不参与身份计数。
            continue

        # 新序号：先查身份冲突。即使该身份在业务库中已归档/删除，日志自身
        # 的一致性仍然必须成立。
        other = sdb.execute(
            "SELECT seq FROM receipts WHERE job_id = ? AND fence = ?",
            (job_id, fence)).fetchone()
        if other is not None:
            raise ValueError(
                "attempt (job_id=%s, fence=%s) is bound to more than one seq"
                % (job_id, fence))

        sdb.execute(
            "INSERT INTO receipts "
            "(seq, job_id, fence, outcome, payload) VALUES (?, ?, ?, ?, ?)",
            (seq, job_id, fence, outcome, payload))
        distinct += 1
        pending += 1
        if pending >= _COMMIT_EVERY:
            sdb.commit()
            pending = 0
    sdb.commit()
    return total_rows, distinct


def _verify_sealed_coverage(sdb, sealed_count, distinct):
    """证明落盘结果恰好是序号 ``1..sealed_count``，精确重送已在摄入时归并。"""
    if distinct != sealed_count:
        raise ValueError(
            "sealed log claims %d receipts but %d distinct seqs were validated"
            % (sealed_count, distinct))
    if sealed_count == 0:
        return
    cursor = sdb.execute("SELECT seq FROM receipts ORDER BY seq")
    expected = 1
    while expected <= sealed_count:
        chunk = cursor.fetchmany(_SEALED_SCAN_CHUNK)
        if not chunk:
            raise ValueError("sealed log has a hole before seq %d" % expected)
        for (actual,) in chunk:
            if actual != expected:
                raise ValueError(
                    "sealed log coverage broken at seq %d (found seq %d)"
                    % (expected, actual))
            expected += 1
    # distinct == sealed_count 且 1..n 全部命中时，不可能还有多余行；这一行
    # 是防御性检查，保证扫描器确实耗尽了索引。
    if cursor.fetchall():
        raise ValueError("sealed log contains seqs beyond sealed_count")


def _apply_reconciliation(pool, epoch, sdb, sealed, pre_existing):
    """在单个短写事务内对账，返回 ``(补登数, 置为可重试数)``。"""
    with pool.borrow() as db:
        db.execute("BEGIN IMMEDIATE")
        try:
            # 枚举的是事务内此刻仍 uncertain 的当前行，而不是流开始时的
            # 快照：期间正常完成、取消或换尝试的行不会被旧状态覆盖。
            current = db.execute(
                "SELECT job_id, fence FROM job_attempts "
                "WHERE epoch = ? AND state = 'uncertain'",
                (epoch,)).fetchall()

            finished = 0
            retryable = 0
            for job_id, fence in current:
                hit = sdb.execute(
                    "SELECT outcome, payload FROM receipts "
                    "WHERE job_id = ? AND fence = ?",
                    (job_id, fence)).fetchone()
                if hit is not None:
                    outcome, payload = hit
                    cursor = db.execute(
                        "UPDATE job_attempts "
                        "SET state = 'finished', outcome = ?, receipt = ? "
                        "WHERE epoch = ? AND job_id = ? AND fence = ? "
                        "AND state = 'uncertain'",
                        (outcome, payload, epoch, job_id, fence))
                    finished += cursor.rowcount
                elif sealed and (job_id, fence) in pre_existing:
                    # 完整性已证且这是对账开始时就存在的旧尝试：没有这次
                    # 尝试的终态回执，交回重试调度。流途中才换上的新身份
                    # 不在 pre_existing 中，原样保留。
                    cursor = db.execute(
                        "UPDATE job_attempts SET state = 'retryable' "
                        "WHERE epoch = ? AND job_id = ? "
                        "AND state = 'uncertain'",
                        (epoch, job_id))
                    retryable += cursor.rowcount
            db.commit()
            return finished, retryable
        except BaseException:
            db.rollback()
            raise


def reconcile_epoch(pool, epoch, receipts, *, sealed_count, scratch_dir):
    """对账一个已退役执行器世代，返回 ``(补登数, 置为可重试数)``。

    参数
    ----
    pool:
        业务库现有的 ``ConnectionPool``。只借用、不关闭，也不另开业务库
        连接。
    epoch:
        退役世代号，1..2147483647 的整数（不接受布尔值）。
    receipts:
        单次迭代的回执流，每行是 ``(seq, job_id, fence, outcome, payload)``
        五元组；本次调用最多消费 160000 行（含重送）。``seq`` 1..120000，
        ``job_id``/``fence`` 1..2147483647，``outcome`` 为 ``"ok"`` 或
        ``"failed"``，``payload`` 为 1..1024 字节的 ``bytes``。同序号重送
        必须四项完全一致；同一 ``(job_id, fence)`` 不得绑定两个序号。
    sealed_count:
        ``None`` 表示只有日志片段：允许补登，缺失不得触发重试。否则必须是
        0..120000 的整数，且有效结果恰好覆盖序号 ``1..sealed_count``，
        否则整次拒绝、不降级、不先补登好行。
    scratch_dir:
        调用方提供的现存隔离目录，用于暂存落盘校验数据；本组件只创建和
        清理自己名下的临时文件。

    异常
    ----
    参数或回执不合法、超上限、身份冲突、封存声明与日志不一致 → ``ValueError``，
    且业务库不留任何改动；迭代器自身的异常原样传播；SQLite 写失败回滚后
    原样传播。
    """
    _validate_epoch(epoch)
    sealed = _validate_sealed_count(sealed_count)
    scratch = _validate_scratch_dir(scratch_dir)

    # 极短的自动提交读：界定"对账开始时已存在的旧尝试身份"，读完即归还。
    # 慢流消费期间不持有任何业务池连接。
    with pool.borrow() as db:
        pre_existing = set(db.execute(
            "SELECT job_id, fence FROM job_attempts WHERE epoch = ?",
            (epoch,)).fetchall())

    sdb = None
    scratch_path = None
    try:
        sdb, scratch_path = _open_scratch(scratch, epoch)
        _ingest(sdb, receipts, epoch)
        if sealed:
            _verify_sealed_coverage(
                sdb, sealed_count,
                sdb.execute("SELECT count(*) FROM receipts").fetchone()[0])
        return _apply_reconciliation(
            pool, epoch, sdb, sealed, pre_existing)
    finally:
        if sdb is not None:
            sdb.close()
        if scratch_path is not None:
            _remove_scratch(scratch_path)
