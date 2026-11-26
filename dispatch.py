"""批次派发的最大匹配与单事务提交组件。

一轮最多收到 16 个执行器名额；每个名额只能接一个作业，每个候选作业也最多
派发一次。候选还受到三类约束：

* 作业种类必须在该执行器支持种类内；
* 候选指定的 ``(tenant_id, slot)`` 不能已有 ``quota_holds`` 占用行；
* 租户必须已完成回填，且本批在该租户上的启动数不能超过
  ``max(0, max_units - used_units)``。

组件先在一个很短的一致性读事务里取全决策输入并立即归还池连接，随后在池外
求“最大启动数 + 字典序最小 ``(job_id, worker_id)`` 序列”的匹配。若调用方
提供 ``before_commit``，此时才在不持有任何借用/事务的情况下调用一次。

提交时重新借用连接并执行 ``BEGIN IMMEDIATE``，比较当前决策输入是否仍与读
快照完全一致。不一致就返回 ``("stale", [])``，不重算、不写业务数据；一致
才在同一事务中删除候选、写 ``quota_holds``、递增 ``quota_index.used_units``
并插入 ``job_attempts``。任何 SQLite 错误都回滚整批并原样抛出。

字典序目标用一次性的最小费用流求解：把按 ``(job_id, worker_id)`` 排序的可
派边记为第 ``r`` 条，赋予层级权重 ``2**(E-1-r)``。较早边的权重大于所有更
晚边权重之和，因此在最大流（最大启动数）中最小化负权重费用，恰好得到规定
的字典序最小分配。
"""

from collections import deque


__all__ = ["initialize_dispatch", "dispatch_wave"]

ID_MIN = 1
ID_MAX = 2147483647
MAX_WORKERS = 16
MIN_KIND = 1
MAX_KIND = 32

_DISPATCH_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dispatch_ready (
    job_id INTEGER PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    slot INTEGER NOT NULL,
    kind INTEGER NOT NULL,
    UNIQUE (tenant_id, slot)
)
"""


class _Worker:
    __slots__ = ("worker_id", "epoch", "kinds")

    def __init__(self, worker_id, epoch, kinds):
        self.worker_id = worker_id
        self.epoch = epoch
        self.kinds = frozenset(kinds)


class _Edge:
    __slots__ = ("to", "rev", "cap", "cost")

    def __init__(self, to, rev, cap, cost):
        self.to = to
        self.rev = rev
        self.cap = cap
        self.cost = cost


def initialize_dispatch(pool):
    """幂等准备批次派发所需的数据库对象。

    调用方通常已经按契约建好 ``dispatch_ready``；这里的
    ``CREATE TABLE IF NOT EXISTS`` 使其可重复调用且兼容只先建好旧表的调用
    方式。本函数不清空候选，也不修改任何既有业务行。
    """
    with pool.borrow() as db:
        db.execute("BEGIN IMMEDIATE")
        try:
            db.execute(_DISPATCH_TABLE_SQL)
            db.commit()
        except BaseException:
            db.rollback()
            raise


def _strict_int(value, name):
    # ``bool`` 是 ``int`` 的子类，但接口契约明确禁止布尔值冒充整数。
    if type(value) is not int:
        raise ValueError("%s must be a non-boolean integer" % name)
    return value


def _parse_workers(workers):
    if isinstance(workers, (str, bytes, bytearray)):
        raise ValueError("workers must be a sequence of slot tuples")
    try:
        raw_workers = list(workers)
    except TypeError as exc:
        raise ValueError("workers must be iterable") from exc
    if not 1 <= len(raw_workers) <= MAX_WORKERS:
        raise ValueError("workers must contain 1..16 slots")

    parsed = []
    seen_workers = set()
    for index, item in enumerate(raw_workers):
        where = "workers[%d]" % index
        if not isinstance(item, (tuple, list)) or len(item) != 3:
            raise ValueError(
                "%s must be (worker_id, epoch, kinds)" % where)
        worker_id, epoch, kinds = item

        _strict_int(worker_id, where + ".worker_id")
        if not ID_MIN <= worker_id <= ID_MAX:
            raise ValueError(where + ".worker_id is out of range")
        if worker_id in seen_workers:
            raise ValueError("duplicate worker_id: %r" % (worker_id,))
        seen_workers.add(worker_id)

        _strict_int(epoch, where + ".epoch")
        if not ID_MIN <= epoch <= ID_MAX:
            raise ValueError(where + ".epoch is out of range")

        if isinstance(kinds, (str, bytes, bytearray)):
            raise ValueError(where + ".kinds must be a collection of integers")
        try:
            kind_iter = iter(kinds)
        except TypeError as exc:
            raise ValueError(where + ".kinds must be iterable") from exc

        kind_values = []
        seen_kinds = set()
        for kind in kind_iter:
            _strict_int(kind, where + ".kind")
            if not MIN_KIND <= kind <= MAX_KIND:
                raise ValueError(where + ".kind is out of range")
            if kind in seen_kinds:
                raise ValueError("duplicate kind in %s" % where)
            seen_kinds.add(kind)
            kind_values.append(kind)

        if not 1 <= len(kind_values) <= MAX_KIND:
            raise ValueError(where + ".kinds must contain 1..32 kinds")

        parsed.append(_Worker(worker_id, epoch, kind_values))

    return sorted(parsed, key=lambda worker: worker.worker_id)


def _add_edge(graph, source, target, capacity, cost):
    forward_rev = len(graph[target])
    backward_rev = len(graph[source])
    graph[source].append(_Edge(target, forward_rev, capacity, cost))
    graph[target].append(_Edge(source, backward_rev, 0, -cost))
    return graph[source][-1]


def _min_cost_max_flow(graph, source, sink):
    """Send all possible integral flow and return its total cost."""
    node_count = len(graph)
    total_flow = 0

    # 图很小（<= 16 个 worker、512 个 job、64 个 tenant），增广次数也 <=16。
    # 初始图是 DAG，没有负环；每次最短增广后仍保持当前流量下的最小费用性质。
    while True:
        distance = [None] * node_count
        previous_node = [-1] * node_count
        previous_edge = [-1] * node_count
        in_queue = [False] * node_count
        queue = deque([source])
        distance[source] = 0
        in_queue[source] = True

        while queue:
            node = queue.popleft()
            in_queue[node] = False
            current_distance = distance[node]
            for edge_index, edge in enumerate(graph[node]):
                if edge.cap <= 0:
                    continue
                candidate_distance = current_distance + edge.cost
                old_distance = distance[edge.to]
                if (old_distance is None
                        or candidate_distance < old_distance):
                    distance[edge.to] = candidate_distance
                    previous_node[edge.to] = node
                    previous_edge[edge.to] = edge_index
                    if not in_queue[edge.to]:
                        in_queue[edge.to] = True
                        queue.append(edge.to)

        if distance[sink] is None:
            break

        add = MAX_WORKERS + 1
        node = sink
        while node != source:
            parent = previous_node[node]
            edge = graph[parent][previous_edge[node]]
            if edge.cap < add:
                add = edge.cap
            node = parent

        node = sink
        while node != source:
            parent = previous_node[node]
            edge_index = previous_edge[node]
            edge = graph[parent][edge_index]
            edge.cap -= add
            graph[node][edge.rev].cap += add
            node = parent

        total_flow += add

    return total_flow


def solve_match(candidates, occupied_slots, headrooms, workers):
    """纯内存求解最大且字典序最小的匹配。

    参数
    ----
    candidates:
        ``(job_id, tenant_id, slot, kind)`` 可迭代对象。
    occupied_slots:
        已被其他工作占用的 ``(tenant_id, slot)`` 集合。
    headrooms:
        已完成回填租户的当前精确余量；非正数租户不允许本批启动。
    workers:
        已规范化或简单的 ``(worker_id, epoch, kinds)`` /
        ``(worker_id, kinds)`` 序列。测试可直接使用简单形式。

    返回按 job_id 递增的 ``[(job_id, worker_id), ...]``。
    """
    normalized_workers = []
    for worker in workers:
        if isinstance(worker, _Worker):
            normalized_workers.append(worker)
        elif len(worker) == 3:
            worker_id, epoch, kinds = worker
            normalized_workers.append(
                _Worker(worker_id, epoch, tuple(kinds)))
        else:
            worker_id, kinds = worker
            normalized_workers.append(
                _Worker(worker_id, None, tuple(kinds)))
    normalized_workers.sort(key=lambda worker: worker.worker_id)

    available = []
    for job_id, tenant_id, slot, kind in candidates:
        if (tenant_id, slot) in occupied_slots:
            continue
        capacity = headrooms.get(tenant_id, 0)
        if capacity <= 0:
            continue
        compatible = [
            worker for worker in normalized_workers
            if kind in worker.kinds
        ]
        if compatible:
            available.append(
                (job_id, tenant_id, slot, kind, compatible))
    available.sort(key=lambda item: item[0])

    # 所有实际可能出现在答案中的 job-worker 边，按 (job_id, worker_id)
    # 字典序排列。
    ranked_edges = []
    for job_id, tenant_id, slot, kind, compatible in available:
        for worker in sorted(compatible, key=lambda item: item.worker_id):
            ranked_edges.append((job_id, worker.worker_id, tenant_id))

    edge_count = len(ranked_edges)
    if edge_count == 0:
        return []

    tenant_ids = sorted({tenant_id for _, _, tenant_id in ranked_edges})
    worker_ids = [worker.worker_id for worker in normalized_workers]

    next_node = 0
    source = next_node
    next_node += 1
    tenant_nodes = {}
    for tenant_id in tenant_ids:
        tenant_nodes[tenant_id] = next_node
        next_node += 1
    job_nodes = {}
    for job_id, *_ in available:
        job_nodes[job_id] = next_node
        next_node += 1
    worker_nodes = {}
    for worker_id in worker_ids:
        worker_nodes[worker_id] = next_node
        next_node += 1
    sink = next_node
    node_count = next_node + 1
    graph = [[] for _ in range(node_count)]

    for tenant_id in tenant_ids:
        # 本批总流量至多 16；把租户容量封顶到 worker 数即可。
        capacity = min(int(headrooms[tenant_id]), len(normalized_workers))
        _add_edge(graph, source, tenant_nodes[tenant_id], capacity, 0)

    # tenant -> job cap 1 保证每个作业最多一次。
    for job_id, tenant_id, slot, kind, compatible in available:
        _add_edge(
            graph,
            tenant_nodes[tenant_id],
            job_nodes[job_id],
            1,
            0)

    # job -> worker cap 1；层级负费用编码字典序。
    assignment_references = []
    for rank, (job_id, worker_id, tenant_id) in enumerate(ranked_edges):
        weight = 1 << (edge_count - 1 - rank)
        edge = _add_edge(
            graph,
            job_nodes[job_id],
            worker_nodes[worker_id],
            1,
            -weight)
        assignment_references.append((job_id, worker_id, edge))

    for worker_id in worker_ids:
        _add_edge(graph, worker_nodes[worker_id], sink, 1, 0)

    _min_cost_max_flow(graph, source, sink)

    selected = [
        (job_id, worker_id)
        for job_id, worker_id, edge in assignment_references
        if edge.cap == 0
    ]
    selected.sort()
    return selected


def _kinds_marker(kinds):
    return ",".join("?" for _ in kinds)


def _load_snapshot(db, kinds):
    """在已建立的读事务内读取完整决策视图。"""
    marker = _kinds_marker(kinds)
    candidates = tuple(
        tuple(row)
        for row in db.execute(
            "SELECT job_id, tenant_id, slot, kind "
            "FROM dispatch_ready "
            "WHERE kind IN (%s) "
            "ORDER BY job_id" % marker,
            tuple(kinds)).fetchall()
    )

    # LEFT JOIN 只按候选的主键号位做点查；这里不聚合 quota_holds 总量。
    occupancy = tuple(
        tuple(row)
        for row in db.execute(
            "SELECT d.job_id, h.units "
            "FROM dispatch_ready AS d "
            "LEFT JOIN quota_holds AS h "
            "  ON h.tenant_id = d.tenant_id AND h.slot = d.slot "
            "WHERE d.kind IN (%s) "
            "ORDER BY d.job_id" % marker,
            tuple(kinds)).fetchall()
    )

    tenant_ids = sorted({row[1] for row in candidates})
    if tenant_ids:
        tenant_marker = ",".join("?" for _ in tenant_ids)
        tenants = tuple(
            tuple(row)
            for row in db.execute(
                "SELECT t.tenant_id, t.max_units, "
                "i.covered_slots, i.used_units, i.ready "
                "FROM quota_tenants AS t "
                "JOIN quota_index AS i ON i.tenant_id = t.tenant_id "
                "WHERE t.tenant_id IN (%s) "
                "ORDER BY t.tenant_id" % tenant_marker,
                tuple(tenant_ids)).fetchall()
        )
    else:
        tenants = ()

    return candidates, occupancy, tenants


def _decision_from_snapshot(snapshot, workers):
    candidates, occupancy, tenant_rows = snapshot

    # 现有配额入口在 units=0 时会删除行；因此“号位被占用”按正单位持有判
    # 定。正常契约数据中留存行均为正 units，这里也兼容遗留的 0 行。
    candidate_by_job = {row[0]: row for row in candidates}
    occupied_slots = set()
    for job_id, units in occupancy:
        if units is not None and units > 0:
            _, tenant_id, slot, _kind = candidate_by_job[job_id]
            occupied_slots.add((tenant_id, slot))

    headrooms = {}
    for tenant_id, max_units, _covered, used_units, ready in tenant_rows:
        if ready:
            headrooms[tenant_id] = max(0, max_units - used_units)

    return solve_match(candidates, occupied_slots, headrooms, workers)


def dispatch_wave(pool, workers, *, before_commit=None):
    """求一轮最大批次匹配并在单个 SQLite 事务中提交。

    返回 ``("committed", [(job_id, worker_id), ...])`` 或
    ``("stale", [])``。参数不合法时抛 ``ValueError``；提交途中的 SQLite
    错误回滚整批后原样传播；``before_commit`` 的异常也原样传播且不提交派发。
    """
    parsed_workers = _parse_workers(workers)
    if before_commit is not None and not callable(before_commit):
        raise ValueError("before_commit must be callable or None")

    supported_kinds = tuple(sorted({
        kind
        for worker in parsed_workers
        for kind in worker.kinds
    }))

    # 借用一：只做一次一致性快照读取。显式 BEGIN 让多条 SELECT 绑定同一
    # WAL 已提交视图；读完立刻 rollback，并由 with 块归还连接。
    with pool.borrow() as db:
        db.execute("BEGIN")
        try:
            snapshot = _load_snapshot(db, supported_kinds)
        finally:
            db.rollback()

    # 慢求解发生在所有借用块之外，不占池连接，也不阻塞其他调度写入。
    assignments = _decision_from_snapshot(snapshot, parsed_workers)

    if before_commit is not None:
        before_commit()

    # 借用二：复核同一决策视图并提交。匹配绝不在此借用内重算。
    with pool.borrow() as db:
        db.execute("BEGIN IMMEDIATE")
        try:
            current_snapshot = _load_snapshot(db, supported_kinds)
            if current_snapshot != snapshot:
                db.rollback()
                return "stale", []

            worker_epochs = {
                worker.worker_id: worker.epoch
                for worker in parsed_workers
            }
            candidate_by_job = {
                row[0]: row for row in snapshot[0]
            }
            old_units_by_job = {
                job_id: units for job_id, units in snapshot[1]
            }

            for job_id, worker_id in assignments:
                _job_id, tenant_id, slot, _kind = candidate_by_job[job_id]
                epoch = worker_epochs[worker_id]

                db.execute(
                    "DELETE FROM dispatch_ready WHERE job_id = ?",
                    (job_id,))
                old_units = old_units_by_job[job_id]
                if old_units is None:
                    db.execute(
                        "INSERT INTO quota_holds "
                        "(tenant_id, slot, units) VALUES (?, ?, 1)",
                        (tenant_id, slot))
                else:
                    # 仅兼容遗留的 units=0 行；正常未占用号位没有留存行。
                    db.execute(
                        "UPDATE quota_holds SET units = units + 1 "
                        "WHERE tenant_id = ? AND slot = ?",
                        (tenant_id, slot))
                db.execute(
                    "UPDATE quota_index "
                    "SET used_units = used_units + 1 "
                    "WHERE tenant_id = ?",
                    (tenant_id,))
                db.execute(
                    "INSERT INTO job_attempts "
                    "(job_id, epoch, fence, state, outcome, receipt) "
                    "VALUES (?, ?, 1, 'dispatched', NULL, NULL)",
                    (job_id, epoch))

            db.commit()
            return "committed", list(assignments)
        except BaseException:
            db.rollback()
            raise
