# SQLite 连接池

服务线程用 `with pool.borrow(timeout=...) as db` 独占租借连接；同时打开的连接
不超过 `capacity`，满额则等归还或抛出 `PoolTimeout`。调用方显式提交；
即使调用方抛异常，归还也会回滚未提交事务。无法回滚的连接不再复用。
关闭池后拒绝新租借，已借连接在归还时关闭。

Python 3.9+，标准库。样例为自制数据、真实临时 SQLite 库：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install --no-index -r requirements.txt
.venv/bin/python -m unittest -v
.venv/bin/python demo.py
```

应输出 `capacity: busy` 和 `reused: [('committed',)]`。

单进程、信任调用方；连接和游标不得逃逸租借块，不能在租借期间跨线程并用。
只清理事务，不重置 PRAGMA、临时表或用户函数；使用者不得留下这类会话变更。
等待时限针对池满，不是 SQLite 锁等待或 SQL 执行的截止时间；不保证公平调度。

## 同批读快照（snapshot groups）

核对作业常把查询分给多个线程。若各线程分别 `borrow`，两次首次查询之间
可能夹进一笔提交：一个线程看到旧明细，另一个看到新余额。启用快照模式后，
`borrow_snapshot_group` 一次交出 `count` 个彼此独立、**绑定同一份已提交主库
视图**的查询连接，作业期间写线程仍可继续提交。

```python
from pool import ConnectionPool

pool = ConnectionPool("ledger.db", capacity=3, snapshot_groups=True)

with pool.borrow_snapshot_group(2, timeout=1) as readers:
    # 每个连接只交给一个工作线程；只读 SQL（含参数化、连接、跨语句重复查询）
    detail = readers[0].execute(
        "SELECT amount FROM ledger WHERE name = ?", ("detail",)).fetchone()
    balance = readers[1].execute(
        "SELECT amount FROM ledger WHERE name = 'balance'").fetchone()

    # 组内仍可写：写借用与其他写借用互斥，但不等待读组退出
    with pool.borrow() as db:
        db.execute("UPDATE ledger SET amount = amount + 5")
        db.commit()
```

### 前提

- 池与数据库在同一进程、同一本地**常规**文件；库须已由调用方置为 WAL。
  构造时只读取 `PRAGMA journal_mode`，非 WAL 或非本地文件以 `ValueError`
  拒绝，且**不改变**其日志模式；池从不自行切换日志模式。
- `snapshot_groups=True` 时 `capacity` 必须为 3 至 16，`count` 必须满足
  `2 <= count < capacity`。验收数据库主库逻辑大小 ≤ 64 MiB。
- 不需要处理池外写入、`ATTACH`、临时表、会话配置变更或作业期间的 DDL。
- 业务写入全部走同一个启用模式的池，仍用 `borrow` + 显式 `commit`。

### 借用与排序规则

- 写借用**互斥**（同一池同时最多一个）。归还时仍回滚未提交事务；回滚
  完成前该写资格仍被视为占用。
- 同一时刻最多一个**已登记**的读组请求（含等待写借用归还、正在建立和已
  可用的组）。已有一个时，第二个以 `PoolTimeout` 立即拒绝，不等待，也不
  影响现有组或写借用。
- 读组一登记，当前已发出的写借用仍可正常提交或回滚；**新的写借用**要等
  整组建立完成或失败后才能获准。
- 取景点严格位于“登记时已发出的那次写借用提交/回滚并归还清理之后”：
  该借用未提交的改动不会进入读组，组内所有连接都绑定到同一份已提交视图。
  即使某个读者在线程里晚到、直到之后又有新事务提交才执行第一条 SQL，
  它仍只看到本组的旧视图。
- 整组交付后，即使所有读连接仍被占用、仍有读取未完成，空出的写资格也能
  立即借出并实际提交。新提交不能被本组看到。
- 组退出时先结束各连接的只读事务再归还；连接随后被复用，下一组的取景点
  不早于当时的已提交状态，因此不会继续读旧版本。
- 离开组前须结束工作、释放游标，不把连接或游标带出组；调用方不在读连接
  上自行管理事务。
- `timeout` 与旧入口口径一致（有限非负数），仅约束**等待池准入**（等写
  借用归还、等连接预算），不是 SQL、连接建立或组建立的执行截止时间。
- 组要么完整交付、要么不交付：部分初始化失败、等待超时或组内抛异常，都
  会回收闸门、借用名额与连接（关闭连接或回滚只读事务后复用），不留临时
  库；SQLite 初始化错误原样抛给调用方，不会以空组表示成功。
- `close()` 拒绝新借用并唤醒等待者（`PoolClosed`）；未交付的组不会随后
  变成成功。已交付的写借用/读组不要求提前退出，归还时不再复用。

### 运行成本

采用**不复制数据库、直接钉住 WAL 读事务**的方案：

- 一组只占 `count` 个普通 SQLite 连接（≤ `capacity`），连接随组复用；
  建组是 O(count) 的 `BEGIN` + 一次首次读（用于固定读快照，`BEGIN` 本身
  不取快照）。全部初始化在池锁之外完成，仅登记和取连接短暂持锁。
- 每个组连接在整个组生命周期内持有一个 WAL 读事务。WAL 只能检查点到最
  老读快照之前；长核对作业期间写并发越高，`-wal` 文件越大，检查点推迟，
  但写提交**不会**被读组阻塞（WAL 读者不阻塞写者）。
- 读组存在时新写借用要等组建立（通常很短）结束；组交付后写者不再受限。
- 写借用在该模式下被串行化（每个池一个写资格），这是快照取景点唯一性的
  代价；读组不额外复制数据，也无每读者私有临时库。
- 组内查询从各自连接读取 WAL 索引，schema 缓存随连接复用而保留。

## WAL 空间上限、回收准入与暂存快照（可选）

长核对组钉住旧 WAL 读视图时，在线派发仍可提交，但旧视图会阻止
`-wal` 被检查点缩短，日志可能持续涨大。`ConnectionPool` 增加两个**可选**
构造参数处理这一风险：

```python
pool = ConnectionPool(
    "dispatch.db",
    capacity=3,
    snapshot_groups=True,      # 启用上限必须同时启用同批读模式
    wal_soft_limit=65536,      # 64 KiB；整数字节，65536..16777216，拒绝 bool
    wal_scratch_dir="/var/lib/dispatch/wal-snap",  # 可选，须已存在且可写
)
```

- 不传 `wal_soft_limit`（默认 `None`）时，原池、原同批读组、配额、对账、
  派发与 demo 的行为完全不变，`reclaim_wal()` 抛 `ValueError`。
- `wal_soft_limit` 必须是 `65536 <= n <= 16777216` 的严格整数（布尔值
  不接受），且必须显式 `snapshot_groups=True`；`wal_scratch_dir` 不能在
  未设上限时给出，且必须是**已存在、实际可创建临时文件**的目录。配置
  非法一律在构造时抛 `ValueError`。

### 写借用准入：什么时候暂缓写入

判断压力只看**主库旁真实的 `<db>-wal` 文件字节数**（`os.path.getsize`，
文件不存在按 0），不是待 checkpoint 的帧数；只把帧写回主库而文件没缩短，
不算腾出空间。

每个新的 `pool.borrow()` 写资格（快照模式下所有 `borrow` 都是互斥写资格）
在即将发放时，于同一套池状态排序内重新取样：

1. 发放时刻 WAL 长度 `< wal_soft_limit`：照常发放，不做额外动作。
2. `>= wal_soft_limit`：池先在该写连接上实际执行
   `PRAGMA wal_checkpoint(TRUNCATE)`（临时把该连接的 `busy_timeout` 调为 0，
   绝不替调用方等待旧读组），再用**新的实测长度**确认；低于上限才发放。
3. 仍 `>= wal_soft_limit`（通常是旧同批读组钉住 WAL，TRUNCATE 返回
   busy）：抛出独立的 `pool.PoolWalPressure`（`RuntimeError` 子类，不是
   `PoolTimeout`），**不交出连接、不等待旧组退出**。

等待写资格的线程在被唤醒、真正取得发放资格后会重新走一遍上述判断，绝不
沿用排队前的测量值。原来的池满、写资格等待、建组闸门与 `timeout` 语义
不变；`PoolWalPressure` 是准入拒绝，不受 `timeout` 剩余时间影响（一旦轮
到资格即立即判定）。

上限只是**发放门槛，不是单笔事务的硬字节上限**：借用发放后，业务连接可以
继续写入、提交、归还，哪怕正是这笔事务让 WAL 第一次越线；已成功的提交不
撤销、不追溯失败（派发回调里已提交的大载荷会保留）。暂缓后不需要人工解
锁：阻碍消失（读组退出）后，下一次 `borrow` 会自行 checkpoint、回收并
继续。调度组件无需改造：`dispatch_wave` 的提交借用若被拒，
`PoolWalPressure` 会原样穿出，候选、占用、配额总量与尝试均不会被该轮
改动；读决策借用与 `before_commit` 中调用方自己的借用遵循同一准入。

### `pool.reclaim_wal()`：值班程序的主动回收入口

已启用上限的池提供：

```python
status, before_bytes, after_bytes = pool.reclaim_wal()
```

它主动尝试把 WAL 缩到 **0 字节**（文件不存在也视为 0）。前后字节数是在
“本次维护决定”的保护（与写资格、建组互斥的同一池状态）下于尝试前后实测
的值。返回状态只有三种：

| status | 含义 | before/after |
| --- | --- | --- |
| `"reclaimed"` | 实测 WAL 已为 0；本来就无 WAL 时也返回它 | 无工作时为 `(0, 0)`；否则为 `(旧长度, 0)` |
| `"lease_busy"` | 已有业务写连接借出（**含提交后仍未退出 `with` 块、以及归还时的 rollback 清理**），或读组尚在建立 | 只取样一次，两列填同一实测值；不尝试 checkpoint、不等待 |
| `"reader_busy"` | 无写借用/建组，但已交付的旧读组（在单进程独占契约下这是唯一阻碍）使 TRUNCATE 无法缩短文件 | 尝试前后的**真实实测值**；不等待读组退出 |

只有实测 `after_bytes == 0` 才报 `"reclaimed"`，绝不伪造回收成功。真实
`sqlite3.Error` 或文件操作 `OSError` **原样传播**，不会被改写成 busy；
失败会结清内部维护闸门与连接计数，池可立即继续使用。池 `close()` 后的
调用抛 `PoolClosed`。

运维上，这个入口适合：

- 值班巡检/定时任务在低峰主动回收 WAL（例如每分钟一次，记录
  `before/after/status` 做容量与阻塞来源监控）；
- 收到 `PoolWalPressure` 告警后，人工或机器人调用它确认是
  `lease_busy`（有长写事务/卡住的归还）还是 `reader_busy`（有长核对组），
  前者查写者，后者推动核对作业结束或为池配置暂存目录；
- 读组退出后立即手动回收一次，加速磁盘空间归还（不调用也没关系，下一
  次写借用会自愈）。

回收只使用池容量内的连接（优先复用空闲连接，否则在 `capacity` 名额内新
开），与建组/维护所用的全部额外连接一样计入 `pool.open_connections`，池
满时不会另开“旁路”连接。池不会直接删除 `-wal`、不会关闭读者、不会改变
主库日志模式，也不会以下线整个池为代价腾空间。

### 暂存目录：用一份快照换取“旧视图 + 可回收 WAL + 继续派发”

默认（不给 `wal_scratch_dir`）时，已交付读组保留主库 WAL 读视图、**不
创建任何副本**：阈值之下不会仅因存在读组就停写；越线且旧组阻碍回收时，
新写入按上面的准入暂缓（`PoolWalPressure`），等组退出后自愈。

提供 `wal_scratch_dir` 表示部署**允许**（但不被强制）用临时空间换取在线
可用性。当前实现会在每次建组时，用 SQLite Online Backup API 从已钉住取
景点的那一条主库连接，把该精确视图复制成暂存目录中**一个全组共享的
`suffix` 唯一快照文件**（默认 rollback-journal 模式，避免再长 WAL/SHM
旁挂），随后组内 `count` 条连接都打开这同一个快照文件。因此：

- 组连接仍是普通的 `sqlite3.Connection`，晚到的第一条查询、跨语句重复查
  询都只能读到取景时的已提交视图（快照文件对池私有且之后不可变）；
- 读组不再钉住主库 WAL，`reclaim_wal()` 即使在组存活时也能真正
  TRUNCATE 到 0，准入不再因该组暂缓，派发可在组内继续提交，而旧组照旧
  读旧值；
- 容量口径不变：1 条取源主连接（用完即归还复用）、备份目标连接与
  `count` 条快照连接都计入 `capacity`，容量 3、count 2 时峰值仍为 3；
- 每组在暂存目录至多一份共享快照（加 `-wal/-shm/-journal` 旁挂），
  **含辅助文件总长度绝不超过 72 MiB**；超过即判失败并删除该快照、抛
  `ValueError`。验收前提是主库逻辑大小 ≤ 64 MiB，一份紧凑快照加旁挂在
  72 MiB 预算内；进程内不保留整库副本；
- 正常退出、组体异常、建组失败都会删除本组快照文件（含已知旁挂后缀）；
  `close()` 不撤销已交付组（快照连接关闭池后仍可查询），在关闭后最后一
  个已交付组归还时同样清干净。池只删除自己用 `mkstemp` 建出的唯一文件及
  其后缀，**调用方预先放在该目录的文件原样保留**。

资源成本对照：

| 方案 | 旧视图成本 | 越线时行为 |
| --- | --- | --- |
| 无暂存目录（默认） | 仅连接 + 被旧快照钉住而无法复用的 WAL 帧；磁盘成本是 WAL 增长，且有软上限兜底，不会无界 | 旧组阻碍 → 新写暂缓，组退出后自愈回收 |
| 有暂存目录 | 每组额外一份 ≤ 主库大小的共享快照磁盘（预算 72 MiB），无进程内全库副本 | 主 WAL 可随时截断，派发继续，旧组读快照 |

仍不处理池外连接、运行中 DDL、`ATTACH` 或调用方自行调整的会话 PRAGMA；
这些前提下单进程、单池独占本地 WAL 主库。

## 租户占用索引与配额写入守卫（`quota.py`）

`ReservationIndex(pool)` 复用同一个 `ConnectionPool`（不另开连接、不接管
关闭），在旧表 `quota_tenants` / `quota_holds` 之上提供可分段上线的占用
总量与准入守卫。库被组件接管后，所有 `quota_holds` 改动都必须走
`set_hold`；不需要兼容池外改表、跨进程或在线改配额。

### 上线步骤

1. 库已由调用方建好并置为 WAL、两张旧表已存在（含历史占用）。
2. 建池后先调用一次 `index.initialize()`：只做 DDL，新增 `quota_index`
   表并为每个租户插入 `(covered_slots=0, used_units=0, ready=0)`。
   **不做全表聚合、不改写旧占用**；重复调用、换池重开都保留已提交的
   回填进度（`INSERT OR IGNORE`）。初始化要在开启同批读组之前完成。
3. 由一个回填工作线程对每个租户反复调用 `advance(tenant_id, slots)`
   （`slots` 为 1 至 512，封顶 4096），把号位 1 起的连续前缀逐段折入
   `used_units`；空号位也计入进度。每步在单个 `BEGIN IMMEDIATE` 事务内
   只扫描本租户本次新增区间（`slot > covered AND slot <= target`），
   不重数已覆盖区、不扫其他租户，提交前连接不外露。
4. `status(tenant_id)` 返回 `(covered_slots, used_units)`；未覆盖全部
   4096 个号位时 `used_units` 为 `None`，部分和不得作为配额依据。
   各租户独立就绪。

### 写入规则（`set_hold`）

热写路径只对目标号位做点查，**绝不重扫租户占用表**；准入总量始终取自
同事务内的 `quota_index`，因此原始占用、回填依据与准入总量共享 SQLite
的同一事务顺序：

- `units=0` 删行（缺行是无操作）；同值、减少、删除在任何阶段都允许，
  即使租户未完成或已超配额。
- 仅净增加触发准入：未完成 → `"not_ready"`；已完成但改后总量超过
  `max_units` → `"limit_exceeded"`；否则提交并返回 `"applied"`。
- 两种拒绝都回滚事务，原始占用与回填状态都不变。SQLite/池自身错误
  原样抛出，不伪装成配额拒绝。
- 已覆盖号位的变动在同事务内增量合并进 `used_units`；未覆盖号位的
  变动只留在原表，等区间扫描时看到——先删后扫不会把旧占用加回来，
  先调低后扫也不会丢掉调低。
- 最后一步回填与业务写相邻时，以数据库提交顺序为准：先提交的回填让
  随后的写按就绪规则用准确总量判定；先提交的业务写仍按未完成规则处理。

### 与同批读快照的配合

先 `initialize()`，再开读组即可，组件不改读组容量与取景规则。读组交付后
持有的是旧快照，`set_hold` 的提交不需要等读组退出（WAL 读者不阻塞写者）；
旧组继续看到旧占用，新组看到新占用。`capacity=3` 的池上，读组占用 2 条
连接，配额写仍可借第 3 条连接提交。

### 参数校验

`tenant_id` 1–64 且必须存在于 `quota_tenants`；`slot` 1–4096；
`set_hold` 的 `units` 0–2147483647；`advance` 的 `slots` 1–512。
布尔值、非整数、越界或未知租户一律 `ValueError`，且在任何数据库改动
之前抛出；SQLite/池错误不归入输入错误。

## 批次最大化派发（`dispatch.py`）

`dispatch_wave(pool, workers, *, before_commit=None)` 接收一轮最多 16 个执行器
名额，先在库外求本轮最大合法匹配，再用一个 SQLite 写事务提交。它只处理初次
派发：候选来自 `dispatch_ready`，成功后插入 `state='dispatched'`、`fence=1`
的 `job_attempts` 记录，不接管旧尝试或对账后的重试。

### 调用方式

调用方先建好 WAL 库中的 `quota_tenants`、`quota_holds`、`job_attempts`、
`dispatch_ready`，并完成 `ReservationIndex(pool).initialize()`。随后可重复
调用 `initialize_dispatch(pool)`；它使用 `CREATE TABLE IF NOT EXISTS` 保证
`dispatch_ready` 可用，不清候选，也不改业务行。

```python
from dispatch import dispatch_wave, initialize_dispatch

initialize_dispatch(pool)

workers = [
    # (worker_id, epoch, kinds)；worker_id 本轮唯一
    (101, 42, [1, 2]),
    (102, 42, [1]),
]

def before_commit():
    # 此处不持有 dispatch 的池借用或数据库事务；可以用同一个池写在线变更。
    ...

status, pairs = dispatch_wave(
    pool, workers, before_commit=before_commit)

# 成功：status == "committed"
# pairs == [(job_id, worker_id), ...]，按 job_id 递增
# 决策视图已被在线提交改变：("stale", [])，调用方应另起一轮
```

每个名额为 `(worker_id, epoch, kinds)`：`worker_id`、`epoch` 均为
1..2147483647 的严格整数（不接受布尔值），`worker_id` 本轮不可重复；
`kinds` 为 1..32 个不重复的 1..32 整数。名额由调用方独占，跨调用的重叠
名额也由调用方禁止；它们不是 `quota_holds` 中的配额。

### 选择规则

只有同时满足以下条件的候选才参与匹配：

1. `kind` 属于本轮至少一个执行器支持的种类；
2. `(tenant_id, slot)` 在一致性视图中没有正单位的 `quota_holds` 占用（现有
   配额入口在 units=0 时删行，因此正常数据即不存在该行）；
3. 租户的 `quota_index.ready=1`；
4. 租户当前精确余量 `max(0, max_units - used_units)` 大于 0。

匹配约束为：每个执行器至多一个作业、每个作业至多派发一次、同一租户本批
启动数不超过其精确余量。先最大化启动总数；若多个分配数量相同，把分配写成
按 `job_id` 递增的 `(job_id, worker_id)` 序列，取字典序最小者。候选物理
顺序和 `workers` 入参顺序均不影响结果。

求解是纯内存最小费用流：最多 512 个候选、16 个执行器和 64 个租户。字典序
通过按 `(job_id, worker_id)` 排序的层级权重编码，不在数据库借用内运行。

### 两次借用与 stale

一轮最多借用池两次：

1. **读取决策输入。** 在一个 `BEGIN` 一致性 WAL 读事务中读取本轮支持种类
   的全部 `dispatch_ready` 行、这些候选号位的 `quota_holds.units`，以及相关
   租户的配额/回填状态；读取后立即 rollback 并归还连接。随后才在池外求解，
   慢求解期间不持有连接，也不阻塞其他调度写入。
2. **复核并提交。** 若提供 `before_commit`，在匹配完成且没有任何借用/事务
   时调用一次（空批也调用，回调无参数）。回调可以用容量为 1 的同一个普通
   池借用并提交；回调异常原样传播，dispatch 不进入提交。之后重新借用并
   `BEGIN IMMEDIATE`，重新读取同一决策视图并与快照相比较。

   - 视图有任何相关变化：返回 `("stale", [])`，不内部重算，也不改候选、
     占用、配额总量或尝试。不能只复核入选者：新插入的可匹配作业也可能改变
     字典序最优分配。
   - 视图相同：在同一事务中逐作业删除 `dispatch_ready`、向
     `quota_holds` 插入该号位的 1 单位占用、将对应
     `quota_index.used_units` 加 1，并插入使用获选执行器 `epoch` 的尝试。
   - 只增删改本轮不支持种类的候选，且占用变化只发生在初始视图未涉及的租户
     时，快照仍相同，不会误判 stale。最终已回滚的写入不会改变快照，也不会
     使批次过期。

提交途中发生 SQLite 错误时回滚整个写事务并原样抛出错误，不会包装成
`stale`，也不会撤销回调或其他调用方此前已经提交的事务。普通 WAL 池和
`snapshot_groups=True` 的池都使用同一组 `pool.borrow()` 写资格；旧读组仍可
继续读旧快照，退出后新取景点才能看到已提交派发。

## 退役执行器对账（`reconcile.py`）

执行器世代退役后，`job_attempts` 里可能还留着 `state='uncertain'` 的尝试：
终态回执也许已经写进执行器的顺序回执日志，只是没来得及回传。
`reconcile_epoch(pool, epoch, receipts, *, sealed_count, scratch_dir)` 消费
一次回执流，补登确有回执的结果；只有日志**完整性可证**时，才把没有终态
回执的旧尝试交回 `retryable`。它修改的是真实调度资格，不执行作业，也不碰
配额与回填规则。

```python
from reconcile import reconcile_epoch

# 片段（sealed_count=None）：只补登命中的 uncertain 尝试，
# 没有回执的行保持 uncertain，绝不据缺失发起重试。
finished, retryable = reconcile_epoch(
    pool, epoch, receipts_iter,
    sealed_count=None, scratch_dir=Path(root) / "scratch")

# 封存完整（sealed_count 为 0..120000 的整数）：有效结果必须恰好覆盖
# 序号 1..sealed_count（允许精确重送）；未命中的旧尝试置 retryable。
finished, retryable = reconcile_epoch(
    pool, epoch, receipts_iter,
    sealed_count=120000, scratch_dir=Path(root) / "scratch")
# 返回值是本次实际改动的 (补登数, 置为可重试数)。
```

### 回执契约与校验

- 每行是 `(seq, job_id, fence, outcome, payload)` 五元组，均属于入参世代；
  `seq` 1..120000，`job_id`/`fence` 1..2147483647，必须是真正的整数
  （布尔值不接受）；`outcome` 为 `"ok"`/`"failed"`；`payload` 是
  1..1024 字节的 `bytes`，按原始字节落盘。单次调用最多消费 160000 行
  （含重送）。
- 同序号重送必须其余四项**逐字节/逐字符精确相同**，不能用摘要判定；同一
  `(job_id, fence)` 不得绑定两个序号。即使对应尝试在库中已被覆盖或删除
  （已归档身份），日志自身的冲突仍被拦截。
- 参数/回执不合法、超上限、身份冲突均抛 `ValueError`；迭代器自身的异常
  原样传播。封存声明与实际日志不一致（洞、越界、多余序号、计数不符）时
  **整次拒绝**，不降级成片段，也不先补登其中的好行——任何输入失败都在
  写事务之前发生，业务库零改动。

### 两阶段与在线调度

1. **准备阶段不占用业务池连接、不写业务库。** 只用一次极短的自动提交读
   记下该世代对账开始时现存的尝试身份（最多 4096 行），随即归还连接；
   之后把流落到 `scratch_dir` 中一个组件私有的临时 SQLite 库完成全部
   校验，慢流不会堵住在线调度。落盘设计使 120000 条 ×1024 字节的乱序流
   在 96 MiB 地址空间上限下也不需要进进程内存。
2. **收尾单写事务借用现有池。** 只枚举该世代、事务内此刻仍为
   `uncertain` 的当前行：回执的作业与 fence 同时精确匹配才置 `finished`
   并写入 `outcome`/`receipt`；封存完整且身份在对账开始时就存在的未命中
   旧尝试置 `retryable`（两列仍为 NULL）；片段未命中保持原状。流期间
   正常完成、取消或换 fence 的在线提交不会被旧状态覆盖，重放返回零改动。
   SQLite 失败整批回滚并传播。临时文件（含 WAL/回滚旁挂文件）在正常或
   异常返回后都不残留；池只借用不关闭，也不另开业务库连接。
3. `retryable` 只表示没有这次尝试的终态回执，不承诺作业外部副作用只
   发生一次；不要求支持多个对账调用并发执行。

## 回归测试

`test_pool.py` 仅依赖标准库，使用真实临时 WAL SQLite 库，覆盖：跨提交的
同组快照与组后新视图、写借用未提交改动不进入组、组登记/取景/归还的先后
关系、`timeout=0` 失败后的恢复、取连接与固定快照两处故障注入、组内异常、
单组登记、关闭路径（等待者被唤醒、未交付组不成功、已交付组仍可读）、
非 WAL 拒绝且日志模式不变、非法 capacity/count/timeout、容量 16 上界以及
从建组到归还期间**实际打开连接数**的峰值。并发场景用条件变量或注入钩子
作为同步点，不靠休眠碰撞。

`test_quota.py` 同为纯标准库 + 真实临时 WAL 库，覆盖：题述租户 1 的完整
分段上线场景（部分覆盖下只允许释放、就绪后精确到限额的准入与拒绝、
租户间独立就绪）、回填中途关池重开后前缀续推且不重不漏、4095→4096
最后一步不越界、单步只查本租户新增区间、并发净增加至多一笔提交（注入
同步点而非休眠）、SQLite 写失败的错误传播与额度保持、capacity=3 读组
存活期间配额写实际提交且旧组旧视图/新组新视图、随机交错与多线程下
`used_units` 恒等于已覆盖前缀真实和的不变量、拒绝时两张表均不变，以及
全部入参的类型与边界校验。

`test_dispatch.py` 同为纯标准库 + 真实临时 WAL 库，覆盖题述双租户专门化
匹配（`[(11,102),(21,101)]` 而非顺序先发）、同数量分配的字典序取舍、未就绪
/号位已占用/租户余量限制、低作业号插入与配额消耗导致的 stale、不支持种类和
未涉及租户变更不 stale、回调内已回滚写入不 stale、容量 1 池中回调实际写入、
第二份尝试写入失败后的整批回滚、故障移除后的同输入提交，以及快照读组的旧
视图/退出后新视图；另含 300 组随机小图与暴力枚举一致性校验。

运行全部回归：

```sh
python3 -m unittest -v test_pool test_quota test_dispatch test_reconcile test_wal_pressure
```

`test_wal_pressure.py` 同为纯标准库 + 真实临时 WAL 库，覆盖：构造参数
与暂存目录校验；`reclaim_wal` 的 `reclaimed`/`lease_busy`/`reader_busy`
三元组与实测前后字节；已提交但未归还的写借用仍判 `lease_busy`；归还后
WAL 真正归零且数据可读；用 `_wait`/checkpoint 钩子做同步点，验证排队中的
写借用取得资格后重新测量压力而不是沿用旧值；注入真实 SQLite 维护错误与
取样 `OSError` 时原样传播且不留写借用/维护闸门；维护闸门与并发借用的排序；
题述租户 1/2、候选 11/12/21、执行器 101/102 的无暂存目录完整场景（回调大
载荷提交保留、双连接旧视图含晚到首查、`reader_busy`、派发抛
`PoolWalPressure` 且四张表零改动、退组后不先维护直接重派得到
`[(11,102),(21,101)]`、新组看到新尝试与大载荷）；以及带暂存目录的继续派
发分支（一份共享快照、主 WAL 实际压低、旧组旧值、容量峰值 ≤3、暂存用量
与 72 MiB 上限、预放文件保留、建快照/开快照连接失败与超预算清理、关闭后
已交付组仍可查且归还后资源清零）。竞争全部用事件/钩子同步，不靠休眠。

`test_reconcile.py` 同为纯标准库 + 真实临时 WAL 库，覆盖：片段只补登/
封存才重试、fence 精确匹配（旧令牌不能完成新尝试）、跨世代隔离、重放
零改动、序号 1/1/3 冒充完整范围、同序号异内容、身份复用（含已归档
身份）、读取中断与各类参数越界的整次拒绝；流消费途中在线换 fence/结束
尝试时只改仍合格的旧尝试、注入 SQLite 批量写失败整批回滚且池仍可用、
双读快照组存活期间对账提交的旧视图/新组新视图；并在独立子进程
`RLIMIT_AS=96 MiB` 下用单次迭代生成 120000 条乱序满规模回执（末尾精确
重送、绝大多数身份已归档）验证补登/重试计数与正常、错误路径均无临时
资源残留。运行：

```sh
python3 -m unittest -v test_pool test_quota test_dispatch test_reconcile test_wal_pressure
```
