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
