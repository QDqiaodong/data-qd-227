from pathlib import Path
from tempfile import TemporaryDirectory

from pool import ConnectionPool, PoolTimeout


if __name__ == "__main__":
    with TemporaryDirectory() as root:
        with ConnectionPool(Path(root) / "events.db", capacity=1) as pool:
            with pool.borrow() as db:
                db.execute("CREATE TABLE events(value TEXT)")
                db.execute("INSERT INTO events VALUES ('committed')")
                db.commit()
                db.execute("INSERT INTO events VALUES ('discarded')")
                try:
                    with pool.borrow(timeout=0):
                        raise AssertionError("unexpected second lease")
                except PoolTimeout:
                    print("capacity: busy")
            with pool.borrow() as next_db:
                assert next_db is db
                rows = next_db.execute("SELECT value FROM events").fetchall()
                assert rows == [("committed",)] and not next_db.in_transaction
                print("reused:", rows)
