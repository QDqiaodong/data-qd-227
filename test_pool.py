from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest

from pool import ConnectionPool, PoolClosed, PoolTimeout


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
