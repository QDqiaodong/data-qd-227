"""Tenant slot-hold reservation index with an inline quota write guard.

The component owns every mutation of ``quota_holds`` once it is deployed.
It keeps a per-tenant, incrementally backfilled running total in
``quota_index`` so the hot admission path never scans the hold table, while a
single backfill worker (``advance``) brings tenants online prefix by prefix
without ever aggregating the whole table or rewriting legacy holds.

State model
-----------
``quota_index.covered_slots`` is the length of the slot prefix (counted from
slot 1) already folded into ``used_units``; slot ``covered_slots + 1`` is the
next slot to inspect.  ``ready`` flips on exactly when the prefix reaches
:data:`SLOT_MAX`.

* ``advance`` only ever reads the raw holds inside the newly added slot range
  (``slot > covered AND slot <= target``); already covered slots and other
  tenants are never re-aggregated.  Empty slots count as progress too.
* While a tenant is not ready, decreases, same-value writes and deletions are
  always allowed.  A change on an already covered slot is merged into the
  running total in the same transaction; a change ahead of the prefix is left
  in the raw table alone and observed later by the range scan, so a deletion
  can never resurrect an old hold and a late decrease can never be lost.
* A net increase is admitted only once the tenant is ready, against the exact
  committed total; otherwise the write returns ``"not_ready"`` (before
  completion) or ``"limit_exceeded"`` (after completion) without touching
  either table.

Every operation runs in one ``BEGIN IMMEDIATE`` transaction on a borrowed
pool connection.  The raw hold, the index update and the admission decision
therefore share SQLite's single commit order, and a failed write is rolled
back and propagated rather than disguised as a quota rejection.  No
transaction or borrowed connection survives an API call.
"""
import threading

TENANT_MIN = 1
TENANT_MAX = 64
SLOT_MIN = 1
SLOT_MAX = 4096
ADVANCE_MIN = 1
ADVANCE_MAX = 512
UNITS_MIN = 0
UNITS_MAX = 2147483647


def _require_int(value, name):
    # ``bool`` is an ``int`` subclass; the contract rejects booleans.
    if type(value) is not int:
        raise ValueError("%s must be an integer" % name)
    return value


class ReservationIndex:
    """Backfillable per-tenant hold totals and quota guard on one pool.

    The pool is borrowed, never owned: this class opens no SQLite connections
    itself and never closes the pool.
    """

    def __init__(self, pool):
        self._pool = pool
        # Component-local serialization for the write entries.  SQLite
        # serializes writers anyway; the lock additionally keeps the backfill
        # worker and business writers from racing for the write lock and
        # makes the interleaving deterministic.  Reads (``status``) do not
        # take it.
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Lifecycle

    def initialize(self):
        """Create the index state for an existing database.

        Idempotent across repeated calls and pool reopenings: rows already
        built for a tenant are preserved, and legacy holds are neither
        aggregated nor rewritten.  Only this method issues DDL; call it once
        before using the component or opening snapshot read groups.
        """
        with self._pool.borrow() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                db.execute(
                    "CREATE TABLE IF NOT EXISTS quota_index ("
                    "tenant_id INTEGER PRIMARY KEY, "
                    "covered_slots INTEGER NOT NULL, "
                    "used_units INTEGER NOT NULL, "
                    "ready INTEGER NOT NULL)")
                # One row per configured tenant, starting with an empty
                # prefix.  ``OR IGNORE`` keeps any committed backfill
                # progress from an earlier pool incarnation.
                db.execute(
                    "INSERT OR IGNORE INTO quota_index "
                    "(tenant_id, covered_slots, used_units, ready) "
                    "SELECT tenant_id, 0, 0, 0 FROM quota_tenants")
                db.commit()
            except BaseException:
                db.rollback()
                raise

    # ------------------------------------------------------------------
    # Backfill

    def advance(self, tenant_id, slots):
        """Extend tenant ``tenant_id``'s covered prefix by ``slots`` slots.

        Returns the committed ``(covered_slots, used_units)``.  Each call
        covers between 1 and 512 new slots, clamped at 4096; empty slots
        count as progress.  On a completed tenant the call is a side-effect
        free read.  A failed transaction never advances the prefix.
        """
        _require_int(tenant_id, "tenant_id")
        _require_int(slots, "slots")
        if not TENANT_MIN <= tenant_id <= TENANT_MAX:
            raise ValueError("tenant_id out of range")
        if not ADVANCE_MIN <= slots <= ADVANCE_MAX:
            raise ValueError("slots must be between 1 and 512")

        with self._lock:
            with self._pool.borrow() as db:
                # Read-only fast path for completed tenants: an autocommit
                # point query leaves no read transaction on the connection
                # and takes no write lock, so it is a genuine side-effect
                # free query.
                ready_row = db.execute(
                    "SELECT used_units, ready FROM quota_index "
                    "WHERE tenant_id = ?", (tenant_id,)).fetchone()
                if ready_row is None:
                    raise ValueError("unknown tenant_id")
                if ready_row[1]:
                    return SLOT_MAX, ready_row[0]

                db.execute("BEGIN IMMEDIATE")
                try:
                    state = db.execute(
                        "SELECT covered_slots, used_units, ready "
                        "FROM quota_index WHERE tenant_id = ?",
                        (tenant_id,)).fetchone()
                    covered, used, ready = state
                    if ready:
                        # Another writer completed the tenant while we waited
                        # for the write lock.
                        db.rollback()
                        return SLOT_MAX, used

                    target = min(SLOT_MAX, covered + slots)
                    if target > covered:
                        # Inspect only the newly covered range of this
                        # tenant.  The composite primary key on quota_holds
                        # makes this a bounded index range scan.
                        added = db.execute(
                            "SELECT COALESCE(SUM(units), 0) "
                            "FROM quota_holds "
                            "WHERE tenant_id = ? AND slot > ? AND slot <= ?",
                            (tenant_id, covered, target)).fetchone()[0]
                        used += added
                        covered = target
                        if covered == SLOT_MAX:
                            db.execute(
                                "UPDATE quota_index "
                                "SET covered_slots = ?, used_units = ?, "
                                "ready = 1 WHERE tenant_id = ?",
                                (covered, used, tenant_id))
                        else:
                            db.execute(
                                "UPDATE quota_index "
                                "SET covered_slots = ?, used_units = ? "
                                "WHERE tenant_id = ?",
                                (covered, used, tenant_id))
                    db.commit()
                    return covered, used
                except BaseException:
                    db.rollback()
                    raise

    def status(self, tenant_id):
        """Return ``(covered_slots, used_units)`` from committed state.

        ``used_units`` is ``None`` until all 4096 slots are covered; a partial
        sum must never be used as an admission total.
        """
        _require_int(tenant_id, "tenant_id")
        if not TENANT_MIN <= tenant_id <= TENANT_MAX:
            raise ValueError("tenant_id out of range")

        with self._pool.borrow() as db:
            row = db.execute(
                "SELECT covered_slots, used_units, ready "
                "FROM quota_index WHERE tenant_id = ?",
                (tenant_id,)).fetchone()
        if row is None:
            raise ValueError("unknown tenant_id")
        covered, used, ready = row
        return covered, used if ready else None

    # ------------------------------------------------------------------
    # Guarded writes

    def set_hold(self, tenant_id, slot, units):
        """Set the hold on one slot to an absolute value.

        ``units = 0`` deletes the row (a missing row is a no-op).  Returns
        ``"applied"``, ``"not_ready"`` or ``"limit_exceeded"``.  Same-value
        writes, decreases and deletions are always allowed; a net increase on
        a tenant that is not ready is ``"not_ready"``, and one that would
        push the exact total over ``max_units`` is ``"limit_exceeded"``.
        Rejections change neither the raw holds nor the backfill state.
        """
        _require_int(tenant_id, "tenant_id")
        _require_int(slot, "slot")
        _require_int(units, "units")
        if not TENANT_MIN <= tenant_id <= TENANT_MAX:
            raise ValueError("tenant_id out of range")
        if not SLOT_MIN <= slot <= SLOT_MAX:
            raise ValueError("slot out of range")
        if not UNITS_MIN <= units <= UNITS_MAX:
            raise ValueError("units out of range")

        with self._lock:
            with self._pool.borrow() as db:
                db.execute("BEGIN IMMEDIATE")
                try:
                    state = db.execute(
                        "SELECT covered_slots, ready "
                        "FROM quota_index WHERE tenant_id = ?",
                        (tenant_id,)).fetchone()
                    if state is None:
                        raise ValueError("unknown tenant_id")
                    covered, ready = state

                    # Point lookup only: the hot path never scans the tenant.
                    old_row = db.execute(
                        "SELECT units FROM quota_holds "
                        "WHERE tenant_id = ? AND slot = ?",
                        (tenant_id, slot)).fetchone()
                    old_units = old_row[0] if old_row is not None else 0

                    if units == 0:
                        verdict = self._apply_deletion(
                            db, tenant_id, slot, old_row, covered)
                    elif old_row is None:
                        verdict = self._apply_insert(
                            db, tenant_id, slot, units, ready)
                    else:
                        verdict = self._apply_update(
                            db, tenant_id, slot, units, old_units,
                            ready, covered)

                    if verdict == "applied":
                        db.commit()
                    else:
                        # Admission rejection: discard the transaction so no
                        # raw hold or index row can change.
                        db.rollback()
                    return verdict
                except BaseException:
                    db.rollback()
                    raise

    # ------------------------------------------------------------------
    # set_hold internals; each runs inside the caller's write transaction

    def _apply_deletion(self, db, tenant_id, slot, old_row, covered):
        if old_row is None:
            return "applied"  # documented no-op
        old_units = old_row[0]
        db.execute(
            "DELETE FROM quota_holds "
            "WHERE tenant_id = ? AND slot = ?",
            (tenant_id, slot))
        if slot <= covered:
            db.execute(
                "UPDATE quota_index SET used_units = used_units - ? "
                "WHERE tenant_id = ?",
                (old_units, tenant_id))
        return "applied"

    def _apply_insert(self, db, tenant_id, slot, units, ready):
        # Every insertion is a net increase and therefore needs admission.
        if not ready:
            return "not_ready"
        if not self._within_limit(db, tenant_id, units):
            return "limit_exceeded"
        db.execute(
            "INSERT INTO quota_holds (tenant_id, slot, units) VALUES (?, ?, ?)",
            (tenant_id, slot, units))
        db.execute(
            "UPDATE quota_index SET used_units = used_units + ? "
            "WHERE tenant_id = ?",
            (units, tenant_id))
        return "applied"

    def _apply_update(self, db, tenant_id, slot, units, old_units,
                      ready, covered):
        delta = units - old_units
        if delta > 0:
            # A net increase is the only change subject to admission.
            if not ready:
                return "not_ready"
            if not self._within_limit(db, tenant_id, delta):
                return "limit_exceeded"
        if delta != 0:
            db.execute(
                "UPDATE quota_holds SET units = ? "
                "WHERE tenant_id = ? AND slot = ?",
                (units, tenant_id, slot))
            if slot <= covered:
                db.execute(
                    "UPDATE quota_index SET used_units = used_units + ? "
                    "WHERE tenant_id = ?",
                    (delta, tenant_id))
        return "applied"

    @staticmethod
    def _within_limit(db, tenant_id, delta):
        """Check the exact committed total plus a nonnegative delta."""
        row = db.execute(
            "SELECT i.used_units, t.max_units "
            "FROM quota_index AS i "
            "JOIN quota_tenants AS t ON t.tenant_id = i.tenant_id "
            "WHERE i.tenant_id = ?",
            (tenant_id,)).fetchone()
        used_units, max_units = row
        return used_units + delta <= max_units
