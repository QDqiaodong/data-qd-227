"""Live active-attempt roster handoff for the dispatch execution monitor.

The monitor only needs an *active attempt roster*: it asks the executor for
progress of the jobs that are currently ``dispatched`` or ``running`` and must
stop asking about attempts that have left that active set.  This component
hands the roster over incrementally, without re-reading the whole
``job_attempts`` table on every poll.

API (one monitor at a time)
===========================

``enable_projection(pool, *, dirty_limit)``
    Install the roster bookkeeping (state, dirty set and SQLite triggers) on
    a database that is accessed through the caller's existing
    :class:`pool.ConnectionPool`.  Must be called before online access.
    ``dirty_limit`` is a non-boolean integer in ``2..4096``.

``next_projection(pool)``
    Return ``None`` when there is nothing new, otherwise
    ``(ticket, kind, rows)`` where ``ticket`` is a monotonically increasing,
    never-reused positive integer, ``kind`` is ``"full"`` or ``"patch"`` and
    ``rows`` is an ascending ``(job_id, entry)`` list.  ``entry`` is
    ``(epoch, fence, state)`` for an active attempt or ``None`` meaning
    "remove this job from the monitor's roster".  The very first ticket is
    always ``"full"`` (even for an empty roster).

``ack_projection(pool, ticket)``
    Acknowledge the single outstanding ticket *after* the monitor actually
    applied it.  Returns ``True`` only for the current unconfirmed ticket;
    any other legal ticket number returns ``False`` without touching it.

Design
======

The triggers on ``job_attempts`` record exactly the roster-relevant changes
in the same business transaction: inserts of an active state, updates where
the ``(state, epoch, fence)`` triple changes while the row is (or becomes)
active, and deletions of active rows.  Rolled back transactions therefore
leave no trace, inactive-only updates record nothing and repeated updates of
the same job collapse into one entry.  A small ``projection_dirty`` set holds
at most ``dirty_limit`` distinct job ids; the already delivered (but not yet
acknowledged) ticket is counted separately.

One ticket is materialized at a single committed fetch point inside a
``BEGIN IMMEDIATE`` transaction (the write lock excludes concurrent roster
commits), then stored row by row, so a repeated pull - even after closing and
reopening the pool - returns the original content and never mixes in later
changes.  Incremental patches only join the bounded dirty set against
``job_attempts`` by primary key; they never scan the whole attempt table.

Overflow policy (when a business write would exceed ``dirty_limit``)
====================================================================

The deployment may choose one of two behaviours.  Both are transactional and
preserve the dispatch transaction boundary:

``overflow="defer"`` (default)
    The offending write fails with ``sqlite3.IntegrityError`` whose message
    contains ``projection_backlog``.  ``dispatch_wave`` rolls back its whole
    batch (candidates, holds, quota totals and attempts) and re-raises the
    error unchanged.  Once the old ticket is acknowledged and the next ticket
    is pulled (freeing the dirty budget) the dispatch can simply be retried.

``overflow="commit"`` (opt in via ``enable_projection(..., overflow="commit")``)
    The write commits; in the same commit the next ticket is obligated to
    resend the full active roster (``force_full``) and the superseded pending
    increment is dropped.  The already delivered unconfirmed ticket is never
    replaced - it must be acknowledged first - and changes made after the new
    full ticket has been issued still go into later batches.
"""

__all__ = ["enable_projection", "next_projection", "ack_projection"]

DIRTY_LIMIT_MIN = 2
DIRTY_LIMIT_MAX = 4096
#: Hard cap on the number of rows one ticket may carry (the full roster or
#: one bounded increment).
ROSTER_MAX = 4096

ACTIVE_STATES = ("dispatched", "running")
OVERFLOW_MODES = ("defer", "commit")

_STATE_SQL = """
CREATE TABLE IF NOT EXISTS projection_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    dirty_limit INTEGER NOT NULL,
    overflow TEXT NOT NULL,
    full_issued INTEGER NOT NULL,
    force_full INTEGER NOT NULL,
    next_ticket INTEGER NOT NULL,
    active_ticket INTEGER
)
"""

_DIRTY_SQL = """
CREATE TABLE IF NOT EXISTS projection_dirty (
    job_id INTEGER PRIMARY KEY
)
"""

_TICKET_SQL = """
CREATE TABLE IF NOT EXISTS projection_ticket (
    ticket INTEGER PRIMARY KEY,
    kind TEXT NOT NULL
)
"""

_TICKET_ROWS_SQL = """
CREATE TABLE IF NOT EXISTS projection_ticket_rows (
    ticket INTEGER NOT NULL,
    job_id INTEGER NOT NULL,
    epoch INTEGER,
    fence INTEGER,
    state TEXT,
    PRIMARY KEY (ticket, job_id)
)
"""

_TRIGGER_NAMES = (
    "projection_dirty_insert",
    "projection_dirty_update",
    "projection_dirty_delete",
)

# ----------------------------------------------------------------------
# Defer-mode triggers
#
# RAISE(ROLLBACK, ...) rolls back the *whole* business transaction (not just
# the triggering statement); Python surfaces it as sqlite3.IntegrityError
# whose message contains ``projection_backlog``.  dispatch_wave therefore
# rolls back its entire batch - candidates, holds, quota totals and attempts -
# and re-raises the error unchanged.  The budget check runs before the dirty
# insert, and an already recorded job id never fires the trigger again.

_DEFER_INSERT_SQL = """
CREATE TRIGGER projection_dirty_insert AFTER INSERT ON job_attempts
WHEN NEW.state IN ('dispatched', 'running')
 AND (SELECT full_issued FROM projection_state WHERE id = 1) = 1
 AND NOT EXISTS (SELECT 1 FROM projection_dirty WHERE job_id = NEW.job_id)
BEGIN
    SELECT RAISE(ROLLBACK, 'projection_backlog')
    WHERE (SELECT COUNT(*) FROM projection_dirty) >=
          (SELECT dirty_limit FROM projection_state WHERE id = 1);
    INSERT INTO projection_dirty (job_id) VALUES (NEW.job_id);
END
"""

_DEFER_UPDATE_SQL = """
CREATE TRIGGER projection_dirty_update AFTER UPDATE ON job_attempts
WHEN (OLD.state IN ('dispatched', 'running')
       OR NEW.state IN ('dispatched', 'running'))
 AND NOT (OLD.state = NEW.state
       AND OLD.epoch = NEW.epoch
       AND OLD.fence = NEW.fence)
 AND (SELECT full_issued FROM projection_state WHERE id = 1) = 1
 AND NOT EXISTS (SELECT 1 FROM projection_dirty WHERE job_id = NEW.job_id)
BEGIN
    SELECT RAISE(ROLLBACK, 'projection_backlog')
    WHERE (SELECT COUNT(*) FROM projection_dirty) >=
          (SELECT dirty_limit FROM projection_state WHERE id = 1);
    INSERT INTO projection_dirty (job_id) VALUES (NEW.job_id);
END
"""

_DEFER_DELETE_SQL = """
CREATE TRIGGER projection_dirty_delete AFTER DELETE ON job_attempts
WHEN OLD.state IN ('dispatched', 'running')
 AND (SELECT full_issued FROM projection_state WHERE id = 1) = 1
 AND NOT EXISTS (SELECT 1 FROM projection_dirty WHERE job_id = OLD.job_id)
BEGIN
    SELECT RAISE(ROLLBACK, 'projection_backlog')
    WHERE (SELECT COUNT(*) FROM projection_dirty) >=
          (SELECT dirty_limit FROM projection_state WHERE id = 1);
    INSERT INTO projection_dirty (job_id) VALUES (OLD.job_id);
END
"""

# ----------------------------------------------------------------------
# Commit-mode triggers
#
# When the pending budget is exhausted the trigger flips ``force_full`` and
# drops the pending increment in the *same commit*; the next ticket then
# resends the full roster.  While ``force_full`` is set no further dirty rows
# are recorded (the full will contain the final state anyway).  Once the full
# is issued ``force_full`` is reset and later changes are recorded again.

_COMMIT_INSERT_SQL = """
CREATE TRIGGER projection_dirty_insert AFTER INSERT ON job_attempts
WHEN NEW.state IN ('dispatched', 'running')
 AND (SELECT full_issued FROM projection_state WHERE id = 1) = 1
 AND NOT EXISTS (SELECT 1 FROM projection_dirty WHERE job_id = NEW.job_id)
 AND (SELECT force_full FROM projection_state WHERE id = 1) = 0
BEGIN
    UPDATE projection_state SET force_full = 1
    WHERE id = 1
      AND (SELECT COUNT(*) FROM projection_dirty) >= dirty_limit;
    DELETE FROM projection_dirty
    WHERE (SELECT force_full FROM projection_state WHERE id = 1) = 1;
    INSERT OR IGNORE INTO projection_dirty (job_id)
    SELECT NEW.job_id
    WHERE (SELECT force_full FROM projection_state WHERE id = 1) = 0;
END
"""

_COMMIT_UPDATE_SQL = """
CREATE TRIGGER projection_dirty_update AFTER UPDATE ON job_attempts
WHEN (OLD.state IN ('dispatched', 'running')
       OR NEW.state IN ('dispatched', 'running'))
 AND NOT (OLD.state = NEW.state
       AND OLD.epoch = NEW.epoch
       AND OLD.fence = NEW.fence)
 AND (SELECT full_issued FROM projection_state WHERE id = 1) = 1
 AND NOT EXISTS (SELECT 1 FROM projection_dirty WHERE job_id = NEW.job_id)
 AND (SELECT force_full FROM projection_state WHERE id = 1) = 0
BEGIN
    UPDATE projection_state SET force_full = 1
    WHERE id = 1
      AND (SELECT COUNT(*) FROM projection_dirty) >= dirty_limit;
    DELETE FROM projection_dirty
    WHERE (SELECT force_full FROM projection_state WHERE id = 1) = 1;
    INSERT OR IGNORE INTO projection_dirty (job_id)
    SELECT NEW.job_id
    WHERE (SELECT force_full FROM projection_state WHERE id = 1) = 0;
END
"""

_COMMIT_DELETE_SQL = """
CREATE TRIGGER projection_dirty_delete AFTER DELETE ON job_attempts
WHEN OLD.state IN ('dispatched', 'running')
 AND (SELECT full_issued FROM projection_state WHERE id = 1) = 1
 AND NOT EXISTS (SELECT 1 FROM projection_dirty WHERE job_id = OLD.job_id)
 AND (SELECT force_full FROM projection_state WHERE id = 1) = 0
BEGIN
    UPDATE projection_state SET force_full = 1
    WHERE id = 1
      AND (SELECT COUNT(*) FROM projection_dirty) >= dirty_limit;
    DELETE FROM projection_dirty
    WHERE (SELECT force_full FROM projection_state WHERE id = 1) = 1;
    INSERT OR IGNORE INTO projection_dirty (job_id)
    SELECT OLD.job_id
    WHERE (SELECT force_full FROM projection_state WHERE id = 1) = 0;
END
"""

_TRIGGER_SQL = {
    "defer": (_DEFER_INSERT_SQL, _DEFER_UPDATE_SQL, _DEFER_DELETE_SQL),
    "commit": (_COMMIT_INSERT_SQL, _COMMIT_UPDATE_SQL, _COMMIT_DELETE_SQL),
}


def _require_int(value, name):
    # ``bool`` is an ``int`` subclass; the contract rejects booleans.
    if type(value) is not int:
        raise ValueError("%s must be a non-boolean integer" % name)
    return value


def _validate_overflow(overflow):
    # ``None`` is the documented "keep the persisted policy" value; every
    # other non-string or unknown name is rejected.
    if overflow is None:
        return None
    if type(overflow) is not str or overflow not in OVERFLOW_MODES:
        raise ValueError(
            "overflow must be one of %s" % (", ".join(OVERFLOW_MODES),))
    return overflow


def _table_exists(db, name):
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,)).fetchone() is not None


def _enabled(db):
    """True when the projection state has been installed on this database."""
    return _table_exists(db, "projection_state")


def _require_enabled(db):
    if not _enabled(db):
        raise ValueError("projection is not enabled on this database")


def _read_state(db):
    """Return ``(dirty_limit, overflow, full_issued, force_full,
    next_ticket, active_ticket)`` or ``None``."""
    return db.execute(
        "SELECT dirty_limit, overflow, full_issued, force_full, "
        "next_ticket, active_ticket FROM projection_state WHERE id = 1"
    ).fetchone()


def _install_triggers(db, overflow):
    """(Re)create exactly the trigger set for ``overflow``."""
    for name in _TRIGGER_NAMES:
        db.execute("DROP TRIGGER IF EXISTS %s" % name)
    for sql in _TRIGGER_SQL[overflow]:
        db.execute(sql)


def enable_projection(pool, *, dirty_limit, overflow=None):
    """Install (or re-confirm) the roster handoff on an existing database.

    Reuses the caller's pool: it only borrows connections, never closes the
    pool and never opens a separate business-database connection.  Call it
    once before online access; calling it again after closing and reopening
    the pool with the same ``dirty_limit`` is idempotent and preserves the
    acknowledgement progress, the pending handoff data and the ticket
    counter.

    ``dirty_limit`` must be a non-boolean integer in ``2..4096``.  Specifying
    a different limit for an already enabled database raises
    :class:`ValueError`; the limit cannot be changed online.  ``overflow``
    optionally selects the backlog-overflow policy (``"defer"`` or
    ``"commit"``); omitting it keeps the persisted policy (a fresh database
    starts with ``"defer"``).  An explicitly different policy on an already
    enabled database also raises :class:`ValueError`.
    """
    _require_int(dirty_limit, "dirty_limit")
    if not DIRTY_LIMIT_MIN <= dirty_limit <= DIRTY_LIMIT_MAX:
        raise ValueError(
            "dirty_limit must be between %d and %d"
            % (DIRTY_LIMIT_MIN, DIRTY_LIMIT_MAX))
    overflow = _validate_overflow(overflow)

    with pool.borrow() as db:
        if not _table_exists(db, "job_attempts"):
            raise ValueError("projection requires a job_attempts table")
        db.execute("BEGIN IMMEDIATE")
        try:
            db.execute(_STATE_SQL)
            db.execute(_DIRTY_SQL)
            db.execute(_TICKET_SQL)
            db.execute(_TICKET_ROWS_SQL)

            row = _read_state(db)
            if row is None:
                policy = overflow if overflow is not None else "defer"
                db.execute(
                    "INSERT INTO projection_state "
                    "(id, dirty_limit, overflow, full_issued, force_full, "
                    "next_ticket, active_ticket) "
                    "VALUES (1, ?, ?, 0, 0, 1, NULL)",
                    (dirty_limit, policy))
            else:
                stored_limit = row[0]
                stored_policy = row[1]
                if stored_limit != dirty_limit:
                    raise ValueError(
                        "projection is already enabled with dirty_limit=%d"
                        % stored_limit)
                if overflow is not None and overflow != stored_policy:
                    raise ValueError(
                        "projection is already enabled with overflow=%r"
                        % (stored_policy,))
                policy = stored_policy

            _install_triggers(db, policy)
            db.commit()
        except BaseException:
            db.rollback()
            raise


def _ticket_rows(db, ticket):
    """Materialized rows of one ticket, independent of later commits."""
    stored = db.execute(
        "SELECT job_id, epoch, fence, state FROM projection_ticket_rows "
        "WHERE ticket = ? ORDER BY job_id", (ticket,)).fetchall()
    return [
        (job_id, None if state is None else (epoch, fence, state))
        for job_id, epoch, fence, state in stored
    ]


def _active_roster(db):
    """Read the whole active roster at the current fetch point (bounded).

    One scan that stops after ``ROSTER_MAX + 1`` rows, so memory stays bounded
    even against a corrupt/overfull table; the extra row only signals "too
    big" for a single ticket.
    """
    rows = db.execute(
        "SELECT job_id, epoch, fence, state FROM job_attempts "
        "WHERE state IN ('dispatched', 'running') ORDER BY job_id "
        "LIMIT ?", (ROSTER_MAX + 1,)).fetchall()
    if len(rows) > ROSTER_MAX:
        raise ValueError(
            "active roster exceeds the %d-row ticket limit" % ROSTER_MAX)
    return [(job_id, (epoch, fence, state))
            for job_id, epoch, fence, state in rows]


def _dirty_patch(db):
    """Resolve the bounded dirty set at the current fetch point.

    The plan is one scan of ``projection_dirty`` plus an INTEGER PRIMARY KEY
    lookup per job; the full attempt table is never scanned.
    """
    joined = db.execute(
        "SELECT d.job_id, a.epoch, a.fence, a.state "
        "FROM projection_dirty AS d "
        "LEFT JOIN job_attempts AS a ON a.job_id = d.job_id "
        "ORDER BY d.job_id").fetchall()
    patch = []
    for job_id, epoch, fence, state in joined:
        if state in ACTIVE_STATES:
            patch.append((job_id, (epoch, fence, state)))
        else:
            # Deleted or no longer active at this fetch point: remove it.
            patch.append((job_id, None))
    return patch


def next_projection(pool):
    """Pull the next roster handoff, or ``None`` when nothing changed.

    Returns ``(ticket, kind, rows)``.  While a ticket is unconfirmed this
    returns that ticket's original materialized content, never changes made
    after its fetch point - including after the pool was closed and reopened.
    The call holds no borrowed connection, cursor or read view when it
    returns.

    Raises :class:`ValueError` when projection is not enabled.
    """
    with pool.borrow() as db:
        _require_enabled(db)
        db.execute("BEGIN IMMEDIATE")
        try:
            state = _read_state(db)
            if state is None:
                raise ValueError("projection is not enabled on this database")
            # ``dirty_limit`` / ``overflow`` are enforced by the installed
            # triggers, not by the pull path.
            _limit, _policy, full_issued, force_full, next_ticket, \
                active_ticket = state

            if active_ticket is not None:
                meta = db.execute(
                    "SELECT kind FROM projection_ticket WHERE ticket = ?",
                    (active_ticket,)).fetchone()
                if meta is None:
                    raise ValueError(
                        "projection ticket %d is missing its metadata"
                        % active_ticket)
                rows = _ticket_rows(db, active_ticket)
                db.rollback()
                return active_ticket, meta[0], rows

            if not full_issued or force_full:
                # First ticket (or a commit-mode overflow): resend the whole
                # active roster; it replaces whatever the monitor had and
                # includes all earlier pending changes.
                rows = _active_roster(db)
                kind = "full"
                db.execute("DELETE FROM projection_dirty")
                db.execute(
                    "UPDATE projection_state "
                    "SET full_issued = 1, force_full = 0 WHERE id = 1")
            else:
                if db.execute(
                        "SELECT 1 FROM projection_dirty LIMIT 1").fetchone() \
                        is None:
                    db.rollback()
                    return None
                rows = _dirty_patch(db)
                kind = "patch"
                # The delivered increment is counted separately from the
                # pending budget from now on.
                db.execute("DELETE FROM projection_dirty")

            ticket = next_ticket
            db.execute(
                "INSERT INTO projection_ticket (ticket, kind) VALUES (?, ?)",
                (ticket, kind))
            db.executemany(
                "INSERT INTO projection_ticket_rows "
                "(ticket, job_id, epoch, fence, state) "
                "VALUES (?, ?, ?, ?, ?)",
                [
                    (ticket, job_id,
                     None if entry is None else entry[0],
                     None if entry is None else entry[1],
                     None if entry is None else entry[2])
                    for job_id, entry in rows
                ])
            db.execute(
                "UPDATE projection_state SET next_ticket = ?, "
                "active_ticket = ? WHERE id = 1",
                (ticket + 1, ticket))
            db.commit()
            return ticket, kind, rows
        except BaseException:
            db.rollback()
            raise


def ack_projection(pool, ticket):
    """Acknowledge the outstanding ticket after the monitor applied it.

    Returns ``True`` only when ``ticket`` is the current unconfirmed ticket;
    other legal ticket numbers (including an already acknowledged or stale
    one) return ``False`` without clearing or advancing anything.  A
    non-boolean positive integer is required; anything else raises
    :class:`ValueError`, as does a database where projection is not enabled.
    """
    _require_int(ticket, "ticket")
    if ticket <= 0:
        raise ValueError("ticket must be a positive integer")

    with pool.borrow() as db:
        _require_enabled(db)
        db.execute("BEGIN IMMEDIATE")
        try:
            state = _read_state(db)
            if state is None:
                raise ValueError("projection is not enabled on this database")
            active_ticket = state[5]
            if active_ticket is None or ticket != active_ticket:
                db.rollback()
                return False
            db.execute(
                "DELETE FROM projection_ticket_rows WHERE ticket = ?",
                (ticket,))
            db.execute(
                "DELETE FROM projection_ticket WHERE ticket = ?", (ticket,))
            db.execute(
                "UPDATE projection_state SET active_ticket = NULL WHERE id = 1")
            db.commit()
            return True
        except BaseException:
            db.rollback()
            raise
