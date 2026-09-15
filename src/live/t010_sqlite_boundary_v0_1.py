"""T010-only owned-store checkpoint observation at a completed work boundary.

A nonblocking RESTART uses SQLite's original WAL/checkpoint machinery. It
never waits for or interrupts a reader, rewrites an application record, accepts
different history, or removes a file. An incomplete restart checkpoint prevents
another ordinary source/ENTRY unit;
the caller must continue to permit original admitted DRY recovery.
"""
from __future__ import annotations

import sqlite3


def checkpoint_owned_stores(runtime):
    """Return exact original SQLite counters, or a fixed fail-closed reason.

    Only the three already-owned WAL connections are used. No database path is
    accepted, no new connection is opened, and no transaction is committed or
    rolled back on the caller's behalf. The producer may wrap its connection;
    executing a PRAGMA through that existing wrapper preserves its ownership.
    """
    if runtime.capability != "NO_BROADCAST":
        raise ValueError("T010_CHECKPOINT_DRY_ONLY")
    observations = {}
    if sqlite3.sqlite_version != "3.53.1":
        return False, "OWNED_SQLITE_QUALIFIED_ENGINE_REQUIRED", observations
    for name, owner, attribute in (("ledger", runtime.ledger, "_conn"),
            ("source", runtime.source, "_conn"), ("producer", runtime.producer, "conn")):
        connection = getattr(owner, attribute, None)
        if connection is None:
            return False, "OWNED_SQLITE_CONNECTION_UNAVAILABLE", observations
        try:
            mode = connection.execute("PRAGMA journal_mode").fetchone()
            if mode is None or tuple(mode) != ("wal",):
                return False, "OWNED_SQLITE_WAL_MODE_REQUIRED", observations
            # These are premises of the source-bound physical page model,
            # observed without changing any connection or persisted setting.
            for pragma, expected in (("page_size", 4096), ("auto_vacuum", 0),
                    ("encoding", "UTF-8")):
                observed = connection.execute("PRAGMA "+pragma).fetchone()
                if observed is None or tuple(observed) != (expected,):
                    return False, "OWNED_SQLITE_PHYSICAL_MODEL_CONFLICT", observations
            previous_timeout = connection.execute("PRAGMA busy_timeout").fetchone()
            if (previous_timeout is None or len(previous_timeout) != 1
                    or type(previous_timeout[0]) is not int or previous_timeout[0] < 0):
                return False, "OWNED_SQLITE_BUSY_TIMEOUT_UNAVAILABLE", observations
            try:
                connection.execute("PRAGMA busy_timeout=0")
                row = connection.execute("PRAGMA wal_checkpoint(RESTART)").fetchone()
            finally:
                # The value came from this same SQLite connection, never a
                # profile string. Preserve the original caller's lock policy.
                connection.execute("PRAGMA busy_timeout="+str(previous_timeout[0]))
        except sqlite3.Error:
            return False, "OWNED_SQLITE_CHECKPOINT_UNAVAILABLE", observations
        if row is None or len(row) != 3 or any(type(value) is not int for value in row):
            return False, "OWNED_SQLITE_CHECKPOINT_COUNTERS_INVALID", observations
        busy, frames, checkpointed = row
        observations[name] = {"busy": busy, "frames": frames, "checkpointed": checkpointed}
        if busy != 0 or frames < 0 or checkpointed < 0 or frames != checkpointed:
            return False, "OWNED_SQLITE_CHECKPOINT_INCOMPLETE", observations
    return True, None, observations
