"""Finite Operations ownership only; no economic permission or process loop.

Control transitions and exact mutation calls serialize on one SQLite writer
transaction. A completed stop/takeover therefore fences every later dispatch;
an already dispatched call must finish before that transition can commit.
The trusted host must configure one canonical store per wallet/economic domain.
"""
from __future__ import annotations

import os
import re
import sqlite3
from contextlib import contextmanager, closing, nullcontext
from dataclasses import dataclass, asdict
from uuid import uuid4

from .ledger_repository_v0_1 import _journal_path
from .ledger_domain_v0_1 import LedgerDomain

VERSION = "live_operations_ownership_v0.1"
_DDL = ("CREATE TABLE operations (singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
                "version TEXT NOT NULL, domain_id TEXT NOT NULL, binding_digest TEXT NOT NULL, generation INTEGER NOT NULL, "
                "process_identity TEXT, nonce TEXT, stopped INTEGER NOT NULL, exhausted INTEGER NOT NULL, "
                "budget_id TEXT NOT NULL, attempts INTEGER NOT NULL, window_start_us INTEGER NOT NULL, "
                "next_attempt_us INTEGER NOT NULL, last_control_us INTEGER NOT NULL, "
                "max_attempts INTEGER NOT NULL, window_us INTEGER NOT NULL, backoff_us INTEGER NOT NULL)")


class OperationsOwnershipError(RuntimeError):
    pass


def _require(value, reason):
    if not value:
        raise OperationsOwnershipError(reason)


@dataclass(frozen=True, slots=True)
class RestartProfile:
    max_attempts: int
    window_us: int
    backoff_us: int

    def __post_init__(self):
        _require(type(self.max_attempts) is int and 1 <= self.max_attempts <= 10000
            and type(self.window_us) is int and 1 <= self.window_us <= 86400000000
            and type(self.backoff_us) is int and 0 <= self.backoff_us <= self.window_us,
            "OPERATIONS_FINITE_RESTART_PROFILE_REQUIRED")


@dataclass(frozen=True, slots=True)
class OwnerFence:
    domain_id: str
    binding_digest: str
    generation: int
    process_identity: str
    nonce: str


class OperationsStore:
    """Explicit initialization; reopen never acquires ownership or resets state.

    Initial acquire and each replacement consume one attempt. A nonexhausted
    window can roll over; reaching the limit denies/latches the next attempt.
    Exhaustion never expires. Only operator_reset with current generation CAS
    creates a new budget identity and clears the Operations stop/exhaustion.
    It cannot access Authority policy, hard-stop, or grants.
    """
    def __init__(self, path, domain):
        self.path = _journal_path(path)
        _require(type(domain) is LedgerDomain and domain.mode == "LIVE", "OPERATIONS_LIVE_DOMAIN_REQUIRED")
        self.domain_id, self.binding_digest = domain.economic_domain_id, domain.binding_digest
        self._file_identity = (self.path.stat().st_dev, self.path.stat().st_ino)
        with self._connection() as conn:
            self._read(conn)

    @classmethod
    def initialize(cls, path, domain, profile, *, now_us):
        _require(type(profile) is RestartProfile, "OPERATIONS_RESTART_PROFILE_REQUIRED")
        _require(type(domain) is LedgerDomain and domain.mode == "LIVE", "OPERATIONS_LIVE_DOMAIN_REQUIRED")
        cls._time(now_us)
        path = _journal_path(path)
        # Exclusive file creation distinguishes initialization from reopen.
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        os.close(fd)
        with closing(sqlite3.connect(path)) as conn:
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute(_DDL)
            conn.execute("INSERT INTO operations VALUES (1,?,?,?,0,NULL,NULL,0,0,?,0,?,?,?, ?,?,?)",
                (VERSION, domain.economic_domain_id, domain.binding_digest, uuid4().hex, now_us, now_us, now_us,
                 profile.max_attempts, profile.window_us, profile.backoff_us))
            conn.commit()
        return cls(path, domain)

    @staticmethod
    def _time(now_us):
        _require(type(now_us) is int and 0 <= now_us <= 9000000000000000,
            "OPERATIONS_FINITE_CLOCK_REQUIRED")

    @contextmanager
    def _connection(self):
        _require(_journal_path(self.path) == self.path and self.path.is_file(),
            "OPERATIONS_EXISTING_STORE_REQUIRED")
        stat = self.path.stat()
        _require((stat.st_dev, stat.st_ino) == self._file_identity, "OPERATIONS_STORE_REPLACED")
        with closing(sqlite3.connect(self.path.as_uri()+"?mode=rw", uri=True, timeout=0,
                                     isolation_level=None)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA synchronous=FULL")
            yield conn

    def _read(self, conn):
        schema_objects = conn.execute("SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").fetchall()
        _require(len(schema_objects) == 1 and schema_objects[0]["type"] == "table"
            and schema_objects[0]["name"] == "operations"
            and " ".join(schema_objects[0]["sql"].split()) == " ".join(_DDL.split()),
            "OPERATIONS_EXACT_SCHEMA_REQUIRED")
        expected = ("singleton", "version", "domain_id", "binding_digest", "generation", "process_identity",
            "nonce", "stopped", "exhausted", "budget_id", "attempts", "window_start_us", "next_attempt_us",
            "last_control_us", "max_attempts", "window_us", "backoff_us")
        schema = conn.execute("PRAGMA table_info(operations)").fetchall()
        _require(tuple(row["name"] for row in schema) == expected
            and tuple(row["type"] for row in schema) == ("INTEGER", "TEXT", "TEXT", "TEXT", "INTEGER",
                "TEXT", "TEXT", "INTEGER", "INTEGER", "TEXT", "INTEGER", "INTEGER", "INTEGER", "INTEGER",
                "INTEGER", "INTEGER", "INTEGER")
            and tuple(row["pk"] for row in schema) == (1,) + (0,)*16,
            "OPERATIONS_SCHEMA_CONFLICT")
        rows = conn.execute("SELECT * FROM operations").fetchall()
        _require(len(rows) == 1, "OPERATIONS_SINGLE_CONTROL_REQUIRED")
        state = dict(rows[0])
        _require(state["version"] == VERSION and state["domain_id"] == self.domain_id
            and state["binding_digest"] == self.binding_digest,
            "OPERATIONS_STORE_IDENTITY_CONFLICT")
        RestartProfile(state["max_attempts"], state["window_us"], state["backoff_us"])
        _require(state["singleton"] == 1 and type(state["generation"]) is int
            and 0 <= state["generation"] < 9223372036854775806
            and type(state["attempts"]) is int and 0 <= state["attempts"] <= state["max_attempts"]
            and all(type(state[k]) is int and state[k] in (0, 1) for k in ("stopped", "exhausted"))
            and type(state["budget_id"]) is str and re.fullmatch("[0-9a-f]{32}", state["budget_id"]),
            "OPERATIONS_CONTROL_STATE_INVALID")
        for key in ("window_start_us", "next_attempt_us", "last_control_us"):
            self._time(state[key])
        _require(state["window_start_us"] <= state["last_control_us"]
            and state["window_start_us"] <= state["next_attempt_us"]
            and state["next_attempt_us"] <= state["last_control_us"] + state["backoff_us"]
            and (not state["exhausted"] or state["attempts"] == state["max_attempts"]),
            "OPERATIONS_RESTART_FACTS_INVALID")
        owner = state["nonce"]
        _require((owner is None and state["process_identity"] is None)
            or (type(owner) is str and re.fullmatch("[0-9a-f]{32}", owner)
                and type(state["process_identity"]) is str and 1 <= len(state["process_identity"]) <= 256
                and state["generation"] > 0 and state["attempts"] > 0
                and not state["stopped"] and not state["exhausted"]), "OPERATIONS_OWNER_FACTS_INVALID")
        return state

    def snapshot(self):
        """Safe audit facts only; this result is never a mutation capability."""
        with self._connection() as conn:
            return self._read(conn)

    @contextmanager
    def _control(self, now_us):
        self._time(now_us)
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                state = self._read(conn)
                _require(now_us >= state["last_control_us"], "OPERATIONS_CLOCK_REGRESSION")
                yield conn, state
                conn.execute("UPDATE operations SET last_control_us=?", (now_us,))
                self._read(conn)
                conn.commit()
            finally:
                if conn.in_transaction:
                    conn.rollback()

    def acquire(self, process_identity, *, now_us, replace_generation=None):
        _require(type(process_identity) is str and 1 <= len(process_identity) <= 256,
            "OPERATIONS_PROCESS_IDENTITY_REQUIRED")
        denied = False
        with self._control(now_us) as (conn, state):
            _require(not state["stopped"], "OPERATIONS_OPERATOR_STOP_LATCHED")
            _require(not state["exhausted"], "OPERATIONS_RESTART_EXHAUSTED")
            if replace_generation is None:
                _require(state["nonce"] is None, "OPERATIONS_ALREADY_OWNED")
            else:
                _require(type(replace_generation) is int and replace_generation == state["generation"],
                    "OPERATIONS_REPLACEMENT_GENERATION_CONFLICT")
            _require(now_us >= state["next_attempt_us"], "OPERATIONS_RESTART_BACKOFF")
            attempts, start = state["attempts"], state["window_start_us"]
            # Exhaustion is evaluated before time can replenish this budget.
            if attempts >= state["max_attempts"]:
                conn.execute("UPDATE operations SET exhausted=1, generation=generation+1, "
                    "process_identity=NULL, nonce=NULL")
                denied = True
            else:
                if now_us >= start + state["window_us"]:
                    attempts, start = 0, now_us
                fence = OwnerFence(self.domain_id, self.binding_digest, state["generation"]+1, process_identity, uuid4().hex)
                conn.execute("UPDATE operations SET generation=?, process_identity=?, nonce=?, attempts=?, "
                    "window_start_us=?, next_attempt_us=?", (fence.generation, fence.process_identity,
                    fence.nonce, attempts+1, start, now_us+state["backoff_us"]))
        _require(not denied, "OPERATIONS_RESTART_EXHAUSTED")
        return OperationsOwnership(self, fence, _token=_TOKEN)

    def operator_stop(self, *, now_us):
        with self._control(now_us) as (conn, state):
            conn.execute("UPDATE operations SET stopped=1, generation=generation+1, "
                "process_identity=NULL, nonce=NULL")

    def operator_reset(self, *, expected_generation, now_us):
        """Explicit operator boundary, never a child restart operation."""
        with self._control(now_us) as (conn, state):
            _require(type(expected_generation) is int and state["generation"] == expected_generation,
                "OPERATIONS_OPERATOR_GENERATION_CONFLICT")
            conn.execute("UPDATE operations SET generation=generation+1, process_identity=NULL, nonce=NULL, "
                "stopped=0, exhausted=0, budget_id=?, attempts=0, window_start_us=?, next_attempt_us=?",
                (uuid4().hex, now_us, now_us))


_TOKEN = object()


class OperationsOwnership:
    """Process-local exact identity; reconstructing store facts cannot mint it."""
    __slots__ = ("_store", "fence", "_pid", "_closed")

    def __init__(self, store, fence, *, _token):
        _require(_token is _TOKEN, "OPERATIONS_FRESH_ACQUISITION_REQUIRED")
        object.__setattr__(self, "_store", store)
        object.__setattr__(self, "fence", fence)
        object.__setattr__(self, "_pid", os.getpid())
        object.__setattr__(self, "_closed", False)

    def __setattr__(self, name, value):
        raise AttributeError("immutable Operations owner")

    def __reduce_ex__(self, protocol):
        raise TypeError("Operations ownership cannot be serialized")

    def close(self):
        """Drop this process capability; reacquisition remains explicit and budgeted."""
        object.__setattr__(self, "_closed", True)

    @contextmanager
    def mutation_guard(self, domain):
        _require(not self._closed and os.getpid() == self._pid and type(domain) is LedgerDomain
            and domain.economic_domain_id == self.fence.domain_id
            and domain.binding_digest == self.fence.binding_digest,
            "OPERATIONS_PROCESS_OR_DOMAIN_CHANGED")
        with self._store._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                state = self._store._read(conn)
                _require(not state["stopped"] and not state["exhausted"], "OPERATIONS_MUTATION_STOPPED")
                _require(all(state[k] == v for k, v in asdict(self.fence).items()), "OPERATIONS_STALE_OWNER")
                yield
            finally:
                if conn.in_transaction:
                    conn.rollback()


def mutation_guard(ownership, domain):
    """Additive accepted component hook; Operations roots supply exact owners."""
    if ownership is None:
        return nullcontext()
    _require(type(ownership) is OperationsOwnership, "OPERATIONS_EXACT_OWNER_REQUIRED")
    return ownership.mutation_guard(domain)
