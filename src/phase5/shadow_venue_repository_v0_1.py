from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

from .shadow_domain_v0_1 import (
    ShadowDeterminismConflict,
    ShadowState,
    canonical_json,
    content_fingerprint,
    deterministic_id,
)
from .shadow_repository_v0_1 import DEFAULT_SHADOW_DATABASE_PATH, PROJECT_ROOT
from .shadow_venue_route_quote_v0_1 import (
    MODEL_FINGERPRINT,
    MODEL_ID,
    QUOTE_SCHEMA_VERSION,
    ROUTE_SCHEMA_VERSION,
    SCHEMA_VERSION,
    STATE_SCHEMA_VERSION,
    ExecutableQuoteV01,
    PumpBondingCurveStateV01,
    PumpSwapStateV01,
    RouteDecisionV01,
)


_OPEN_TOKEN = object()


def _is_within(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _validate_database_path(path: Path) -> Path:
    resolved = path.resolve()
    protected = (
        (PROJECT_ROOT / "data" / "paper").resolve(),
        (PROJECT_ROOT / "data" / "db").resolve(),
    )
    if any(_is_within(resolved, root) for root in protected):
        raise ValueError("T002 repository cannot use a Phase-4/source data path")
    return resolved


def _create_schema(conn: sqlite3.Connection) -> None:
    if conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='shadow_execution_intents'"
    ).fetchone() is None:
        raise ShadowDeterminismConflict("T001 shadow schema must exist before T002")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS shadow_t002_schema_meta (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            model_id TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            model_fingerprint TEXT NOT NULL,
            state_schema_version TEXT NOT NULL,
            route_schema_version TEXT NOT NULL,
            quote_schema_version TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS shadow_t002_venue_states (
            state_id TEXT PRIMARY KEY,
            intent_id TEXT NOT NULL,
            venue TEXT NOT NULL CHECK(venue IN ('PUMP_BONDING_CURVE','PUMPSWAP_CANONICAL')),
            slot_min INTEGER NOT NULL CHECK(slot_min >= 0),
            slot_max INTEGER NOT NULL CHECK(slot_max >= slot_min),
            content_fingerprint TEXT NOT NULL,
            state_json TEXT NOT NULL,
            FOREIGN KEY(intent_id) REFERENCES shadow_execution_intents(intent_id)
        );

        CREATE TABLE IF NOT EXISTS shadow_t002_routes (
            route_id TEXT PRIMARY KEY,
            intent_id TEXT NOT NULL,
            selected_state_id TEXT,
            outcome TEXT NOT NULL,
            content_fingerprint TEXT NOT NULL,
            route_json TEXT NOT NULL,
            FOREIGN KEY(intent_id) REFERENCES shadow_execution_intents(intent_id),
            FOREIGN KEY(selected_state_id) REFERENCES shadow_t002_venue_states(state_id)
        );

        CREATE TABLE IF NOT EXISTS shadow_t002_quotes (
            quote_id TEXT PRIMARY KEY,
            intent_id TEXT NOT NULL,
            state_id TEXT NOT NULL,
            route_id TEXT NOT NULL,
            content_fingerprint TEXT NOT NULL,
            quote_json TEXT NOT NULL,
            FOREIGN KEY(intent_id) REFERENCES shadow_execution_intents(intent_id),
            FOREIGN KEY(state_id) REFERENCES shadow_t002_venue_states(state_id),
            FOREIGN KEY(route_id) REFERENCES shadow_t002_routes(route_id)
        );

        CREATE INDEX IF NOT EXISTS idx_shadow_t002_state_intent
            ON shadow_t002_venue_states(intent_id, state_id);
        CREATE INDEX IF NOT EXISTS idx_shadow_t002_route_intent
            ON shadow_t002_routes(intent_id, route_id);
        CREATE INDEX IF NOT EXISTS idx_shadow_t002_quote_intent
            ON shadow_t002_quotes(intent_id, quote_id);

        CREATE TRIGGER IF NOT EXISTS shadow_t002_states_no_update
        BEFORE UPDATE ON shadow_t002_venue_states BEGIN
            SELECT RAISE(ABORT, 'T002 venue state is immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS shadow_t002_states_no_delete
        BEFORE DELETE ON shadow_t002_venue_states BEGIN
            SELECT RAISE(ABORT, 'T002 venue state is append-only');
        END;
        CREATE TRIGGER IF NOT EXISTS shadow_t002_routes_no_update
        BEFORE UPDATE ON shadow_t002_routes BEGIN
            SELECT RAISE(ABORT, 'T002 route is immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS shadow_t002_routes_no_delete
        BEFORE DELETE ON shadow_t002_routes BEGIN
            SELECT RAISE(ABORT, 'T002 route is append-only');
        END;
        CREATE TRIGGER IF NOT EXISTS shadow_t002_quotes_no_update
        BEFORE UPDATE ON shadow_t002_quotes BEGIN
            SELECT RAISE(ABORT, 'T002 quote is immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS shadow_t002_quotes_no_delete
        BEFORE DELETE ON shadow_t002_quotes BEGIN
            SELECT RAISE(ABORT, 'T002 quote is append-only');
        END;
        """
    )
    expected = (
        MODEL_ID,
        SCHEMA_VERSION,
        MODEL_FINGERPRINT,
        STATE_SCHEMA_VERSION,
        ROUTE_SCHEMA_VERSION,
        QUOTE_SCHEMA_VERSION,
    )
    row = conn.execute(
        "SELECT model_id,schema_version,model_fingerprint,state_schema_version,"
        "route_schema_version,quote_schema_version FROM shadow_t002_schema_meta "
        "WHERE singleton=1"
    ).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO shadow_t002_schema_meta VALUES(1,?,?,?,?,?,?)", expected
        )
    elif tuple(row) != expected:
        raise ShadowDeterminismConflict("T002 repository contract mismatch")
    conn.commit()


def open_venue_repository(
    path: str | Path = DEFAULT_SHADOW_DATABASE_PATH,
) -> ShadowVenueRepositoryV01:
    resolved = _validate_database_path(Path(path))
    if not resolved.exists():
        raise FileNotFoundError("open the T001 shadow repository first")
    conn = sqlite3.connect(resolved, timeout=5.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        mode = str(conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower()
        conn.execute("PRAGMA synchronous=FULL")
        if (
            mode != "wal"
            or int(conn.execute("PRAGMA synchronous").fetchone()[0]) != 2
            or int(conn.execute("PRAGMA foreign_keys").fetchone()[0]) != 1
        ):
            raise RuntimeError("T002 repository requires WAL/FULL/foreign keys")
        _create_schema(conn)
        return ShadowVenueRepositoryV01(conn, resolved, _open_token=_OPEN_TOKEN)
    except BaseException:
        conn.close()
        raise


class ShadowVenueRepositoryV01:
    def __init__(
        self,
        conn: sqlite3.Connection,
        database_path: Path,
        *,
        _open_token: object,
    ) -> None:
        if _open_token is not _OPEN_TOKEN:
            raise TypeError("use open_venue_repository()")
        resolved = _validate_database_path(database_path)
        main = [row for row in conn.execute("PRAGMA database_list") if str(row[1]) == "main"]
        if len(main) != 1 or Path(str(main[0][2])).resolve() != resolved:
            raise ValueError("T002 repository connection/path mismatch")
        self._conn = conn
        self.database_path = resolved
        self._closed = False

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("T002 repository is closed")

    def _intent_state(self, intent_id: str) -> ShadowState:
        row = self._conn.execute(
            "SELECT current_state FROM shadow_state_machines WHERE intent_id=?",
            (intent_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown execution intent: {intent_id}")
        return ShadowState(str(row[0]))

    def persist_state(
        self, state: PumpBondingCurveStateV01 | PumpSwapStateV01
    ) -> str:
        self._require_open()
        if not isinstance(state, (PumpBondingCurveStateV01, PumpSwapStateV01)):
            raise TypeError("state must be a T002 immutable venue state")
        if self._intent_state(state.intent_id) not in (
            ShadowState.ELIGIBILITY_CHECKED,
            ShadowState.ROUTE_BOUND,
            ShadowState.QUOTE_BOUND,
        ):
            raise ShadowDeterminismConflict(
                "venue state requires ELIGIBILITY_CHECKED or later T002 state"
            )
        payload = canonical_json(state.payload())
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                "SELECT content_fingerprint,state_json FROM shadow_t002_venue_states "
                "WHERE state_id=?", (state.state_id,),
            ).fetchone()
            if row is not None:
                if (str(row[0]), str(row[1])) != (state.fingerprint, payload):
                    raise ShadowDeterminismConflict("venue-state replay conflict")
                return state.state_id
            self._conn.execute(
                "INSERT INTO shadow_t002_venue_states VALUES(?,?,?,?,?,?,?)",
                (
                    state.state_id, state.intent_id, state.venue.value, state.slot_min,
                    state.slot_max, state.fingerprint, payload,
                ),
            )
        return state.state_id

    def persist_route(self, route: RouteDecisionV01) -> str:
        self._require_open()
        if not isinstance(route, RouteDecisionV01):
            raise TypeError("route must be RouteDecisionV01")
        current = self._intent_state(route.intent_id)
        if route.executable and current is not ShadowState.ROUTE_BOUND:
            raise ShadowDeterminismConflict("executable route requires ROUTE_BOUND prerequisite")
        if not route.executable and current not in (
            ShadowState.REJECTED,
            ShadowState.EXPIRED,
            ShadowState.FAILED,
        ):
            raise ShadowDeterminismConflict("fail-closed route requires terminal prerequisite")
        payload = canonical_json(route.payload())
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                "SELECT content_fingerprint,route_json FROM shadow_t002_routes WHERE route_id=?",
                (route.route_id,),
            ).fetchone()
            if row is not None:
                if (str(row[0]), str(row[1])) != (route.fingerprint, payload):
                    raise ShadowDeterminismConflict("route replay conflict")
                return route.route_id
            persisted_fingerprints = {
                str(row[0])
                for row in self._conn.execute(
                    "SELECT content_fingerprint FROM shadow_t002_venue_states "
                    "WHERE intent_id=?",
                    (route.intent_id,),
                )
            }
            if not set(route.state_fingerprints).issubset(persisted_fingerprints):
                raise ShadowDeterminismConflict(
                    "route references unpersisted venue-state evidence"
                )
            if route.selected_state_id is not None:
                state_row = self._conn.execute(
                    "SELECT intent_id,content_fingerprint FROM shadow_t002_venue_states "
                    "WHERE state_id=?", (route.selected_state_id,),
                ).fetchone()
                if (
                    state_row is None
                    or str(state_row[0]) != route.intent_id
                    or str(state_row[1]) not in route.state_fingerprints
                ):
                    raise ShadowDeterminismConflict("route lacks exact persisted state lineage")
            self._conn.execute(
                "INSERT INTO shadow_t002_routes VALUES(?,?,?,?,?,?)",
                (
                    route.route_id, route.intent_id, route.selected_state_id,
                    route.outcome.value, route.fingerprint, payload,
                ),
            )
        return route.route_id

    def persist_quote(self, quote: ExecutableQuoteV01) -> str:
        self._require_open()
        if not isinstance(quote, ExecutableQuoteV01):
            raise TypeError("quote must be ExecutableQuoteV01")
        if self._intent_state(quote.intent_id) is not ShadowState.QUOTE_BOUND:
            raise ShadowDeterminismConflict("quote requires QUOTE_BOUND prerequisite")
        payload = canonical_json(quote.payload())
        with self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                "SELECT content_fingerprint,quote_json FROM shadow_t002_quotes WHERE quote_id=?",
                (quote.quote_id,),
            ).fetchone()
            if row is not None:
                if (str(row[0]), str(row[1])) != (quote.fingerprint, payload):
                    raise ShadowDeterminismConflict("quote replay conflict")
                return quote.quote_id
            intent = self._conn.execute(
                "SELECT content_fingerprint,intent_json FROM shadow_execution_intents "
                "WHERE intent_id=?", (quote.intent_id,),
            ).fetchone()
            state = self._conn.execute(
                "SELECT intent_id,content_fingerprint,state_json FROM shadow_t002_venue_states "
                "WHERE state_id=?", (quote.venue_state_id,),
            ).fetchone()
            route = self._conn.execute(
                "SELECT intent_id,selected_state_id,content_fingerprint,route_json "
                "FROM shadow_t002_routes WHERE route_id=?", (quote.route_id,),
            ).fetchone()
            if (
                intent is None
                or state is None
                or route is None
                or str(state[0]) != quote.intent_id
                or str(state[1]) != quote.venue_state_fingerprint
                or str(route[0]) != quote.intent_id
                or str(route[1]) != quote.venue_state_id
                or str(route[2]) != quote.route_fingerprint
            ):
                raise ShadowDeterminismConflict(
                    "quote lacks exact intent/state/route lineage"
                )

            intent_payload = json.loads(str(intent[1]))
            state_payload = json.loads(str(state[2]))
            route_payload = json.loads(str(route[3]))

            fee_config_payload = state_payload.get("fee_config")
            if not isinstance(fee_config_payload, dict):
                raise ShadowDeterminismConflict(
                    "quote state lacks fee-config evidence"
                )

            expected_fee_fingerprint = content_fingerprint(fee_config_payload)
            expected_route_outcome = (
                "ROUTE_PUMP_BONDING_CURVE"
                if quote.venue.value == "PUMP_BONDING_CURVE"
                else "ROUTE_PUMPSWAP_CANONICAL"
            )

            if (
                str(intent[0]) != quote.intent_fingerprint
                or intent_payload.get("intent_id") != quote.intent_id
                or intent_payload.get("fingerprint") != quote.intent_fingerprint
                or intent_payload.get("side") != quote.side.value
                or intent_payload.get("input_amount_base_units") != quote.input_amount
                or state_payload.get("venue") != quote.venue.value
                or state_payload.get("slot_min") != quote.slot_min
                or state_payload.get("slot_max") != quote.slot_max
                or route_payload.get("outcome") != expected_route_outcome
                or quote.fees.fee_config_fingerprint != expected_fee_fingerprint
            ):
                raise ShadowDeterminismConflict(
                    "quote conflicts with exact intent/state/route/fee lineage"
                )
            self._conn.execute(
                "INSERT INTO shadow_t002_quotes VALUES(?,?,?,?,?,?)",
                (
                    quote.quote_id, quote.intent_id, quote.venue_state_id,
                    quote.route_id, quote.fingerprint, payload,
                ),
            )
        return quote.quote_id

    def get_json(self, table: str, identity: str) -> dict[str, Any] | None:
        self._require_open()
        allowed = {
            "state": ("shadow_t002_venue_states", "state_id", "state_json"),
            "route": ("shadow_t002_routes", "route_id", "route_json"),
            "quote": ("shadow_t002_quotes", "quote_id", "quote_json"),
        }
        if table not in allowed:
            raise ValueError("unknown T002 evidence table")
        table_name, id_column, json_column = allowed[table]
        row = self._conn.execute(
            f"SELECT {json_column} FROM {table_name} WHERE {id_column}=?", (identity,)
        ).fetchone()
        if row is None:
            return None
        value = json.loads(str(row[0]))
        if not isinstance(value, dict) or canonical_json(value) != str(row[0]):
            raise ShadowDeterminismConflict("persisted T002 JSON is noncanonical")
        return value

    def canonical_digest(self) -> str:
        self._require_open()
        self.audit()
        payload = {
            "model_id": MODEL_ID,
            "model_fingerprint": MODEL_FINGERPRINT,
            "states": [dict(row) for row in self._conn.execute(
                "SELECT * FROM shadow_t002_venue_states ORDER BY state_id"
            )],
            "routes": [dict(row) for row in self._conn.execute(
                "SELECT * FROM shadow_t002_routes ORDER BY route_id"
            )],
            "quotes": [dict(row) for row in self._conn.execute(
                "SELECT * FROM shadow_t002_quotes ORDER BY quote_id"
            )],
        }
        return hashlib.sha256(canonical_json(payload).encode()).hexdigest()

    def audit(self) -> bool:
        self._require_open()
        specifications = (
            (
                "shadow_t002_venue_states", "state_id", "state_json",
                "content_fingerprint", "P5VS", STATE_SCHEMA_VERSION,
            ),
            (
                "shadow_t002_routes", "route_id", "route_json",
                "content_fingerprint", "P5RT", ROUTE_SCHEMA_VERSION,
            ),
            (
                "shadow_t002_quotes", "quote_id", "quote_json",
                "content_fingerprint", "P5QT", QUOTE_SCHEMA_VERSION,
            ),
        )
        for table, identity_column, json_column, fingerprint_column, prefix, version in specifications:
            rows = self._conn.execute(
                f"SELECT {identity_column},{json_column},{fingerprint_column} FROM {table}"
            ).fetchall()
            for row in rows:
                raw_json = str(row[json_column])
                value = json.loads(raw_json)
                if not isinstance(value, dict) or canonical_json(value) != raw_json:
                    raise ShadowDeterminismConflict("persisted T002 JSON is noncanonical")
                fingerprint = content_fingerprint(value)
                if (
                    fingerprint != str(row[fingerprint_column])
                    or deterministic_id(prefix, version, fingerprint)
                    != str(row[identity_column])
                ):
                    raise ShadowDeterminismConflict("persisted T002 identity/fingerprint conflict")
        state_rows = self._conn.execute(
            "SELECT * FROM shadow_t002_venue_states"
        ).fetchall()
        for row in state_rows:
            value = json.loads(str(row["state_json"]))
            if (
                str(row["intent_id"]) != value.get("intent_id")
                or str(row["venue"]) != value.get("venue")
                or int(row["slot_min"]) != value.get("slot_min")
                or int(row["slot_max"]) != value.get("slot_max")
            ):
                raise ShadowDeterminismConflict("persisted venue-state columns conflict")
        route_rows = self._conn.execute(
            "SELECT * FROM shadow_t002_routes"
        ).fetchall()
        for row in route_rows:
            value = json.loads(str(row["route_json"]))
            if (
                str(row["intent_id"]) != value.get("intent_id")
                or str(row["outcome"]) != value.get("outcome")
                or row["selected_state_id"] != value.get("selected_state_id")
            ):
                raise ShadowDeterminismConflict("persisted route columns conflict")
            fingerprints = {
                str(item[0])
                for item in self._conn.execute(
                    "SELECT content_fingerprint FROM shadow_t002_venue_states "
                    "WHERE intent_id=?", (str(row["intent_id"]),)
                )
            }
            if not set(value.get("state_fingerprints", ())).issubset(fingerprints):
                raise ShadowDeterminismConflict("persisted route evidence is incomplete")
        quote_rows = self._conn.execute(
            "SELECT * FROM shadow_t002_quotes"
        ).fetchall()
        for row in quote_rows:
            value = json.loads(str(row["quote_json"]))
            if (
                str(row["intent_id"]) != value.get("intent_id")
                or str(row["state_id"]) != value.get("venue_state_id")
                or str(row["route_id"]) != value.get("route_id")
            ):
                raise ShadowDeterminismConflict("persisted quote columns conflict")
        lineage = self._conn.execute(
            "SELECT q.intent_id,q.state_id,q.route_id,s.intent_id,r.intent_id,"
            "r.selected_state_id FROM shadow_t002_quotes q "
            "JOIN shadow_t002_venue_states s ON s.state_id=q.state_id "
            "JOIN shadow_t002_routes r ON r.route_id=q.route_id"
        ).fetchall()
        if any(
            str(row[0]) != str(row[3])
            or str(row[0]) != str(row[4])
            or str(row[1]) != str(row[5])
            for row in lineage
        ):
            raise ShadowDeterminismConflict("persisted T002 lineage conflict")
        if self.foreign_key_violations():
            raise ShadowDeterminismConflict("persisted T002 foreign-key conflict")
        return True

    @property
    def foreign_keys_enabled(self) -> bool:
        self._require_open()
        return bool(self._conn.execute("PRAGMA foreign_keys").fetchone()[0])

    def foreign_key_violations(self) -> tuple[tuple[Any, ...], ...]:
        self._require_open()
        return tuple(tuple(row) for row in self._conn.execute("PRAGMA foreign_key_check"))

    def quick_check(self) -> str:
        self._require_open()
        return str(self._conn.execute("PRAGMA quick_check").fetchone()[0])

    def close(self) -> None:
        if not self._closed:
            self._conn.close()
            self._closed = True

    def __enter__(self) -> ShadowVenueRepositoryV01:
        self._require_open()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
