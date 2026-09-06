from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import tempfile
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence

from phase4.paper_continuous_market_source_v0_2 import (
    MAX_BATCH_SIZE,
    MODEL_FINGERPRINT as SOURCE_MODEL_FINGERPRINT,
    MODEL_ID as SOURCE_MODEL_ID,
    ContinuousMarketSourceRecordV02,
    ContinuousMarketSourceV02,
)
from phase4.paper_continuous_firstpullback_binding_v0_1 import (
    LOCKED_PARAMETER_SET_BY_ROLE,
    LOCKED_ROLE_ORDER,
    LOCKED_SELECTION_SHA256,
)
from phase4.paper_continuous_firstpullback_binding_v0_2 import (
    MODEL_FINGERPRINT as FIRSTPULLBACK_BINDING_FINGERPRINT,
)
from phase4.post24h_counterfactual_replay_v0_1 import (
    BASELINE_POLICIES,
    MODEL_FINGERPRINT as COUNTERFACTUAL_REPLAY_FINGERPRINT,
    CounterfactualExitPolicyV01,
    TimeoutOrigin,
    replay_counterfactual_v0_1,
)
from phase4.post24h_frozen_replay_v0_1 import (
    FrozenEntry,
    ReplayMismatch,
    canonical_sha256,
    sha256_file,
)


MODEL_ID = "P6-FROZEN-OOS-HYPOTHESIS-EVALUATION-0001"
SCHEMA_VERSION = "phase6_frozen_oos_hypothesis_evaluation_v0.1"
PROTOCOL_ID = "P6-OOS-PROTOCOL-0001"

MINIMUM_WINDOW_SECONDS = 72 * 60 * 60
H1_MINIMUM_ELIGIBLE_ENTRIES = 150
MINIMUM_FILL_RATE = Fraction(90, 100)
MINIMUM_PROFIT_FACTOR = Fraction(110, 100)
MINIMUM_NET_TO_DRAWDOWN = Fraction(50, 100)
MINIMUM_POSITIVE_DAY_FRACTION = Fraction(60, 100)

# Frozen from the accepted in-sample POST24H-T002B report. These values are
# protocol inputs, never estimates calculated from the OOS window.
ADVERSE_SLIPPAGE_QUARTILE_BOUNDARIES_BPS = {"p25": 0, "p50": 61, "p75": 585}

DIAGNOSTIC_CLASSIFICATION = "DIAGNOSTIC_ONLY_NON_CAUSAL_ENTRY_FILTER"
PROMOTION_CLASSIFICATION = "PROMOTION_CANDIDATE_CAUSAL"
MATCHED_CONTROL_CLASSIFICATION = "MATCHED_CAUSAL_CONTROL"
REFERENCE_CLASSIFICATION = "FROZEN_REFERENCE_ONLY"

ELIGIBILITY_ALL = "ALL_FROZEN_FIRSTPULLBACK_CANDIDATES"
ELIGIBILITY_H1_TIME = "CANDIDATE_SIGNAL_UTC_00_00_INCLUSIVE_06_00_EXCLUSIVE"
ELIGIBILITY_TIME_OR_Q4 = "H1_TIME_OR_ENTRY_ADVERSE_SLIPPAGE_Q4"
ELIGIBILITY_Q4 = "ENTRY_ADVERSE_SLIPPAGE_Q4"
ELIGIBILITY_EXCLUDE_Q3 = "EXCLUDE_ENTRY_ADVERSE_SLIPPAGE_Q3"
EXPECTED_POLICY_SET_SHA256 = "c4a2a277af1d1c7e1e8316e258b2a6da2abd302cd31d2498e4e7a45d08b2582c"

PROTOCOL_DEFINITION: dict[str, Any] = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "protocol_id": PROTOCOL_ID,
    "policy_set_sha256": EXPECTED_POLICY_SET_SHA256,
    "minimum_window_seconds": MINIMUM_WINDOW_SECONDS,
    "h1_minimum_eligible_entries": H1_MINIMUM_ELIGIBLE_ENTRIES,
    "minimum_fill_rate": "0.90",
    "minimum_profit_factor": "1.10",
    "minimum_net_to_max_realized_closed_equity_drawdown": "0.50",
    "exactly_three_complete_utc_days_minimum_positive_days": 2,
    "more_than_three_complete_utc_days_minimum_positive_fraction": "0.60",
    "primary_comparison": "H1_EXPECTANCY_GT_CTRL_ALL_TP10_T30_EXPECTANCY",
    "gate_arithmetic": "EXACT_INTEGER_FRACTIONS_WITH_12_DECIMAL_REPORTING",
    "net_pnl_scope": "REALIZED_CLOSED_TRADES_ONLY",
    "unresolved_pnl_treatment": "EXCLUDED_NOT_ZERO_FILLED_PNL",
    "zero_drawdown_rule": (
        "POSITIVE_NET_WITH_ZERO_DRAWDOWN_IS_INFINITE_AND_PASSES;"
        "NON_POSITIVE_NET_WITH_ZERO_DRAWDOWN_IS_ZERO_AND_FAILS_NET_GATE"
    ),
    "h1_eligibility_clock": "CandidateSignal.signal_observed_at UTC",
    "h1_utc_interval": "[00:00:00,06:00:00)",
    "h1_forbidden_inputs": [
        "entry_observed_at",
        "selected_market_observed_at",
        "fill_observed_at",
        "execution_ready_at",
        "entry_adverse_slippage_bps",
    ],
    "adverse_slippage_quartiles_frozen_in_sample_bps": (
        ADVERSE_SLIPPAGE_QUARTILE_BOUNDARIES_BPS
    ),
    "q4_rule": "entry_adverse_slippage_bps > p75",
    "h3_exclusion_rule": "exclude Q3: p50 < entry_adverse_slippage_bps <= p75",
    "firstpullback_strategy_version": "v1.1",
    "firstpullback_parameter_set_ids": sorted(LOCKED_PARAMETER_SET_BY_ROLE.values()),
    "firstpullback_selection_sha256": LOCKED_SELECTION_SHA256,
    "firstpullback_binding_fingerprint": FIRSTPULLBACK_BINDING_FINGERPRINT,
    "candidate_source_binding": (
        "context source_cursor/event_key/role and filled entry source lineage exact"
    ),
    "diagnostic_classification": DIAGNOSTIC_CLASSIFICATION,
    "diagnostic_promotion_allowed": False,
    "counterfactual_replay_fingerprint": COUNTERFACTUAL_REPLAY_FINGERPRINT,
    "source_model_id": SOURCE_MODEL_ID,
    "source_model_fingerprint": SOURCE_MODEL_FINGERPRINT,
    "source_connections": "SQLITE_URI_MODE_RO_AND_PRAGMA_QUERY_ONLY",
    "median_method": "NEAREST_RANK_CEILING_1_INDEXED",
    "output_scope": "SEPARATE_PHASE6_RESEARCH_PATH",
    "parameter_search": False,
    "adaptive_thresholds": False,
    "window_search": False,
}
MODEL_FINGERPRINT = canonical_sha256(PROTOCOL_DEFINITION)


class OOSBindingError(RuntimeError):
    """Raised when frozen OOS inputs do not match their declared binding."""


@dataclass(frozen=True, slots=True)
class FrozenOOSWindowV01:
    start_at: datetime
    end_at: datetime
    source_start_after_ingest_seq: int
    source_end_ingest_seq: int
    source_db_sha256: str
    source_model_id: str = SOURCE_MODEL_ID
    source_model_fingerprint: str = SOURCE_MODEL_FINGERPRINT
    protocol_fingerprint: str = MODEL_FINGERPRINT

    def __post_init__(self) -> None:
        _require_utc(self.start_at, "start_at")
        _require_utc(self.end_at, "end_at")
        if self.end_at <= self.start_at:
            raise ValueError("OOS end_at must be later than start_at")
        if self.duration_seconds < MINIMUM_WINDOW_SECONDS:
            raise ValueError("OOS window must contain at least 72 contiguous hours")
        if self.source_start_after_ingest_seq < 0:
            raise ValueError("source start cursor must be non-negative")
        if self.source_end_ingest_seq <= self.source_start_after_ingest_seq:
            raise ValueError("source end cursor must be greater than source start cursor")
        if not _is_sha256(self.source_db_sha256):
            raise ValueError("source_db_sha256 must be a lowercase SHA256")
        if self.source_model_id != SOURCE_MODEL_ID:
            raise OOSBindingError("source model ID differs from the accepted v0.2 source")
        if self.source_model_fingerprint != SOURCE_MODEL_FINGERPRINT:
            raise OOSBindingError("source model fingerprint mismatch")
        if self.protocol_fingerprint != MODEL_FINGERPRINT:
            raise OOSBindingError("Phase-6 protocol fingerprint mismatch")

    @property
    def duration_seconds(self) -> int:
        return int((self.end_at - self.start_at).total_seconds())

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "start_at": _iso(self.start_at),
            "end_at": _iso(self.end_at),
            "duration_seconds": self.duration_seconds,
            "source_start_after_ingest_seq": self.source_start_after_ingest_seq,
            "source_end_ingest_seq": self.source_end_ingest_seq,
            "source_db_sha256": self.source_db_sha256,
            "source_model_id": self.source_model_id,
            "source_model_fingerprint": self.source_model_fingerprint,
            "protocol_id": PROTOCOL_ID,
            "protocol_fingerprint": self.protocol_fingerprint,
            "evaluation_schema_version": SCHEMA_VERSION,
        }

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.canonical_payload())


@dataclass(frozen=True, slots=True)
class FrozenOOSCandidateV01:
    route_id: str
    signal_key: str
    mint: str
    strategy_version: str
    parameter_set_id: str
    role: str
    candidate_source_event_key: str
    signal_observed_at: str
    signal_ingest_seq: int
    reference_price_identity: str
    reference_price_numerator_raw: int
    reference_price_denominator_raw: int
    requested_size_lamports: int
    execution_ready_at: str
    entry_state: str
    state_reason: str
    selected_market_observed_at: str | None
    selected_market_ingest_seq: int | None
    selected_source_event_key: str | None
    simulated_entry_price_numerator_raw: int | None
    simulated_entry_price_denominator_raw: int | None
    total_entry_explicit_cost_lamports: int | None
    entry_adverse_slippage_bps: int | None

    def __post_init__(self) -> None:
        _parse_utc(self.signal_observed_at, "signal_observed_at")
        _parse_utc(self.execution_ready_at, "execution_ready_at")
        if self.entry_state not in {"ROUTED", "ENTRY_PENDING", "FILLED", "REJECTED"}:
            raise ValueError(f"unsupported entry state: {self.entry_state}")
        if self.signal_ingest_seq < 0 or self.requested_size_lamports <= 0:
            raise ValueError("invalid candidate sequence or requested size")
        if not self.route_id or not self.signal_key or not self.mint:
            raise ValueError("candidate identity fields are required")
        if self.strategy_version != "v1.1":
            raise OOSBindingError("candidate FirstPullback strategy version mismatch")
        if self.role not in LOCKED_ROLE_ORDER:
            raise OOSBindingError("candidate role is outside the locked FirstPullback selection")
        if self.parameter_set_id != LOCKED_PARAMETER_SET_BY_ROLE[self.role]:
            raise OOSBindingError("candidate parameter set does not match its locked role")
        if not self.candidate_source_event_key.endswith(f":CANDIDATE:{self.role}"):
            raise OOSBindingError("candidate source event does not match its locked role")
        if self.reference_price_numerator_raw <= 0 or self.reference_price_denominator_raw <= 0:
            raise ValueError("candidate rule-reference price must be positive")
        if self.entry_state == "FILLED":
            required = (
                self.selected_market_observed_at,
                self.selected_market_ingest_seq,
                self.selected_source_event_key,
                self.simulated_entry_price_numerator_raw,
                self.simulated_entry_price_denominator_raw,
                self.total_entry_explicit_cost_lamports,
                self.entry_adverse_slippage_bps,
            )
            if any(value is None for value in required):
                raise OOSBindingError("FILLED entry is missing immutable execution evidence")
            selected_at = _parse_utc(
                str(self.selected_market_observed_at), "selected_market_observed_at"
            )
            if int(self.selected_market_ingest_seq or -1) < 0:
                raise ValueError("filled entry source sequence must be non-negative")
            if int(self.simulated_entry_price_numerator_raw or 0) <= 0 or int(
                self.simulated_entry_price_denominator_raw or 0
            ) <= 0:
                raise ValueError("filled entry execution price must be positive")
            if (selected_at, int(self.selected_market_ingest_seq or -1)) <= (
                self.signal_at,
                self.signal_ingest_seq,
            ):
                raise OOSBindingError("filled entry evidence is not causal after CandidateSignal")

    @property
    def signal_at(self) -> datetime:
        return _parse_utc(self.signal_observed_at, "signal_observed_at")

    def frozen_entry(self) -> FrozenEntry:
        if self.entry_state != "FILLED":
            raise ValueError("only FILLED candidates have a replayable FrozenEntry")
        assert self.selected_market_observed_at is not None
        assert self.selected_market_ingest_seq is not None
        assert self.selected_source_event_key is not None
        assert self.simulated_entry_price_numerator_raw is not None
        assert self.simulated_entry_price_denominator_raw is not None
        assert self.total_entry_explicit_cost_lamports is not None
        return FrozenEntry(
            route_id=self.route_id,
            signal_key=self.signal_key,
            mint=self.mint,
            signal_observed_at=self.signal_observed_at,
            signal_ingest_seq=self.signal_ingest_seq,
            reference_price_identity=self.reference_price_identity,
            reference_price_numerator_raw=self.reference_price_numerator_raw,
            reference_price_denominator_raw=self.reference_price_denominator_raw,
            entry_observed_at=self.selected_market_observed_at,
            entry_ingest_seq=self.selected_market_ingest_seq,
            source_event_key=self.selected_source_event_key,
            principal_lamports=self.requested_size_lamports,
            entry_price_numerator_raw=self.simulated_entry_price_numerator_raw,
            entry_price_denominator_raw=self.simulated_entry_price_denominator_raw,
            entry_explicit_cost_lamports=self.total_entry_explicit_cost_lamports,
        )

    def canonical_payload(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class FrozenOOSPolicyV01:
    policy_id: str
    eligibility_rule: str
    exit_policy: CounterfactualExitPolicyV01
    classification: str
    promotion_eligible: bool

    def __post_init__(self) -> None:
        if self.exit_policy.policy_id != self.policy_id:
            raise ValueError("policy and exit-policy IDs must match")
        uses_outcome = self.eligibility_rule in {
            ELIGIBILITY_TIME_OR_Q4,
            ELIGIBILITY_Q4,
            ELIGIBILITY_EXCLUDE_Q3,
        }
        if uses_outcome and (
            self.classification != DIAGNOSTIC_CLASSIFICATION
            or self.promotion_eligible
        ):
            raise ValueError("execution-outcome entry filters must be non-promotable")
        if self.policy_id == "H1_SIMPLE_TIME_TP10_T30":
            if (
                self.eligibility_rule != ELIGIBILITY_H1_TIME
                or self.classification != PROMOTION_CLASSIFICATION
                or not self.promotion_eligible
                or self.exit_policy.timeout_origin is not TimeoutOrigin.ENTRY_FILL_CLOCK
                or self.exit_policy.timeout_ms != 30_000
                or self.exit_policy.tp_bps != 1_000
                or self.exit_policy.sl_bps is not None
                or self.exit_policy.trailing_activation_bps is not None
            ):
                raise ValueError("H1 eligibility/promotion contract drift")
        diagnostic_rules = {
            "H2_TIME_OR_Q4_TP20_T30": ELIGIBILITY_TIME_OR_Q4,
            "CTRL_Q4_TP20_T30": ELIGIBILITY_Q4,
            "H3_EXCLUSION_TRAIL10_GB3_T30": ELIGIBILITY_EXCLUDE_Q3,
        }
        if self.policy_id in diagnostic_rules and (
            self.eligibility_rule != diagnostic_rules[self.policy_id]
            or self.classification != DIAGNOSTIC_CLASSIFICATION
            or self.promotion_eligible
        ):
            raise ValueError("named diagnostic policy contract drift")

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "eligibility_rule": self.eligibility_rule,
            "exit": self.exit_policy.canonical_payload(),
            "classification": self.classification,
            "promotion_eligible": self.promotion_eligible,
        }

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.canonical_payload())


def _exit_policy(
    policy_id: str,
    *,
    tp_bps: int | None = None,
    trailing_activation_bps: int | None = None,
    trailing_giveback_bps: int | None = None,
) -> CounterfactualExitPolicyV01:
    return CounterfactualExitPolicyV01(
        policy_id=policy_id,
        timeout_ms=30_000,
        timeout_origin=TimeoutOrigin.ENTRY_FILL_CLOCK,
        tp_bps=tp_bps,
        trailing_activation_bps=trailing_activation_bps,
        trailing_giveback_bps=trailing_giveback_bps,
    )


POLICIES: Mapping[str, FrozenOOSPolicyV01] = MappingProxyType({
    "H1_SIMPLE_TIME_TP10_T30": FrozenOOSPolicyV01(
        policy_id="H1_SIMPLE_TIME_TP10_T30",
        eligibility_rule=ELIGIBILITY_H1_TIME,
        exit_policy=_exit_policy("H1_SIMPLE_TIME_TP10_T30", tp_bps=1_000),
        classification=PROMOTION_CLASSIFICATION,
        promotion_eligible=True,
    ),
    "CTRL_ALL_TP10_T30": FrozenOOSPolicyV01(
        policy_id="CTRL_ALL_TP10_T30",
        eligibility_rule=ELIGIBILITY_ALL,
        exit_policy=_exit_policy("CTRL_ALL_TP10_T30", tp_bps=1_000),
        classification=MATCHED_CONTROL_CLASSIFICATION,
        promotion_eligible=False,
    ),
    "CTRL_ALL_TP20_T30": FrozenOOSPolicyV01(
        policy_id="CTRL_ALL_TP20_T30",
        eligibility_rule=ELIGIBILITY_ALL,
        exit_policy=_exit_policy("CTRL_ALL_TP20_T30", tp_bps=2_000),
        classification=REFERENCE_CLASSIFICATION,
        promotion_eligible=False,
    ),
    "H2_TIME_OR_Q4_TP20_T30": FrozenOOSPolicyV01(
        policy_id="H2_TIME_OR_Q4_TP20_T30",
        eligibility_rule=ELIGIBILITY_TIME_OR_Q4,
        exit_policy=_exit_policy("H2_TIME_OR_Q4_TP20_T30", tp_bps=2_000),
        classification=DIAGNOSTIC_CLASSIFICATION,
        promotion_eligible=False,
    ),
    "CTRL_Q4_TP20_T30": FrozenOOSPolicyV01(
        policy_id="CTRL_Q4_TP20_T30",
        eligibility_rule=ELIGIBILITY_Q4,
        exit_policy=_exit_policy("CTRL_Q4_TP20_T30", tp_bps=2_000),
        classification=DIAGNOSTIC_CLASSIFICATION,
        promotion_eligible=False,
    ),
    "H3_EXCLUSION_TRAIL10_GB3_T30": FrozenOOSPolicyV01(
        policy_id="H3_EXCLUSION_TRAIL10_GB3_T30",
        eligibility_rule=ELIGIBILITY_EXCLUDE_Q3,
        exit_policy=_exit_policy(
            "H3_EXCLUSION_TRAIL10_GB3_T30",
            trailing_activation_bps=1_000,
            trailing_giveback_bps=300,
        ),
        classification=DIAGNOSTIC_CLASSIFICATION,
        promotion_eligible=False,
    ),
    "FINAL-A": FrozenOOSPolicyV01(
        policy_id="FINAL-A",
        eligibility_rule=ELIGIBILITY_ALL,
        exit_policy=BASELINE_POLICIES["FINAL-A"],
        classification=REFERENCE_CLASSIFICATION,
        promotion_eligible=False,
    ),
    "FINAL-B": FrozenOOSPolicyV01(
        policy_id="FINAL-B",
        eligibility_rule=ELIGIBILITY_ALL,
        exit_policy=BASELINE_POLICIES["FINAL-B"],
        classification=REFERENCE_CLASSIFICATION,
        promotion_eligible=False,
    ),
})


def _require_utc(value: datetime, label: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{label} must be timezone-aware UTC")


def _parse_utc(value: str, label: str) -> datetime:
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid {label}: {value}") from exc
    _require_utc(result, label)
    return result


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    _require_utc(value, "timestamp")
    return value.isoformat(timespec="microseconds")


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def _primitive(value: Any) -> Any:
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, Mapping):
        return {str(key): _primitive(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_primitive(item) for item in value]
    return value


def policy_definitions_v0_1() -> list[dict[str, Any]]:
    return [
        {**POLICIES[policy_id].canonical_payload(), "fingerprint": POLICIES[policy_id].fingerprint}
        for policy_id in sorted(POLICIES)
    ]


def _verify_frozen_policy_registry_v0_1() -> None:
    actual = canonical_sha256(policy_definitions_v0_1())
    if actual != EXPECTED_POLICY_SET_SHA256:
        raise OOSBindingError(
            f"frozen Phase-6 policy set drift: {actual} != {EXPECTED_POLICY_SET_SHA256}"
        )
    h1_exit = POLICIES["H1_SIMPLE_TIME_TP10_T30"].exit_policy.canonical_payload().copy()
    control_exit = POLICIES["CTRL_ALL_TP10_T30"].exit_policy.canonical_payload().copy()
    h1_exit.pop("policy_id")
    control_exit.pop("policy_id")
    if h1_exit != control_exit:
        raise OOSBindingError("H1 and matched control exit contracts differ")


_verify_frozen_policy_registry_v0_1()


def h1_time_eligible_v0_1(signal_observed_at: str | datetime) -> bool:
    observed = (
        _parse_utc(signal_observed_at, "signal_observed_at")
        if isinstance(signal_observed_at, str)
        else signal_observed_at
    )
    _require_utc(observed, "signal_observed_at")
    return time(0, 0, 0) <= observed.time().replace(tzinfo=None) < time(6, 0, 0)


def _adverse_quartile(value: int | None) -> str | None:
    if value is None:
        return None
    if value <= ADVERSE_SLIPPAGE_QUARTILE_BOUNDARIES_BPS["p25"]:
        return "Q1"
    if value <= ADVERSE_SLIPPAGE_QUARTILE_BOUNDARIES_BPS["p50"]:
        return "Q2"
    if value <= ADVERSE_SLIPPAGE_QUARTILE_BOUNDARIES_BPS["p75"]:
        return "Q3"
    return "Q4"


def candidate_is_eligible_v0_1(
    candidate: FrozenOOSCandidateV01,
    policy: FrozenOOSPolicyV01,
) -> bool:
    if policy.eligibility_rule == ELIGIBILITY_ALL:
        return True
    if policy.eligibility_rule == ELIGIBILITY_H1_TIME:
        return h1_time_eligible_v0_1(candidate.signal_observed_at)
    quartile = _adverse_quartile(candidate.entry_adverse_slippage_bps)
    if policy.eligibility_rule == ELIGIBILITY_TIME_OR_Q4:
        return h1_time_eligible_v0_1(candidate.signal_observed_at) or quartile == "Q4"
    if policy.eligibility_rule == ELIGIBILITY_Q4:
        return quartile == "Q4"
    if policy.eligibility_rule == ELIGIBILITY_EXCLUDE_Q3:
        return quartile is not None and quartile != "Q3"
    raise OOSBindingError(f"unknown frozen eligibility rule: {policy.eligibility_rule}")


def _open_readonly(path: str | Path) -> sqlite3.Connection:
    absolute = Path(path).resolve()
    if not absolute.is_file():
        raise FileNotFoundError(absolute)
    conn = sqlite3.connect(absolute.as_uri() + "?mode=ro", uri=True, timeout=2.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    row = conn.execute("PRAGMA query_only").fetchone()
    if row is None or int(row[0]) != 1:
        conn.close()
        raise OOSBindingError(f"SQLite query_only unavailable for {absolute}")
    return conn


def _required_columns(
    conn: sqlite3.Connection,
    table: str,
    expected: set[str],
) -> None:
    actual = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    missing = expected - actual
    if missing:
        raise OOSBindingError(f"{table} missing required columns: {sorted(missing)}")


def load_frozen_candidates_v0_1(
    paper_db: str | Path,
    window: FrozenOOSWindowV01,
) -> list[FrozenOOSCandidateV01]:
    conn = _open_readonly(paper_db)
    try:
        _required_columns(
            conn,
            "paper_continuous_signal_contexts_v0_1",
            {
                "signal_key", "route_id", "mint", "strategy_version",
                "parameter_set_id", "role", "source_cursor", "source_event_key",
            },
        )
        _required_columns(
            conn,
            "paper_entry_routes",
            {
                "route_id", "candidate_id", "mint", "strategy_version",
                "parameter_set_id", "signal_observed_at", "signal_ingest_seq",
                "reference_price_identity", "reference_price_numerator_raw",
                "reference_price_denominator_raw", "requested_size_lamports",
                "execution_ready_at", "state", "state_reason",
                "selected_market_observed_at", "selected_market_ingest_seq",
                "selected_source_event_key", "simulated_entry_price_numerator_raw",
                "simulated_entry_price_denominator_raw",
                "total_entry_explicit_cost_lamports", "adverse_slippage_bps",
            },
        )
        orphan_contexts = int(conn.execute(
            "SELECT COUNT(*) FROM paper_continuous_signal_contexts_v0_1 AS c "
            "LEFT JOIN paper_entry_routes AS r ON r.route_id=c.route_id "
            "WHERE r.route_id IS NULL"
        ).fetchone()[0])
        orphan_routes = int(conn.execute(
            "SELECT COUNT(*) FROM paper_entry_routes AS r "
            "LEFT JOIN paper_continuous_signal_contexts_v0_1 AS c ON c.route_id=r.route_id "
            "WHERE c.route_id IS NULL"
        ).fetchone()[0])
        if orphan_contexts or orphan_routes:
            raise OOSBindingError(
                "paper CandidateSignal/entry-route relation contains orphan rows"
            )
        rows = conn.execute(
            """
            SELECT c.signal_key AS context_signal_key,
                   c.route_id AS context_route_id,
                   c.mint AS context_mint,
                   c.strategy_version AS context_strategy_version,
                   c.parameter_set_id AS context_parameter_set_id,
                   c.role AS context_role,
                   c.source_cursor AS context_source_cursor,
                   c.source_event_key AS context_source_event_key,
                   r.*
            FROM paper_continuous_signal_contexts_v0_1 AS c
            JOIN paper_entry_routes AS r ON r.route_id=c.route_id
            ORDER BY r.signal_observed_at,r.signal_ingest_seq,c.signal_key
            """
        ).fetchall()
    finally:
        conn.close()
    candidates: list[FrozenOOSCandidateV01] = []
    seen: set[str] = set()
    for row in rows:
        signal_at = _parse_utc(str(row["signal_observed_at"]), "signal_observed_at")
        if not (window.start_at <= signal_at < window.end_at):
            continue
        if (
            row["context_signal_key"] != row["candidate_id"]
            or row["context_route_id"] != row["route_id"]
            or row["context_mint"] != row["mint"]
            or row["context_strategy_version"] != row["strategy_version"]
            or row["context_parameter_set_id"] != row["parameter_set_id"]
            or int(row["context_source_cursor"]) != int(row["signal_ingest_seq"])
        ):
            raise OOSBindingError("candidate context and entry-route lineage mismatch")
        signal_key = str(row["candidate_id"])
        if signal_key in seen:
            raise OOSBindingError(f"duplicate frozen CandidateSignal: {signal_key}")
        seen.add(signal_key)
        candidate = FrozenOOSCandidateV01(
            route_id=str(row["route_id"]),
            signal_key=signal_key,
            mint=str(row["mint"]),
            strategy_version=str(row["strategy_version"]),
            parameter_set_id=str(row["parameter_set_id"]),
            role=str(row["context_role"]),
            candidate_source_event_key=str(row["context_source_event_key"]),
            signal_observed_at=_iso(signal_at) or "",
            signal_ingest_seq=int(row["signal_ingest_seq"]),
            reference_price_identity=str(row["reference_price_identity"]),
            reference_price_numerator_raw=int(row["reference_price_numerator_raw"]),
            reference_price_denominator_raw=int(row["reference_price_denominator_raw"]),
            requested_size_lamports=int(row["requested_size_lamports"]),
            execution_ready_at=_iso(
                _parse_utc(str(row["execution_ready_at"]), "execution_ready_at")
            ) or "",
            entry_state=str(row["state"]),
            state_reason=str(row["state_reason"]),
            selected_market_observed_at=(
                None if row["selected_market_observed_at"] is None
                else _iso(_parse_utc(str(row["selected_market_observed_at"]), "selected_market_observed_at"))
            ),
            selected_market_ingest_seq=(
                None if row["selected_market_ingest_seq"] is None
                else int(row["selected_market_ingest_seq"])
            ),
            selected_source_event_key=(
                None if row["selected_source_event_key"] is None
                else str(row["selected_source_event_key"])
            ),
            simulated_entry_price_numerator_raw=(
                None if row["simulated_entry_price_numerator_raw"] is None
                else int(row["simulated_entry_price_numerator_raw"])
            ),
            simulated_entry_price_denominator_raw=(
                None if row["simulated_entry_price_denominator_raw"] is None
                else int(row["simulated_entry_price_denominator_raw"])
            ),
            total_entry_explicit_cost_lamports=(
                None if row["total_entry_explicit_cost_lamports"] is None
                else int(row["total_entry_explicit_cost_lamports"])
            ),
            entry_adverse_slippage_bps=(
                None if row["adverse_slippage_bps"] is None
                else int(row["adverse_slippage_bps"])
            ),
        )
        if not (
            window.source_start_after_ingest_seq
            < candidate.signal_ingest_seq
            <= window.source_end_ingest_seq
        ):
            raise OOSBindingError("CandidateSignal source cursor lies outside frozen window")
        if candidate.entry_state == "FILLED" and not (
            window.source_start_after_ingest_seq
            < int(candidate.selected_market_ingest_seq or -1)
            <= window.source_end_ingest_seq
        ):
            raise OOSBindingError("filled entry source cursor lies outside frozen window")
        candidates.append(candidate)
    return candidates


def load_frozen_market_paths_v0_1(
    source_db: str | Path,
    window: FrozenOOSWindowV01,
) -> tuple[dict[str, tuple[ContinuousMarketSourceRecordV02, ...]], dict[str, Any]]:
    actual_sha = sha256_file(source_db)
    if actual_sha != window.source_db_sha256:
        raise OOSBindingError(
            f"source database SHA256 mismatch: {actual_sha} != {window.source_db_sha256}"
        )
    source = ContinuousMarketSourceV02(
        source_db,
        start_after_p1_rowid=window.source_start_after_ingest_seq,
        database_identity=actual_sha,
    )
    cursor = window.source_start_after_ingest_seq
    records: list[ContinuousMarketSourceRecordV02] = []
    raw_rows = 0
    skips = 0
    while cursor < window.source_end_ingest_seq:
        # Count only rows through the frozen cursor before using the accepted
        # normalizer. This prevents even a discarded post-window row from
        # being inspected by the evaluator when rowid gaps exist.
        boundary = _open_readonly(source_db)
        try:
            bounded_count = int(boundary.execute(
                "SELECT COUNT(*) FROM (SELECT rowid FROM pump_events "
                "WHERE rowid>? AND rowid<=? ORDER BY rowid LIMIT ?)",
                (cursor, window.source_end_ingest_seq, MAX_BATCH_SIZE),
            ).fetchone()[0])
        finally:
            boundary.close()
        if bounded_count <= 0:
            raise OOSBindingError("source ended before the declared frozen end cursor")
        batch = source.fetch_batch(after_p1_rowid=cursor, batch_size=bounded_count)
        if batch.highest_fetched_p1_rowid > window.source_end_ingest_seq:
            raise OOSBindingError("source adapter inspected a row beyond frozen end cursor")
        raw_rows += batch.raw_rows_fetched
        skips += len(batch.skips)
        records.extend(batch.records)
        if batch.highest_fetched_p1_rowid <= cursor:
            raise OOSBindingError("source ended before the declared frozen end cursor")
        cursor = batch.highest_fetched_p1_rowid
    paths: dict[str, list[ContinuousMarketSourceRecordV02]] = {}
    for record in records:
        paths.setdefault(record.mint, []).append(record)
    frozen_paths = {
        mint: tuple(sorted(rows, key=lambda row: (row.observed_at, row.ingest_seq)))
        for mint, rows in sorted(paths.items())
    }
    source_rows_digest = canonical_sha256(
        [_primitive(asdict(record)) for record in sorted(records, key=lambda row: row.ingest_seq)]
    )
    return frozen_paths, {
        "source_db_sha256": actual_sha,
        "source_model_id": SOURCE_MODEL_ID,
        "source_model_fingerprint": SOURCE_MODEL_FINGERPRINT,
        "source_identity": source.source_identity,
        "connection_mode": "mode=ro; PRAGMA query_only=ON",
        "source_start_after_ingest_seq": window.source_start_after_ingest_seq,
        "source_end_ingest_seq": window.source_end_ingest_seq,
        "raw_rows_scanned": raw_rows,
        "normalized_records": len(records),
        "deterministic_skips": skips,
        "normalized_source_rows_sha256": source_rows_digest,
    }


def evaluate_policy_rows_v0_1(
    candidates: Sequence[FrozenOOSCandidateV01],
    paths: Mapping[str, tuple[Any, ...]],
    policy: FrozenOOSPolicyV01,
    *,
    frozen_watermark: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in sorted(candidates, key=lambda row: (row.signal_observed_at, row.signal_ingest_seq, row.signal_key)):
        if not candidate_is_eligible_v0_1(candidate, policy):
            continue
        base = {
            "policy_id": policy.policy_id,
            "policy_fingerprint": policy.fingerprint,
            "policy_classification": policy.classification,
            "promotion_eligible": policy.promotion_eligible,
            "eligibility_rule": policy.eligibility_rule,
            "eligibility_clock": "CandidateSignal.signal_observed_at UTC",
            "signal_key": candidate.signal_key,
            "mint": candidate.mint,
            "signal_observed_at": candidate.signal_observed_at,
            "signal_ingest_seq": candidate.signal_ingest_seq,
            "entry_state": candidate.entry_state,
            "entry_state_reason": candidate.state_reason,
            "entry_observed_at": candidate.selected_market_observed_at,
            "entry_adverse_slippage_bps": candidate.entry_adverse_slippage_bps,
            "attempted": True,
        }
        if candidate.entry_state != "FILLED":
            classification = (
                "ENTRY_REJECTED" if candidate.entry_state == "REJECTED" else "ENTRY_UNRESOLVED"
            )
            rows.append({
                **base,
                "classification": classification,
                "rejection_classification": candidate.state_reason,
                "exit_reason": None,
                "exit_requested_at": None,
                "exit_attempt_count": 0,
                "exit_fill_source_row": None,
                "exit_fill_observed_at": None,
                "gross_execution_pnl_lamports": None,
                "entry_explicit_cost_lamports": None,
                "exit_explicit_cost_lamports": None,
                "total_explicit_cost_lamports": None,
                "net_pnl_lamports": None,
                "holding_time_us": None,
            })
            continue
        entry = candidate.frozen_entry()
        result = replay_counterfactual_v0_1(
            entry,
            paths.get(candidate.mint, ()),
            policy.exit_policy,
            frozen_watermark=frozen_watermark,
        )
        outcome = result.execution
        classification = (
            "FILLED" if result.classification == "FILLED"
            else f"EXIT_{result.classification}"
        )
        rows.append({
            **base,
            "classification": classification,
            "rejection_classification": outcome.rejection_state,
            "exit_reason": result.intent.reason.value,
            "exit_requested_at": _iso(result.intent.requested_at),
            "exit_attempt_count": outcome.attempt_count,
            "exit_fill_source_row": outcome.fill_source_row,
            "exit_fill_observed_at": _iso(outcome.fill_observed_at),
            "gross_execution_pnl_lamports": outcome.gross_execution_pnl_lamports,
            "entry_explicit_cost_lamports": entry.entry_explicit_cost_lamports,
            "exit_explicit_cost_lamports": outcome.exit_explicit_cost_lamports,
            "total_explicit_cost_lamports": (
                entry.entry_explicit_cost_lamports
                + int(outcome.exit_explicit_cost_lamports or 0)
            ),
            "net_pnl_lamports": outcome.net_pnl_lamports,
            "holding_time_us": outcome.holding_time_us,
        })
    return rows


def validate_entry_source_lineage_v0_1(
    candidates: Sequence[FrozenOOSCandidateV01],
    paths: Mapping[str, tuple[Any, ...]],
) -> int:
    by_identity: dict[tuple[str, int], Any] = {}
    for mint, path in paths.items():
        for observation in path:
            key = (mint, int(observation.ingest_seq))
            prior = by_identity.get(key)
            if prior is not None and prior != observation:
                raise OOSBindingError("conflicting source records share mint/ingest identity")
            by_identity[key] = observation
    validated = 0
    for candidate in candidates:
        signal_observation = by_identity.get((candidate.mint, candidate.signal_ingest_seq))
        if signal_observation is None:
            raise OOSBindingError(
                f"CandidateSignal source lineage is missing for {candidate.signal_key}"
            )
        if (
            not candidate.candidate_source_event_key.startswith(
                f"{signal_observation.event_key}:CANDIDATE:"
            )
            or _iso(signal_observation.observed_at) != candidate.signal_observed_at
            or signal_observation.price_identity != candidate.reference_price_identity
            or bool(signal_observation.is_gap_recovery)
        ):
            raise OOSBindingError(
                f"CandidateSignal source lineage mismatch for {candidate.signal_key}"
            )
        if candidate.entry_state != "FILLED":
            continue
        assert candidate.selected_market_ingest_seq is not None
        assert candidate.selected_market_observed_at is not None
        assert candidate.selected_source_event_key is not None
        observation = by_identity.get((candidate.mint, candidate.selected_market_ingest_seq))
        if observation is None:
            raise OOSBindingError(
                f"filled entry source lineage is missing for {candidate.signal_key}"
            )
        actual = (
            _iso(observation.observed_at),
            f"{observation.event_key}:MARKET",
            observation.price_identity,
        )
        expected = (
            candidate.selected_market_observed_at,
            candidate.selected_source_event_key,
            candidate.reference_price_identity,
        )
        if actual != expected or bool(observation.is_gap_recovery):
            raise OOSBindingError(
                f"filled entry source lineage mismatch for {candidate.signal_key}"
            )
        validated += 1
    return validated


def _ratio_text(numerator: int, denominator: int, places: int = 12) -> str | None:
    if denominator == 0:
        return None
    value = Decimal(numerator) / Decimal(denominator)
    return format(value.quantize(Decimal(1).scaleb(-places)), "f")


def _mean_text(values: Sequence[int]) -> str | None:
    return None if not values else _ratio_text(sum(values), len(values))


def _median_nearest_rank(values: Sequence[int]) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[(len(ordered) - 1) // 2]


def _complete_utc_days(window: FrozenOOSWindowV01) -> tuple[date, ...]:
    first = window.start_at.date()
    if window.start_at.time() != time(0, 0, 0):
        first += timedelta(days=1)
    result: list[date] = []
    current = first
    while datetime.combine(current + timedelta(days=1), time.min, timezone.utc) <= window.end_at:
        result.append(current)
        current += timedelta(days=1)
    return tuple(result)


def policy_metrics_v0_1(
    rows: Sequence[Mapping[str, Any]],
    policy: FrozenOOSPolicyV01,
    window: FrozenOOSWindowV01,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    filled = [row for row in rows if row["classification"] == "FILLED"]
    net_values = [int(row["net_pnl_lamports"]) for row in filled]
    winners = [value for value in net_values if value > 0]
    losers = [value for value in net_values if value < 0]
    breakevens = [value for value in net_values if value == 0]
    gross_profit = sum(winners)
    gross_loss = -sum(losers)
    if gross_loss:
        profit_factor = _ratio_text(gross_profit, gross_loss)
    elif gross_profit:
        profit_factor = "INFINITE"
    else:
        profit_factor = None

    equity = 0
    peak = 0
    max_drawdown = 0
    for row in sorted(
        filled,
        key=lambda item: (
            str(item["exit_fill_observed_at"]),
            int(item["exit_fill_source_row"]),
            str(item["signal_key"]),
        ),
    ):
        equity += int(row["net_pnl_lamports"])
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)

    complete_days = _complete_utc_days(window)
    daily: list[dict[str, Any]] = []
    for day in complete_days:
        day_values = [
            int(row["net_pnl_lamports"])
            for row in filled
            if _parse_utc(str(row["signal_observed_at"]), "signal_observed_at").date() == day
        ]
        daily.append({
            "policy_id": policy.policy_id,
            "utc_day": day.isoformat(),
            "attribution_clock": "CandidateSignal.signal_observed_at UTC",
            "filled_entries": len(day_values),
            "net_pnl_lamports": sum(day_values),
            "positive": sum(day_values) > 0,
        })
    positive_days = sum(bool(row["positive"]) for row in daily)
    rejection_counts: Counter[str] = Counter()
    for row in rows:
        rejection = row["rejection_classification"]
        if rejection is not None:
            rejection_counts[str(rejection)] += 1
        elif row["classification"] != "FILLED":
            rejection_counts[str(row["classification"])] += 1
    exit_reasons = Counter(
        str(row["exit_reason"])
        for row in rows
        if row["exit_reason"] is not None
    )
    entry_filled = sum(row["entry_state"] == "FILLED" for row in rows)
    entry_rejected = sum(row["entry_state"] == "REJECTED" for row in rows)
    return {
        "policy_id": policy.policy_id,
        "policy_fingerprint": policy.fingerprint,
        "policy_classification": policy.classification,
        "promotion_eligible": policy.promotion_eligible,
        "eligible_entry_count": len(rows),
        "attempted_entries": sum(bool(row["attempted"]) for row in rows),
        "entry_filled_count": entry_filled,
        "entry_rejected_count": entry_rejected,
        "filled_entries": len(filled),
        "unresolved_or_rejected_entries": len(rows) - len(filled),
        "fill_rate": _ratio_text(len(filled), len(rows)),
        "fill_rate_numerator": len(filled),
        "fill_rate_denominator": len(rows),
        "rejection_classifications": dict(sorted(rejection_counts.items())),
        "gross_execution_pnl_lamports": sum(int(row["gross_execution_pnl_lamports"]) for row in filled),
        "entry_explicit_cost_lamports": sum(
            int(row["entry_explicit_cost_lamports"])
            for row in rows
            if row["entry_state"] == "FILLED"
        ),
        "exit_explicit_cost_lamports": sum(int(row["exit_explicit_cost_lamports"]) for row in filled),
        "total_explicit_cost_lamports": (
            sum(
                int(row["entry_explicit_cost_lamports"])
                for row in rows
                if row["entry_state"] == "FILLED"
            )
            + sum(int(row["exit_explicit_cost_lamports"]) for row in filled)
        ),
        "net_pnl_lamports": sum(net_values),
        "expectancy_per_filled_trade_lamports": _mean_text(net_values),
        "expectancy_numerator_lamports": sum(net_values),
        "expectancy_denominator_filled_trades": len(filled),
        "wins": len(winners),
        "losses": len(losers),
        "breakevens": len(breakevens),
        "win_rate": _ratio_text(len(winners), len(filled)),
        "average_winner_lamports": _mean_text(winners),
        "average_loser_lamports": _mean_text(losers),
        "profit_factor": profit_factor,
        "profit_factor_gross_profit_lamports": gross_profit,
        "profit_factor_gross_loss_lamports": gross_loss,
        "median_holding_time_us": _median_nearest_rank([int(row["holding_time_us"]) for row in filled]),
        "max_realized_closed_equity_drawdown_lamports": max_drawdown,
        "exit_reason_counts": dict(sorted(exit_reasons.items())),
        "complete_utc_days": len(daily),
        "positive_complete_utc_days": positive_days,
        "positive_complete_utc_day_fraction": _ratio_text(positive_days, len(daily)),
        "daily_pnl_attribution": "CandidateSignal.signal_observed_at UTC",
        "net_pnl_scope": "REALIZED_CLOSED_TRADES_ONLY",
        "entry_cost_scope": "ALL_FILLED_ENTRIES_INCLUDING_UNRESOLVED_EXITS",
        "unresolved_pnl_treatment": "EXCLUDED_NOT_ZERO_FILLED_PNL",
    }, daily


def _metric_fraction(metrics: Mapping[str, Any], key: str) -> Fraction | None:
    value = metrics.get(key)
    if value is None:
        return None
    if value == "INFINITE":
        return None
    return Fraction(str(value))


def evaluate_h1_gates_v0_1(
    h1: Mapping[str, Any],
    control: Mapping[str, Any],
    window: FrozenOOSWindowV01,
) -> dict[str, Any]:
    fill_denominator = int(h1.get("fill_rate_denominator", 0))
    fill_rate = (
        Fraction(int(h1["fill_rate_numerator"]), fill_denominator)
        if fill_denominator
        else (_metric_fraction(h1, "fill_rate") or Fraction(0))
    )
    expectancy_denominator = int(h1.get("expectancy_denominator_filled_trades", 0))
    expectancy = (
        Fraction(int(h1["expectancy_numerator_lamports"]), expectancy_denominator)
        if expectancy_denominator
        else _metric_fraction(h1, "expectancy_per_filled_trade_lamports")
    )
    control_denominator = int(control.get("expectancy_denominator_filled_trades", 0))
    control_expectancy = (
        Fraction(int(control["expectancy_numerator_lamports"]), control_denominator)
        if control_denominator
        else _metric_fraction(control, "expectancy_per_filled_trade_lamports")
    )
    if "profit_factor_gross_profit_lamports" in h1:
        gross_profit = int(h1["profit_factor_gross_profit_lamports"])
        gross_loss = int(h1["profit_factor_gross_loss_lamports"])
        profit_factor_pass = (
            gross_profit > 0 if gross_loss == 0
            else Fraction(gross_profit, gross_loss) >= MINIMUM_PROFIT_FACTOR
        )
    else:
        profit_factor_value = h1.get("profit_factor")
        profit_factor_pass = profit_factor_value == "INFINITE" or (
            profit_factor_value is not None
            and Fraction(str(profit_factor_value)) >= MINIMUM_PROFIT_FACTOR
        )
    net = int(h1["net_pnl_lamports"])
    drawdown = int(h1["max_realized_closed_equity_drawdown_lamports"])
    if drawdown == 0:
        drawdown_ratio = "INFINITE" if net > 0 else "0"
        drawdown_pass = net > 0
    else:
        exact_ratio = Fraction(net, drawdown)
        drawdown_ratio = _ratio_text(net, drawdown)
        drawdown_pass = exact_ratio >= MINIMUM_NET_TO_DRAWDOWN
    complete_days = int(h1["complete_utc_days"])
    positive_days = int(h1["positive_complete_utc_days"])
    positive_fraction = Fraction(positive_days, complete_days) if complete_days else Fraction(0)
    if complete_days == 3:
        daily_pass = positive_days >= 2
        daily_rule = "EXACTLY_3_DAYS_AT_LEAST_2_POSITIVE"
    elif complete_days > 3:
        daily_pass = positive_fraction >= MINIMUM_POSITIVE_DAY_FRACTION
        daily_rule = "MORE_THAN_3_DAYS_POSITIVE_FRACTION_GTE_0.60"
    else:
        daily_pass = False
        daily_rule = "FEWER_THAN_3_COMPLETE_DAYS_INSUFFICIENT"
    gates = {
        "minimum_window_duration": window.duration_seconds >= MINIMUM_WINDOW_SECONDS,
        "minimum_h1_eligible_entries": int(h1["eligible_entry_count"]) >= H1_MINIMUM_ELIGIBLE_ENTRIES,
        "minimum_fill_rate": fill_rate >= MINIMUM_FILL_RATE,
        "positive_net_pnl": net > 0,
        "positive_expectancy": expectancy is not None and expectancy > 0,
        "minimum_profit_factor": profit_factor_pass,
        "minimum_net_to_drawdown": drawdown_pass,
        "daily_robustness": daily_pass,
        "h1_expectancy_gt_matched_control": (
            expectancy is not None
            and control_expectancy is not None
            and expectancy > control_expectancy
        ),
    }
    hard_names = (
        "minimum_fill_rate",
        "positive_net_pnl",
        "positive_expectancy",
        "minimum_profit_factor",
        "minimum_net_to_drawdown",
        "h1_expectancy_gt_matched_control",
    )
    robustness_names = (
        "minimum_window_duration",
        "minimum_h1_eligible_entries",
        "daily_robustness",
    )
    if not all(gates[name] for name in hard_names):
        disposition = "NO_GO"
    elif not all(gates[name] for name in robustness_names):
        disposition = "CONDITIONAL_POSITIVE_BUT_INSUFFICIENT_ROBUSTNESS"
    else:
        disposition = "GO_TO_PAPER_GATE"
    return {
        "policy_id": "H1_SIMPLE_TIME_TP10_T30",
        "protocol_id": PROTOCOL_ID,
        "protocol_fingerprint": MODEL_FINGERPRINT,
        "gates": gates,
        "hard_gate_names": list(hard_names),
        "robustness_gate_names": list(robustness_names),
        "daily_rule_applied": daily_rule,
        "net_to_max_drawdown_ratio": drawdown_ratio,
        "disposition": disposition,
        "promotion_allowed": disposition == "GO_TO_PAPER_GATE",
        "no_live_promotion_path": True,
        "window_extension_decision": "EXTERNAL_DURATION_SAMPLE_TECHNICAL_ONLY_NOT_PERFORMED",
        "window_extension_must_not_use_pnl": True,
    }


def _assert_separate_phase6_output(output_dir: str | Path, *inputs: str | Path) -> Path:
    output = Path(output_dir).resolve()
    if "phase6" not in {part.lower() for part in output.parts}:
        raise OOSBindingError("evaluation output must be under a separate Phase-6 path")
    for item in inputs:
        if output == Path(item).resolve():
            raise OOSBindingError("output path cannot be an input database")
    return output


def _assert_no_sqlite_write_sidecars(path: str | Path) -> None:
    absolute = Path(path).resolve()
    present = [
        str(sidecar)
        for suffix in ("-wal", "-journal")
        if (sidecar := Path(str(absolute) + suffix)).is_file()
        and sidecar.stat().st_size > 0
    ]
    if present:
        raise OOSBindingError(
            "frozen SQLite input has uncheckpointed write sidecars: " + ", ".join(present)
        )


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(_primitive(value), sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    ).encode("utf-8")


def _write_artifact(path: Path, value: Any) -> str:
    body = _json_bytes(value)
    path.write_bytes(body)
    return hashlib.sha256(body).hexdigest()


def write_evaluation_artifacts_v0_1(
    *,
    output_dir: str | Path,
    window: FrozenOOSWindowV01,
    candidates: Sequence[FrozenOOSCandidateV01],
    policy_rows: Sequence[Mapping[str, Any]],
    policy_metrics: Sequence[Mapping[str, Any]],
    daily_metrics: Sequence[Mapping[str, Any]],
    gate_evaluation: Mapping[str, Any],
    source_evidence: Mapping[str, Any],
    paper_db_sha256: str,
) -> dict[str, Any]:
    output = _assert_separate_phase6_output(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    dispositions = []
    for definition in policy_definitions_v0_1():
        policy_id = str(definition["policy_id"])
        policy = POLICIES[policy_id]
        if policy_id == "H1_SIMPLE_TIME_TP10_T30":
            disposition = gate_evaluation["disposition"]
        elif policy.classification == DIAGNOSTIC_CLASSIFICATION:
            disposition = DIAGNOSTIC_CLASSIFICATION
        else:
            disposition = "REFERENCE_ONLY_NOT_PROMOTABLE"
        dispositions.append({
            "policy_id": policy_id,
            "policy_classification": policy.classification,
            "promotion_eligible": policy.promotion_eligible,
            "disposition": disposition,
        })
    protocol_manifest = {
        **PROTOCOL_DEFINITION,
        "model_fingerprint": MODEL_FINGERPRINT,
        "policy_set_sha256": canonical_sha256(policy_definitions_v0_1()),
    }
    source_window_manifest = {
        "window": window.canonical_payload(),
        "window_fingerprint": window.fingerprint,
        "paper_db_sha256": paper_db_sha256,
        "candidate_count": len(candidates),
        "candidate_cohort_sha256": canonical_sha256([
            candidate.canonical_payload()
            for candidate in sorted(
                candidates,
                key=lambda row: (
                    row.signal_observed_at,
                    row.signal_ingest_seq,
                    row.signal_key,
                ),
            )
        ]),
        "source": dict(source_evidence),
    }
    artifacts: dict[str, Any] = {
        "protocol_manifest.json": protocol_manifest,
        "source_window_manifest.json": source_window_manifest,
        "policy_definitions.json": policy_definitions_v0_1(),
        "per_trade_evaluation_rows.json": list(policy_rows),
        "policy_metrics.json": list(policy_metrics),
        "daily_metrics.json": list(daily_metrics),
        "gate_evaluation.json": dict(gate_evaluation),
        "final_disposition.json": dispositions,
    }
    hashes = {
        name: _write_artifact(output / name, value)
        for name, value in sorted(artifacts.items())
    }
    run_digest = canonical_sha256({"artifact_sha256": hashes})
    manifest = {
        "model_id": MODEL_ID,
        "model_fingerprint": MODEL_FINGERPRINT,
        "window_fingerprint": window.fingerprint,
        "artifact_sha256": hashes,
        "run_digest": run_digest,
        "deterministic": True,
    }
    _write_artifact(output / "artifact_manifest.json", manifest)
    return manifest


def evaluate_frozen_inputs_v0_1(
    *,
    candidates: Sequence[FrozenOOSCandidateV01],
    paths: Mapping[str, tuple[Any, ...]],
    window: FrozenOOSWindowV01,
    output_dir: str | Path,
    source_evidence: Mapping[str, Any],
    paper_db_sha256: str,
) -> dict[str, Any]:
    _assert_separate_phase6_output(output_dir)
    if source_evidence.get("source_db_sha256") != window.source_db_sha256:
        raise OOSBindingError("source evidence does not match frozen OOS window")
    if source_evidence.get("source_model_id") != window.source_model_id:
        raise OOSBindingError("source evidence model does not match frozen OOS window")
    if source_evidence.get("source_model_fingerprint") != window.source_model_fingerprint:
        raise OOSBindingError("source evidence fingerprint does not match frozen OOS window")
    seen_candidates: set[str] = set()
    for candidate in candidates:
        if candidate.signal_key in seen_candidates:
            raise OOSBindingError(f"duplicate frozen CandidateSignal: {candidate.signal_key}")
        seen_candidates.add(candidate.signal_key)
        if not (window.start_at <= candidate.signal_at < window.end_at):
            raise OOSBindingError("CandidateSignal timestamp lies outside frozen OOS window")
        if not (
            window.source_start_after_ingest_seq
            < candidate.signal_ingest_seq
            <= window.source_end_ingest_seq
        ):
            raise OOSBindingError("CandidateSignal cursor lies outside frozen source window")
        if candidate.entry_state == "FILLED" and not (
            window.source_start_after_ingest_seq
            < int(candidate.selected_market_ingest_seq or -1)
            <= window.source_end_ingest_seq
        ):
            raise OOSBindingError("filled entry cursor lies outside frozen source window")
    for mint, path in paths.items():
        for observation in path:
            if observation.mint != mint:
                raise OOSBindingError("market path key/mint mismatch")
            if not (
                window.source_start_after_ingest_seq
                < int(observation.ingest_seq)
                <= window.source_end_ingest_seq
            ):
                raise OOSBindingError("market observation lies outside frozen source window")
    lineage_count = validate_entry_source_lineage_v0_1(candidates, paths)
    bound_source_evidence = {
        **source_evidence,
        "normalized_filled_entry_lineage_count": lineage_count,
        "normalized_filled_entry_lineage_exact": True,
    }
    all_rows: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    daily: list[dict[str, Any]] = []
    by_policy_metrics: dict[str, dict[str, Any]] = {}
    for policy_id in sorted(POLICIES):
        policy = POLICIES[policy_id]
        rows = evaluate_policy_rows_v0_1(
            candidates,
            paths,
            policy,
            frozen_watermark=window.source_end_ingest_seq,
        )
        policy_metric, policy_daily = policy_metrics_v0_1(rows, policy, window)
        all_rows.extend(rows)
        metrics.append(policy_metric)
        daily.extend(policy_daily)
        by_policy_metrics[policy_id] = policy_metric
    gates = evaluate_h1_gates_v0_1(
        by_policy_metrics["H1_SIMPLE_TIME_TP10_T30"],
        by_policy_metrics["CTRL_ALL_TP10_T30"],
        window,
    )
    manifest = write_evaluation_artifacts_v0_1(
        output_dir=output_dir,
        window=window,
        candidates=candidates,
        policy_rows=all_rows,
        policy_metrics=metrics,
        daily_metrics=daily,
        gate_evaluation=gates,
        source_evidence=bound_source_evidence,
        paper_db_sha256=paper_db_sha256,
    )
    return {
        "manifest": manifest,
        "gate_evaluation": gates,
        "policy_metrics": metrics,
        "daily_metrics": daily,
        "policy_rows": all_rows,
    }


def run_sqlite_evaluation_v0_1(
    *,
    paper_db: str | Path,
    source_db: str | Path,
    output_dir: str | Path,
    window: FrozenOOSWindowV01,
    expected_paper_db_sha256: str,
) -> dict[str, Any]:
    output = _assert_separate_phase6_output(output_dir, paper_db, source_db)
    if output.exists():
        raise OOSBindingError("Phase-6 output directory must be new and immutable per run")
    if not _is_sha256(expected_paper_db_sha256):
        raise ValueError("expected_paper_db_sha256 must be a lowercase SHA256")
    _assert_no_sqlite_write_sidecars(paper_db)
    _assert_no_sqlite_write_sidecars(source_db)
    before_paper = sha256_file(paper_db)
    before_source = sha256_file(source_db)
    if before_paper != expected_paper_db_sha256:
        raise OOSBindingError("paper database SHA256 mismatch")
    if before_source != window.source_db_sha256:
        raise OOSBindingError("source database SHA256 mismatch")
    candidates = load_frozen_candidates_v0_1(paper_db, window)
    paths, source_evidence = load_frozen_market_paths_v0_1(source_db, window)
    after_read_paper = sha256_file(paper_db)
    after_read_source = sha256_file(source_db)
    if (before_paper, before_source) != (after_read_paper, after_read_source):
        raise OOSBindingError("frozen SQLite input changed during read-only evaluation")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.staging-", dir=output.parent))
    try:
        result = evaluate_frozen_inputs_v0_1(
            candidates=candidates,
            paths=paths,
            window=window,
            output_dir=staging,
            source_evidence={
                **source_evidence,
                "paper_connection_mode": "mode=ro; PRAGMA query_only=ON",
                "input_hashes_unchanged_after_read": True,
            },
            paper_db_sha256=before_paper,
        )
        if sha256_file(paper_db) != before_paper or sha256_file(source_db) != before_source:
            raise OOSBindingError("frozen SQLite input changed during artifact finalization")
        staging.replace(output)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    result["input_immutability"] = {
        "paper_db_sha256_before_after_equal": True,
        "source_db_sha256_before_after_equal": True,
    }
    return result


def protocol_review_payload_v0_1() -> dict[str, Any]:
    h1 = POLICIES["H1_SIMPLE_TIME_TP10_T30"]
    control = POLICIES["CTRL_ALL_TP10_T30"]
    h1_exit = h1.exit_policy.canonical_payload().copy()
    control_exit = control.exit_policy.canonical_payload().copy()
    h1_exit.pop("policy_id")
    control_exit.pop("policy_id")
    return {
        "model_id": MODEL_ID,
        "model_fingerprint": MODEL_FINGERPRINT,
        "protocol_id": PROTOCOL_ID,
        "protocol_definition": PROTOCOL_DEFINITION,
        "policies": policy_definitions_v0_1(),
        "policy_set_sha256": canonical_sha256(policy_definitions_v0_1()),
        "h1_and_matched_control_exit_contract_exact": h1_exit == control_exit,
        "h1_and_control_only_intended_cohort_difference": (
            h1.eligibility_rule == ELIGIBILITY_H1_TIME
            and control.eligibility_rule == ELIGIBILITY_ALL
        ),
        "no_72h_evaluation_executed": True,
    }
