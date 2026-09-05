from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from fractions import Fraction
from pathlib import Path
from typing import Any, Iterable, Protocol

from .paper_cost_baseline_v0_1 import P4_COST_BASELINE_0001
from .paper_continuous_market_source_v0_2 import ContinuousMarketSourceRecordV02
from .post24h_frozen_replay_v0_1 import (
    EXPECTED,
    EXPECTED_COHORT_SHA256,
    EXPECTED_PHYSICAL_ENTRIES,
    EXPECTED_PAPER_SHA256,
    FROZEN_WATERMARK,
    MODEL_FINGERPRINT as T001_MODEL_FINGERPRINT,
    ReplayExecutionInputV01,
    ReplayExecutionOutcomeV01,
    ReplayMismatch,
    canonical_sha256,
    execute_exit_intent_v0_1,
    load_frozen_entries,
    load_frozen_market_paths,
    replay_executions,
    replay_intents,
    sha256_file,
)
from .runtime_exit_price_impact_v0_1 import (
    MODEL_FINGERPRINT as EXIT_IMPACT_FINGERPRINT,
)
from .runtime_exit_price_impact_v0_1 import MODEL_ID as EXIT_IMPACT_MODEL_ID


MODEL_ID = "P4-POST24H-COUNTERFACTUAL-EXIT-REPLAY-0001"
SCHEMA_VERSION = "phase4_post24h_counterfactual_exit_replay_v0.1"
EXPECTED_T001_REPLAY_SHA256 = (
    "3ee403382569ac1f041da4798e3e15818ec5b4c1d60c0c4a42b3c24e9c96b61a"
)


class TimeoutOrigin(StrEnum):
    PHASE4_SIGNAL_CLOCK = "PHASE4_SIGNAL_CLOCK"
    ENTRY_FILL_CLOCK = "ENTRY_FILL_CLOCK"


class PolicyFamily(StrEnum):
    TIMEOUT_ONLY = "TIMEOUT_ONLY"
    TP_TIMEOUT = "TP_TIMEOUT"
    SL_TIMEOUT = "SL_TIMEOUT"
    TP_SL_TIMEOUT = "TP_SL_TIMEOUT"
    TRAILING_TIMEOUT = "TRAILING_TIMEOUT"


class CounterfactualExitReason(StrEnum):
    TAKE_PROFIT = "TAKE_PROFIT"
    STOP_LOSS = "STOP_LOSS"
    TRAIL = "TRAIL"
    FALLBACK = "FALLBACK"


@dataclass(frozen=True, slots=True)
class CounterfactualExitPolicyV01:
    policy_id: str
    timeout_ms: int
    timeout_origin: TimeoutOrigin
    tp_bps: int | None = None
    sl_bps: int | None = None
    trailing_activation_bps: int | None = None
    trailing_giveback_bps: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.policy_id, str) or not self.policy_id.strip():
            raise ValueError("policy_id is required")
        if isinstance(self.timeout_ms, bool) or not isinstance(self.timeout_ms, int):
            raise ValueError("timeout_ms must be an integer")
        if self.timeout_ms <= 0:
            raise ValueError("timeout_ms must be > 0")
        if not isinstance(self.timeout_origin, TimeoutOrigin):
            raise ValueError("timeout_origin must be explicit")
        for label, value in (
            ("tp_bps", self.tp_bps),
            ("sl_bps", self.sl_bps),
            ("trailing_activation_bps", self.trailing_activation_bps),
            ("trailing_giveback_bps", self.trailing_giveback_bps),
        ):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value <= 0
            ):
                raise ValueError(f"{label} must be a positive integer when provided")
        trailing_values = (
            self.trailing_activation_bps,
            self.trailing_giveback_bps,
        )
        if (trailing_values[0] is None) != (trailing_values[1] is None):
            raise ValueError("trailing activation and giveback must be provided together")
        if trailing_values[0] is not None and (
            self.tp_bps is not None or self.sl_bps is not None
        ):
            raise ValueError("trailing cannot combine with fixed TP or SL")

    @property
    def family(self) -> PolicyFamily:
        if self.trailing_activation_bps is not None:
            return PolicyFamily.TRAILING_TIMEOUT
        if self.tp_bps is not None and self.sl_bps is not None:
            return PolicyFamily.TP_SL_TIMEOUT
        if self.tp_bps is not None:
            return PolicyFamily.TP_TIMEOUT
        if self.sl_bps is not None:
            return PolicyFamily.SL_TIMEOUT
        return PolicyFamily.TIMEOUT_ONLY

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.canonical_payload())

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "family": self.family.value,
            "timeout_ms": self.timeout_ms,
            "timeout_origin": self.timeout_origin.value,
            "tp_bps": self.tp_bps,
            "sl_bps": self.sl_bps,
            "trailing_activation_bps": self.trailing_activation_bps,
            "trailing_giveback_bps": self.trailing_giveback_bps,
        }


class MarketObservationV01(Protocol):
    mint: str
    observed_at: datetime
    ingest_seq: int
    event_key: str
    price_identity: str
    price_numerator_raw: int
    price_denominator_raw: int
    current_virtual_token_reserve_raw: int
    is_gap_recovery: bool


@dataclass(frozen=True, slots=True)
class CounterfactualMarketObservationV01:
    mint: str
    observed_at: datetime
    ingest_seq: int
    event_key: str
    price_identity: str
    price_numerator_raw: int
    price_denominator_raw: int
    current_virtual_token_reserve_raw: int
    is_gap_recovery: bool = False

    def __post_init__(self) -> None:
        _require_utc(self.observed_at)
        if self.ingest_seq < 0:
            raise ValueError("ingest_seq must be >= 0")
        if self.price_numerator_raw <= 0 or self.price_denominator_raw <= 0:
            raise ValueError("price components must be positive")
        if self.current_virtual_token_reserve_raw <= 0:
            raise ValueError("current_virtual_token_reserve_raw must be positive")


@dataclass(frozen=True, slots=True)
class CounterfactualExitIntentV01:
    intent_id: str
    policy_id: str
    policy_fingerprint: str
    signal_key: str
    mint: str
    reason: CounterfactualExitReason
    requested_at: datetime
    timeout_deadline_at: datetime
    timeout_fire_at: datetime
    trigger_observed_at: datetime | None
    trigger_ingest_seq: int | None
    trigger_source_event_key: str | None
    trigger_price_numerator_raw: int | None
    trigger_price_denominator_raw: int | None
    rule_return_bps: int | None
    trail_peak_return_bps: int | None
    reference_source: str
    reference_observed_at: datetime
    reference_ingest_seq: int
    reference_price_numerator_raw: int
    reference_price_denominator_raw: int
    last_fresh_observed_at: datetime | None
    last_fresh_ingest_seq: int | None
    last_fresh_source_event_key: str | None
    last_fresh_return_bps: int | None


@dataclass(frozen=True, slots=True)
class CounterfactualReplayResultV01:
    intent: CounterfactualExitIntentV01
    execution: ReplayExecutionOutcomeV01

    @property
    def classification(self) -> str:
        if self.execution.exit_state == "FILLED":
            return "FILLED"
        if self.execution.rejection_state in (
            "REJECTED_SLIPPAGE",
            "NO_CAUSAL_FILL",
        ):
            return self.execution.rejection_state
        raise ReplayMismatch("counterfactual execution has invalid terminal classification")


SPEC = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "t001_execution_model_fingerprint": T001_MODEL_FINGERPRINT,
    "cost_model_fingerprint": P4_COST_BASELINE_0001.fingerprint,
    "exit_impact_model_id": EXIT_IMPACT_MODEL_ID,
    "exit_impact_fingerprint": EXIT_IMPACT_FINGERPRINT,
    "path_order": ["observed_at", "ingest_seq"],
    "source_boundary_validation": "FULL_PATH_BEFORE_TERMINAL_DECISION",
    "duplicate_source_identity": "EXACT_REPLAY_DEDUPLICATED_CONFLICT_FAIL_CLOSED",
    "threshold_reference": "FROZEN_PHASE4_RULE_REFERENCE",
    "timeout_origins": [origin.value for origin in TimeoutOrigin],
    "timeout_fire_offset_us": 1,
    "families": [family.value for family in PolicyFamily],
    "execution": "SHARED_T001_CANONICAL_EXECUTION_HELPER",
    "terminal_classifications": ["FILLED", "REJECTED_SLIPPAGE", "NO_CAUSAL_FILL"],
}
MODEL_FINGERPRINT = canonical_sha256(SPEC)


BASELINE_POLICIES: dict[str, CounterfactualExitPolicyV01] = {
    "FINAL-A": CounterfactualExitPolicyV01(
        policy_id="FINAL-A",
        timeout_ms=15_000,
        timeout_origin=TimeoutOrigin.PHASE4_SIGNAL_CLOCK,
        tp_bps=1_000,
    ),
    "FINAL-B": CounterfactualExitPolicyV01(
        policy_id="FINAL-B",
        timeout_ms=15_000,
        timeout_origin=TimeoutOrigin.PHASE4_SIGNAL_CLOCK,
        trailing_activation_bps=1_000,
        trailing_giveback_bps=300,
    ),
    "SENS-C": CounterfactualExitPolicyV01(
        policy_id="SENS-C",
        timeout_ms=5_000,
        timeout_origin=TimeoutOrigin.PHASE4_SIGNAL_CLOCK,
        tp_bps=2_000,
    ),
}


def _require_utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("datetime must be timezone-aware UTC")


def _dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ReplayMismatch(f"naive timestamp in frozen entry: {value}")
    return parsed.astimezone(timezone.utc)


def _return_bps(
    ref_num: int,
    ref_den: int,
    price_num: int,
    price_den: int,
) -> int:
    delta = (Fraction(price_num, price_den) / Fraction(ref_num, ref_den) - 1) * 10_000
    sign = -1 if delta < 0 else 1
    absolute = abs(delta)
    quotient, remainder = divmod(absolute.numerator, absolute.denominator)
    if remainder * 2 >= absolute.denominator:
        quotient += 1
    return sign * quotient


def _event_key(observation: MarketObservationV01) -> str:
    return f"{observation.event_key}:MARKET"


def _observation_payload(observation: MarketObservationV01) -> tuple[Any, ...]:
    return (
        observation.mint,
        observation.observed_at.isoformat(timespec="microseconds"),
        observation.ingest_seq,
        observation.event_key,
        observation.price_identity,
        observation.price_numerator_raw,
        observation.price_denominator_raw,
        observation.current_virtual_token_reserve_raw,
        observation.is_gap_recovery,
    )


def _validated_ordered_path(
    path: Iterable[MarketObservationV01],
    frozen_watermark: int,
) -> tuple[MarketObservationV01, ...]:
    unique: list[MarketObservationV01] = []
    seen: dict[tuple[datetime, int], tuple[Any, ...]] = {}
    for observation in path:
        _require_utc(observation.observed_at)
        if observation.ingest_seq > frozen_watermark:
            raise ReplayMismatch(
                f"counterfactual source row exceeds frozen watermark: {observation.ingest_seq}"
            )
        key = (observation.observed_at, int(observation.ingest_seq))
        payload = _observation_payload(observation)
        old_payload = seen.get(key)
        if old_payload is not None:
            if old_payload != payload:
                raise ReplayMismatch(
                    "same observed_at + ingest_seq has conflicting market content"
                )
            continue
        seen[key] = payload
        unique.append(observation)
    return tuple(sorted(unique, key=lambda row: (row.observed_at, row.ingest_seq)))


def _intent_id(
    entry: Any,
    policy: CounterfactualExitPolicyV01,
    reason: CounterfactualExitReason,
    requested_at: datetime,
    trigger_ingest_seq: int | None,
) -> str:
    body = "\x1f".join(
        (
            entry.signal_key,
            entry.mint,
            policy.policy_id,
            policy.fingerprint,
            reason.value,
            requested_at.isoformat(timespec="microseconds"),
            str(trigger_ingest_seq),
        )
    )
    return f"cf-exit-{hashlib.sha256(body.encode('utf-8')).hexdigest()[:32]}"


def evaluate_counterfactual_intent_v0_1(
    entry: Any,
    path: Iterable[MarketObservationV01],
    policy: CounterfactualExitPolicyV01,
    *,
    frozen_watermark: int = FROZEN_WATERMARK,
) -> CounterfactualExitIntentV01:
    signal_at = _dt(entry.signal_observed_at)
    entry_at = _dt(entry.entry_observed_at)
    entry_key = (entry_at, int(entry.entry_ingest_seq))
    origin_at = (
        signal_at
        if policy.timeout_origin is TimeoutOrigin.PHASE4_SIGNAL_CLOCK
        else entry_at
    )
    deadline = origin_at + timedelta(milliseconds=policy.timeout_ms)
    fire_at = deadline + timedelta(microseconds=1)
    last_fresh: MarketObservationV01 | None = None
    last_fresh_return: int | None = None
    peak_return: int | None = None
    ordered_path = _validated_ordered_path(path, frozen_watermark)
    for observation in ordered_path:
        if observation.mint != entry.mint:
            continue
        observation_key = (observation.observed_at, int(observation.ingest_seq))
        if observation_key <= entry_key:
            continue
        if observation.observed_at > deadline:
            break

        if observation.is_gap_recovery:
            continue
        if observation.price_identity != entry.reference_price_identity:
            continue
        current_return = _return_bps(
            int(entry.reference_price_numerator_raw),
            int(entry.reference_price_denominator_raw),
            int(observation.price_numerator_raw),
            int(observation.price_denominator_raw),
        )
        last_fresh = observation
        last_fresh_return = current_return

        reason: CounterfactualExitReason | None = None
        if policy.family is PolicyFamily.TRAILING_TIMEOUT:
            assert policy.trailing_activation_bps is not None
            assert policy.trailing_giveback_bps is not None
            if peak_return is None:
                if current_return >= policy.trailing_activation_bps:
                    peak_return = current_return
            else:
                peak_return = max(peak_return, current_return)
                if peak_return - current_return >= policy.trailing_giveback_bps:
                    reason = CounterfactualExitReason.TRAIL
        else:
            if policy.tp_bps is not None and current_return >= policy.tp_bps:
                reason = CounterfactualExitReason.TAKE_PROFIT
            elif policy.sl_bps is not None and current_return <= -policy.sl_bps:
                reason = CounterfactualExitReason.STOP_LOSS

        if reason is not None:
            return CounterfactualExitIntentV01(
                intent_id=_intent_id(
                    entry, policy, reason, observation.observed_at, observation.ingest_seq
                ),
                policy_id=policy.policy_id,
                policy_fingerprint=policy.fingerprint,
                signal_key=entry.signal_key,
                mint=entry.mint,
                reason=reason,
                requested_at=observation.observed_at,
                timeout_deadline_at=deadline,
                timeout_fire_at=fire_at,
                trigger_observed_at=observation.observed_at,
                trigger_ingest_seq=observation.ingest_seq,
                trigger_source_event_key=_event_key(observation),
                trigger_price_numerator_raw=observation.price_numerator_raw,
                trigger_price_denominator_raw=observation.price_denominator_raw,
                rule_return_bps=current_return,
                trail_peak_return_bps=peak_return,
                reference_source="TRIGGER_MARKET",
                reference_observed_at=observation.observed_at,
                reference_ingest_seq=observation.ingest_seq,
                reference_price_numerator_raw=observation.price_numerator_raw,
                reference_price_denominator_raw=observation.price_denominator_raw,
                last_fresh_observed_at=observation.observed_at,
                last_fresh_ingest_seq=observation.ingest_seq,
                last_fresh_source_event_key=_event_key(observation),
                last_fresh_return_bps=current_return,
            )

    if last_fresh is None:
        reference_source = "RULE_REFERENCE_FALLBACK"
        reference_at = signal_at
        reference_seq = int(entry.signal_ingest_seq)
        reference_num = int(entry.reference_price_numerator_raw)
        reference_den = int(entry.reference_price_denominator_raw)
    else:
        reference_source = "LAST_FRESH_MARKET"
        reference_at = last_fresh.observed_at
        reference_seq = int(last_fresh.ingest_seq)
        reference_num = int(last_fresh.price_numerator_raw)
        reference_den = int(last_fresh.price_denominator_raw)
    reason = CounterfactualExitReason.FALLBACK
    return CounterfactualExitIntentV01(
        intent_id=_intent_id(entry, policy, reason, fire_at, None),
        policy_id=policy.policy_id,
        policy_fingerprint=policy.fingerprint,
        signal_key=entry.signal_key,
        mint=entry.mint,
        reason=reason,
        requested_at=fire_at,
        timeout_deadline_at=deadline,
        timeout_fire_at=fire_at,
        trigger_observed_at=None,
        trigger_ingest_seq=None,
        trigger_source_event_key=None,
        trigger_price_numerator_raw=None,
        trigger_price_denominator_raw=None,
        rule_return_bps=None,
        trail_peak_return_bps=peak_return,
        reference_source=reference_source,
        reference_observed_at=reference_at,
        reference_ingest_seq=reference_seq,
        reference_price_numerator_raw=reference_num,
        reference_price_denominator_raw=reference_den,
        last_fresh_observed_at=None if last_fresh is None else last_fresh.observed_at,
        last_fresh_ingest_seq=None if last_fresh is None else last_fresh.ingest_seq,
        last_fresh_source_event_key=None if last_fresh is None else _event_key(last_fresh),
        last_fresh_return_bps=last_fresh_return,
    )


def execute_counterfactual_intent_v0_1(
    entry: Any,
    path: Iterable[ContinuousMarketSourceRecordV02],
    intent: CounterfactualExitIntentV01,
    *,
    frozen_watermark: int = FROZEN_WATERMARK,
) -> ReplayExecutionOutcomeV01:
    return execute_exit_intent_v0_1(
        ReplayExecutionInputV01(
            signal_key=entry.signal_key,
            mint=entry.mint,
            price_identity=entry.reference_price_identity,
            entry_observed_at=_dt(entry.entry_observed_at),
            entry_ingest_seq=int(entry.entry_ingest_seq),
            open_at=_dt(entry.entry_observed_at),
            entry_principal_lamports=int(entry.principal_lamports),
            entry_price_numerator_raw=int(entry.entry_price_numerator_raw),
            entry_price_denominator_raw=int(entry.entry_price_denominator_raw),
            entry_explicit_cost_lamports=int(entry.entry_explicit_cost_lamports),
            exit_reason=intent.reason.value,
            requested_at=intent.requested_at,
            reference_source=intent.reference_source,
            reference_observed_at=intent.reference_observed_at,
            reference_ingest_seq=intent.reference_ingest_seq,
            reference_price_numerator_raw=intent.reference_price_numerator_raw,
            reference_price_denominator_raw=intent.reference_price_denominator_raw,
        ),
        path,
        frozen_watermark=frozen_watermark,
    )


def replay_counterfactual_v0_1(
    entry: Any,
    path: Iterable[ContinuousMarketSourceRecordV02],
    policy: CounterfactualExitPolicyV01,
    *,
    frozen_watermark: int = FROZEN_WATERMARK,
) -> CounterfactualReplayResultV01:
    frozen_path = tuple(path)
    intent = evaluate_counterfactual_intent_v0_1(
        entry, frozen_path, policy, frozen_watermark=frozen_watermark
    )
    execution = execute_counterfactual_intent_v0_1(
        entry, frozen_path, intent, frozen_watermark=frozen_watermark
    )
    return CounterfactualReplayResultV01(intent=intent, execution=execution)


def _baseline_row(
    accepted: dict[str, Any],
    result: CounterfactualReplayResultV01,
    entry: Any,
) -> dict[str, Any]:
    intent = result.intent
    outcome = result.execution
    return {
        "track_id": accepted["track_id"],
        "exit_variant": accepted["exit_variant"],
        "signal_key": entry.signal_key,
        "mint": entry.mint,
        "paper_position_id": accepted["paper_position_id"],
        "replay_exit_state": outcome.exit_state,
        "exit_reason": intent.reason.value,
        "exit_requested_at": intent.requested_at.isoformat(timespec="microseconds"),
        "trigger_source_row": intent.trigger_ingest_seq,
        "fill_source_row": outcome.fill_source_row,
        "trigger_observed_at": (
            None
            if intent.trigger_observed_at is None
            else intent.trigger_observed_at.isoformat(timespec="microseconds")
        ),
        "fill_observed_at": (
            None
            if outcome.fill_observed_at is None
            else outcome.fill_observed_at.isoformat(timespec="microseconds")
        ),
        "execution_rejection_state": outcome.rejection_state,
        "execution_attempt_count": outcome.attempt_count,
        "entry_principal_lamports": int(entry.principal_lamports),
        "entry_explicit_cost_lamports": int(entry.entry_explicit_cost_lamports),
        "gross_exit_proceeds_lamports": outcome.gross_exit_proceeds_lamports,
        "exit_explicit_cost_lamports": outcome.exit_explicit_cost_lamports,
        "gross_execution_pnl_lamports": outcome.gross_execution_pnl_lamports,
        "net_pnl_lamports": outcome.net_pnl_lamports,
        "holding_time_us": outcome.holding_time_us,
        "reference_source": intent.reference_source,
    }


def _load_actual_execution_routes(paper_db: str | Path) -> dict[str, dict[str, Any]]:
    absolute = Path(paper_db).resolve()
    conn = sqlite3.connect(f"file:{absolute.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only=ON")
        if conn.execute("PRAGMA query_only").fetchone()[0] != 1:
            raise ReplayMismatch("query_only unavailable for baseline comparison")
        return {
            row["paper_position_id"]: dict(row)
            for row in conn.execute("SELECT * FROM paper_exit_execution_routes")
        }
    finally:
        conn.close()


def run_frozen_baseline_gate_v0_1(
    paper_db: str | Path,
    source_db: str | Path,
) -> dict[str, Any]:
    if sha256_file(paper_db) != EXPECTED_PAPER_SHA256:
        raise ReplayMismatch("accepted paper DB SHA256 mismatch")
    entries, cohort = load_frozen_entries(paper_db, source_db)
    paths, source = load_frozen_market_paths(
        source_db, {entry.mint for entry in entries}
    )
    accepted_intents, _, _ = replay_intents(paper_db, paths)
    accepted_rows, accepted_summary, t001_digest = replay_executions(
        paper_db, accepted_intents, paths
    )
    if t001_digest != EXPECTED_T001_REPLAY_SHA256:
        raise ReplayMismatch(
            f"accepted T001 digest changed: {t001_digest} != {EXPECTED_T001_REPLAY_SHA256}"
        )

    entries_by_signal = {entry.signal_key: entry for entry in entries}
    if len(entries_by_signal) != EXPECTED_PHYSICAL_ENTRIES:
        raise ReplayMismatch("physical signal keys are not unique")

    generic_rows: list[dict[str, Any]] = []
    actual_routes = _load_actual_execution_routes(paper_db)
    comparison_fields = tuple(accepted_rows[0]) + (
        "exit_price_numerator_raw",
        "exit_price_denominator_raw",
    )
    mismatches: list[str] = []
    for accepted in accepted_rows:
        entry = entries_by_signal[accepted["signal_key"]]
        policy = BASELINE_POLICIES[accepted["track_id"]]
        result = replay_counterfactual_v0_1(entry, paths.get(entry.mint, ()), policy)
        generic = _baseline_row(accepted, result, entry)
        generic_rows.append(generic)
        for field in accepted_rows[0]:
            if generic[field] != accepted[field]:
                mismatches.append(
                    f"{accepted['paper_position_id']}:{field}:"
                    f"{generic[field]}!={accepted[field]}"
                )
        actual_route = actual_routes.get(accepted["paper_position_id"])
        if actual_route is None:
            mismatches.append(f"{accepted['paper_position_id']}:missing ACTUAL comparison route")
        else:
            actual_price_num = actual_route["simulated_exit_price_numerator_raw"]
            actual_price_den = actual_route["simulated_exit_price_denominator_raw"]
            expected_num = None if actual_price_num is None else int(actual_price_num)
            expected_den = None if actual_price_den is None else int(actual_price_den)
            if result.execution.exit_price_numerator_raw != expected_num:
                mismatches.append(
                    f"{accepted['paper_position_id']}:exit_price_numerator_raw:"
                    f"{result.execution.exit_price_numerator_raw}!={expected_num}"
                )
            if result.execution.exit_price_denominator_raw != expected_den:
                mismatches.append(
                    f"{accepted['paper_position_id']}:exit_price_denominator_raw:"
                    f"{result.execution.exit_price_denominator_raw}!={expected_den}"
                )
    if mismatches:
        raise ReplayMismatch(
            "generic counterfactual baseline differs from accepted T001: "
            + "; ".join(mismatches[:30])
        )

    generic_digest = canonical_sha256(generic_rows)
    if generic_digest != t001_digest:
        raise ReplayMismatch(
            f"generic baseline digest differs from T001: {generic_digest} != {t001_digest}"
        )
    # The accepted summary includes extra diagnostic keys; lock every accepted
    # total field-by-field while retaining those diagnostics as evidence.
    for track, expected in EXPECTED.items():
        for key, value in expected.items():
            if accepted_summary[track].get(key) != value:
                raise ReplayMismatch(
                    f"accepted baseline {track}.{key} mismatch: "
                    f"{accepted_summary[track].get(key)} != {value}"
                )

    rerun_rows: list[dict[str, Any]] = []
    for accepted in accepted_rows:
        entry = entries_by_signal[accepted["signal_key"]]
        result = replay_counterfactual_v0_1(
            entry,
            paths.get(entry.mint, ()),
            BASELINE_POLICIES[accepted["track_id"]],
        )
        rerun_rows.append(_baseline_row(accepted, result, entry))
    if rerun_rows != generic_rows:
        raise ReplayMismatch("generic counterfactual baseline rerun is not exact")

    t002a_digest = canonical_sha256(
        {
            "model_id": MODEL_ID,
            "model_fingerprint": MODEL_FINGERPRINT,
            "policies": [BASELINE_POLICIES[name].canonical_payload() for name in BASELINE_POLICIES],
            "baseline_rows": generic_rows,
        }
    )
    return {
        "model_id": MODEL_ID,
        "model_fingerprint": MODEL_FINGERPRINT,
        "cohort": cohort,
        "source": source,
        "policies": {
            name: {
                **policy.canonical_payload(),
                "fingerprint": policy.fingerprint,
            }
            for name, policy in BASELINE_POLICIES.items()
        },
        "summary": accepted_summary,
        "positions": len(generic_rows),
        "fills": sum(row["replay_exit_state"] == "FILLED" for row in generic_rows),
        "unresolved": sum(row["replay_exit_state"] != "FILLED" for row in generic_rows),
        "comparison_fields": list(comparison_fields),
        "exact_comparison": True,
        "actual_exit_route_required_for_counterfactual_execution": False,
        "t001_replay_digest": t001_digest,
        "generic_baseline_digest": generic_digest,
        "t002a_deterministic_digest": t002a_digest,
        "deterministic_rerun": "EXACT",
    }


def result_payload(result: CounterfactualReplayResultV01) -> dict[str, Any]:
    intent = asdict(result.intent)
    execution = asdict(result.execution)
    for key, value in tuple(intent.items()):
        if isinstance(value, datetime):
            intent[key] = value.isoformat(timespec="microseconds")
        elif isinstance(value, StrEnum):
            intent[key] = value.value
    for key, value in tuple(execution.items()):
        if isinstance(value, datetime):
            execution[key] = value.isoformat(timespec="microseconds")
    return {
        "intent": intent,
        "execution": execution,
        "classification": result.classification,
    }
