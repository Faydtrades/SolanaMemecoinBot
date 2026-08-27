from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Mapping


MODEL_ID = "P5-SHADOW-DOMAIN-CAPABILITY-FIREWALL-0001"
SCHEMA_VERSION = "phase5_shadow_domain_capability_firewall_v0.1"
INTENT_SCHEMA_VERSION = "phase5_execution_intent_v0.1"
STATE_MACHINE_VERSION = "phase5_shadow_state_machine_v0.1"


class IntentRole(StrEnum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"


class IntentSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class ShadowState(StrEnum):
    CREATED = "CREATED"
    ELIGIBILITY_CHECKED = "ELIGIBILITY_CHECKED"
    ROUTE_BOUND = "ROUTE_BOUND"
    QUOTE_BOUND = "QUOTE_BOUND"
    PLAN_BUILT = "PLAN_BUILT"
    SIMULATED = "SIMULATED"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"


TERMINAL_STATES = frozenset(
    {
        ShadowState.COMPLETED,
        ShadowState.REJECTED,
        ShadowState.EXPIRED,
        ShadowState.FAILED,
    }
)

STATE_TRANSITIONS: Mapping[ShadowState, frozenset[ShadowState]] = MappingProxyType({
    ShadowState.CREATED: frozenset(
        {
            ShadowState.ELIGIBILITY_CHECKED,
            ShadowState.REJECTED,
            ShadowState.EXPIRED,
            ShadowState.FAILED,
        }
    ),
    ShadowState.ELIGIBILITY_CHECKED: frozenset(
        {
            ShadowState.ROUTE_BOUND,
            ShadowState.REJECTED,
            ShadowState.EXPIRED,
            ShadowState.FAILED,
        }
    ),
    ShadowState.ROUTE_BOUND: frozenset(
        {
            ShadowState.QUOTE_BOUND,
            ShadowState.REJECTED,
            ShadowState.EXPIRED,
            ShadowState.FAILED,
        }
    ),
    ShadowState.QUOTE_BOUND: frozenset(
        {
            ShadowState.PLAN_BUILT,
            ShadowState.REJECTED,
            ShadowState.EXPIRED,
            ShadowState.FAILED,
        }
    ),
    ShadowState.PLAN_BUILT: frozenset(
        {
            ShadowState.SIMULATED,
            ShadowState.EXPIRED,
            ShadowState.FAILED,
        }
    ),
    ShadowState.SIMULATED: frozenset(
        {ShadowState.COMPLETED, ShadowState.FAILED}
    ),
    ShadowState.COMPLETED: frozenset(),
    ShadowState.REJECTED: frozenset(),
    ShadowState.EXPIRED: frozenset(),
    ShadowState.FAILED: frozenset(),
})


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def content_fingerprint(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def deterministic_id(prefix: str, *parts: object) -> str:
    body = "\x1f".join(str(part) for part in parts)
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()[:32]
    return f"{prefix}-{digest}"


def _required_text(label: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} is required")
    return value


def _optional_text(label: str, value: str | None) -> str | None:
    if value is not None:
        _required_text(label, value)
    return value


def _integer(label: str, value: int, *, positive: bool = False) -> int:
    if type(value) is not int:
        raise TypeError(f"{label} must be an integer")
    if positive and value <= 0:
        raise ValueError(f"{label} must be > 0")
    if not positive and value < 0:
        raise ValueError(f"{label} must be >= 0")
    return value


@dataclass(frozen=True, slots=True)
class ExecutionIntentV01:
    candidate_signal_id: str
    candidate_run_id: str
    strategy_evaluation_id: str | None
    strategy_version: str
    parameter_set_id: str
    source_run_id: str | None
    source_event_key: str
    source_ingest_seq: int
    decision_at_us: int
    mint: str
    role: IntentRole
    side: IntentSide
    input_asset: str
    input_amount_base_units: int
    position_id: str | None = None
    parent_entry_intent_id: str | None = None
    exit_decision_id: str | None = None
    exit_track_id: str | None = None
    exit_lifecycle_id: str | None = None
    schema_version: str = field(init=False, default=INTENT_SCHEMA_VERSION)
    intent_id: str = field(init=False)
    fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        role = IntentRole(self.role)
        side = IntentSide(self.side)
        object.__setattr__(self, "role", role)
        object.__setattr__(self, "side", side)

        for label, value in (
            ("candidate_signal_id", self.candidate_signal_id),
            ("candidate_run_id", self.candidate_run_id),
            ("strategy_version", self.strategy_version),
            ("parameter_set_id", self.parameter_set_id),
            ("source_event_key", self.source_event_key),
            ("mint", self.mint),
            ("input_asset", self.input_asset),
        ):
            _required_text(label, value)
        for label, value in (
            ("strategy_evaluation_id", self.strategy_evaluation_id),
            ("source_run_id", self.source_run_id),
            ("position_id", self.position_id),
            ("parent_entry_intent_id", self.parent_entry_intent_id),
            ("exit_decision_id", self.exit_decision_id),
            ("exit_track_id", self.exit_track_id),
            ("exit_lifecycle_id", self.exit_lifecycle_id),
        ):
            _optional_text(label, value)
        _integer("source_ingest_seq", self.source_ingest_seq)
        _integer("decision_at_us", self.decision_at_us)
        _integer(
            "input_amount_base_units",
            self.input_amount_base_units,
            positive=True,
        )

        exit_lineage = (
            self.position_id,
            self.parent_entry_intent_id,
            self.exit_decision_id,
            self.exit_track_id,
            self.exit_lifecycle_id,
        )
        if role is IntentRole.ENTRY:
            if side is not IntentSide.BUY:
                raise ValueError("ENTRY intent must use BUY side")
            if self.strategy_evaluation_id is None:
                raise ValueError("ENTRY intent requires strategy_evaluation_id")
            if any(value is not None for value in exit_lineage):
                raise ValueError("ENTRY intent cannot contain exit lineage")
        else:
            if side is not IntentSide.SELL:
                raise ValueError("EXIT intent must use SELL side")
            if any(value is None for value in exit_lineage):
                raise ValueError(
                    "EXIT intent requires position, parent entry, decision, "
                    "track, and lifecycle lineage"
                )

        payload = self.economic_payload()
        fingerprint = content_fingerprint(payload)
        object.__setattr__(self, "fingerprint", fingerprint)
        object.__setattr__(
            self,
            "intent_id",
            deterministic_id("P5EI", INTENT_SCHEMA_VERSION, fingerprint),
        )

    def economic_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "candidate_signal_id": self.candidate_signal_id,
            "candidate_run_id": self.candidate_run_id,
            "strategy_evaluation_id": self.strategy_evaluation_id,
            "strategy_version": self.strategy_version,
            "parameter_set_id": self.parameter_set_id,
            "source_run_id": self.source_run_id,
            "source_event_key": self.source_event_key,
            "source_ingest_seq": self.source_ingest_seq,
            "decision_at_us": self.decision_at_us,
            "mint": self.mint,
            "role": self.role.value,
            "side": self.side.value,
            "input_asset": self.input_asset,
            "input_amount_base_units": self.input_amount_base_units,
            "position_id": self.position_id,
            "parent_entry_intent_id": self.parent_entry_intent_id,
            "exit_decision_id": self.exit_decision_id,
            "exit_track_id": self.exit_track_id,
            "exit_lifecycle_id": self.exit_lifecycle_id,
        }

    def to_record(self) -> dict[str, Any]:
        return {
            "model_id": MODEL_ID,
            "model_fingerprint": MODEL_FINGERPRINT,
            "intent_id": self.intent_id,
            "fingerprint": self.fingerprint,
            **self.economic_payload(),
        }

    def serialize(self) -> str:
        return canonical_json(self.to_record())

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> ExecutionIntentV01:
        if record.get("model_id") != MODEL_ID:
            raise ValueError("execution intent model_id mismatch")
        if record.get("model_fingerprint") != MODEL_FINGERPRINT:
            raise ValueError("execution intent model fingerprint mismatch")
        if record.get("schema_version") != INTENT_SCHEMA_VERSION:
            raise ValueError("execution intent schema version mismatch")
        intent = cls(
            candidate_signal_id=str(record["candidate_signal_id"]),
            candidate_run_id=str(record["candidate_run_id"]),
            strategy_evaluation_id=record.get("strategy_evaluation_id"),
            strategy_version=str(record["strategy_version"]),
            parameter_set_id=str(record["parameter_set_id"]),
            source_run_id=record.get("source_run_id"),
            source_event_key=str(record["source_event_key"]),
            source_ingest_seq=record["source_ingest_seq"],
            decision_at_us=record["decision_at_us"],
            mint=str(record["mint"]),
            role=IntentRole(str(record["role"])),
            side=IntentSide(str(record["side"])),
            input_asset=str(record["input_asset"]),
            input_amount_base_units=record["input_amount_base_units"],
            position_id=record.get("position_id"),
            parent_entry_intent_id=record.get("parent_entry_intent_id"),
            exit_decision_id=record.get("exit_decision_id"),
            exit_track_id=record.get("exit_track_id"),
            exit_lifecycle_id=record.get("exit_lifecycle_id"),
        )
        if intent.intent_id != record.get("intent_id"):
            raise ValueError("execution intent identity mismatch")
        if intent.fingerprint != record.get("fingerprint"):
            raise ValueError("execution intent content fingerprint mismatch")
        return intent


class InvalidShadowTransition(RuntimeError):
    pass


class InvalidStateTimestamp(InvalidShadowTransition):
    pass


class ShadowDeterminismConflict(RuntimeError):
    pass


class ShadowStateMachineV01:
    @staticmethod
    def validate(current: ShadowState, target: ShadowState) -> None:
        source = ShadowState(current)
        destination = ShadowState(target)
        if destination not in STATE_TRANSITIONS[source]:
            raise InvalidShadowTransition(
                f"shadow state {source.value} -> {destination.value} is not allowed"
            )

    @staticmethod
    def is_terminal(state: ShadowState) -> bool:
        return ShadowState(state) in TERMINAL_STATES


@dataclass(frozen=True, slots=True)
class ShadowTransitionV01:
    transition_id: str
    intent_id: str
    sequence: int
    idempotency_key: str
    from_state: ShadowState | None
    to_state: ShadowState
    effective_at_us: int
    reason_code: str
    evidence_json: str
    fingerprint: str

    @classmethod
    def create(
        cls,
        *,
        intent_id: str,
        sequence: int,
        idempotency_key: str,
        from_state: ShadowState | None,
        to_state: ShadowState,
        effective_at_us: int,
        reason_code: str,
        evidence: Mapping[str, Any],
    ) -> ShadowTransitionV01:
        _required_text("intent_id", intent_id)
        _required_text("idempotency_key", idempotency_key)
        _required_text("reason_code", reason_code)
        _integer("sequence", sequence)
        _integer("effective_at_us", effective_at_us)
        destination = ShadowState(to_state)
        source = None if from_state is None else ShadowState(from_state)
        evidence_json = canonical_json(dict(evidence))
        payload = {
            "schema_version": STATE_MACHINE_VERSION,
            "intent_id": intent_id,
            "sequence": sequence,
            "idempotency_key": idempotency_key,
            "from_state": None if source is None else source.value,
            "to_state": destination.value,
            "effective_at_us": effective_at_us,
            "reason_code": reason_code,
            "evidence_json": evidence_json,
        }
        fingerprint = content_fingerprint(payload)
        return cls(
            transition_id=deterministic_id(
                "P5ST", STATE_MACHINE_VERSION, intent_id, idempotency_key
            ),
            intent_id=intent_id,
            sequence=sequence,
            idempotency_key=idempotency_key,
            from_state=source,
            to_state=destination,
            effective_at_us=effective_at_us,
            reason_code=reason_code,
            evidence_json=evidence_json,
            fingerprint=fingerprint,
        )

    def evidence(self) -> dict[str, Any]:
        value = json.loads(self.evidence_json)
        if not isinstance(value, dict):
            raise ValueError("transition evidence must be a JSON object")
        return value


CONTRACT_SPEC = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "intent_schema_version": INTENT_SCHEMA_VERSION,
    "state_machine_version": STATE_MACHINE_VERSION,
    "candidate_identity": "PHASE2_CANDIDATE_SIGNAL_SIGNAL_ID",
    "entry_cardinality": "ONE_SHARED_ENTRY_PER_CANONICAL_CANDIDATE",
    "economic_quantities": "INTEGER_BASE_UNITS_ONLY",
    "normal_states": [
        ShadowState.CREATED.value,
        ShadowState.ELIGIBILITY_CHECKED.value,
        ShadowState.ROUTE_BOUND.value,
        ShadowState.QUOTE_BOUND.value,
        ShadowState.PLAN_BUILT.value,
        ShadowState.SIMULATED.value,
        ShadowState.COMPLETED.value,
    ],
    "negative_terminal_states": [
        ShadowState.REJECTED.value,
        ShadowState.EXPIRED.value,
        ShadowState.FAILED.value,
    ],
    "persistence_owner": "ISOLATED_DATA_SHADOW_SQLITE",
    "capabilities": [
        "READ_PUBLIC_STATE",
        "CONSTRUCT_UNSIGNED_REPRESENTATION_LATER",
        "SIMULATE_UNSIGNED_REPRESENTATION_LATER",
        "PERSIST_SHADOW_EVIDENCE",
    ],
    "capability_surface": "STRUCTURAL_ALLOWLIST_ONLY",
}
MODEL_FINGERPRINT = content_fingerprint(CONTRACT_SPEC)
