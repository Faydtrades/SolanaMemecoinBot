from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from fractions import Fraction


MODEL_SCHEMA_VERSION = "phase4_paper_cost_model_v0.1"
ENGINE_VERSION = "PaperCostModelV01"
BPS_DENOMINATOR = 10_000


class CostSide(StrEnum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"


@dataclass(frozen=True, slots=True)
class PaperCostAssumptions:
    """Immutable deterministic assumptions for Phase-4 paper execution friction.

    This object deliberately separates *model mechanics* from *calibration*.
    Numeric values must be supplied by a named assumption set; this module does
    not silently invent a production/research baseline.

    Slippage and price impact are represented separately for auditability even
    though both worsen the simulated execution price in v0.1. Latency is kept
    as an explicit timing assumption. The live router (later Phase 4 step) must
    use it to choose an eligible observed market reference at/after ready_at;
    this module never looks ahead or fabricates a latency-moved market price.
    """

    assumption_set_id: str
    entry_fee_bps: int
    exit_fee_bps: int
    entry_slippage_bps: int
    exit_slippage_bps: int
    entry_price_impact_bps: int
    exit_price_impact_bps: int
    base_network_fee_lamports_per_tx: int
    priority_fee_lamports_per_tx: int
    entry_latency_ms: int
    exit_latency_ms: int

    def __post_init__(self) -> None:
        if not self.assumption_set_id:
            raise ValueError("assumption_set_id is required")
        for name in (
            "entry_fee_bps",
            "exit_fee_bps",
            "entry_slippage_bps",
            "exit_slippage_bps",
            "entry_price_impact_bps",
            "exit_price_impact_bps",
            "base_network_fee_lamports_per_tx",
            "priority_fee_lamports_per_tx",
            "entry_latency_ms",
            "exit_latency_ms",
        ):
            value = getattr(self, name)
            if not isinstance(value, int):
                raise TypeError(f"{name} must be int")
            if value < 0:
                raise ValueError(f"{name} must be >= 0")

        if self.entry_slippage_bps + self.entry_price_impact_bps >= BPS_DENOMINATOR:
            raise ValueError("entry slippage + impact must be < 100%")
        if self.exit_slippage_bps + self.exit_price_impact_bps >= BPS_DENOMINATOR:
            raise ValueError("exit slippage + impact must be < 100%")

    @property
    def fingerprint(self) -> str:
        payload = {
            "schema_version": MODEL_SCHEMA_VERSION,
            "engine_version": ENGINE_VERSION,
            **asdict(self),
        }
        body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(body.encode("utf-8")).hexdigest()

    def canonical_json(self) -> str:
        payload = {
            "schema_version": MODEL_SCHEMA_VERSION,
            "engine_version": ENGINE_VERSION,
            **asdict(self),
            "fingerprint": self.fingerprint,
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class RationalPrice:
    identity: str
    numerator_raw: int
    denominator_raw: int

    def __post_init__(self) -> None:
        if not self.identity:
            raise ValueError("price identity is required")
        if self.numerator_raw <= 0 or self.denominator_raw <= 0:
            raise ValueError("price numerator/denominator must be > 0")
        canonical = Fraction(self.numerator_raw, self.denominator_raw)
        object.__setattr__(self, "numerator_raw", canonical.numerator)
        object.__setattr__(self, "denominator_raw", canonical.denominator)

    @property
    def fraction(self) -> Fraction:
        return Fraction(self.numerator_raw, self.denominator_raw)

    @classmethod
    def from_fraction(cls, identity: str, value: Fraction) -> "RationalPrice":
        if value <= 0:
            raise ValueError("price must be > 0")
        return cls(identity, value.numerator, value.denominator)


@dataclass(frozen=True, slots=True)
class SideCostQuote:
    side: CostSide
    assumption_set_id: str
    model_fingerprint: str
    reference_price: RationalPrice
    simulated_execution_price: RationalPrice
    notional_lamports: int
    variable_fee_bps: int
    variable_fee_lamports: int
    base_network_fee_lamports: int
    priority_fee_lamports: int
    explicit_cost_lamports: int
    slippage_bps: int
    price_impact_bps: int
    total_price_friction_bps: int
    latency_ms: int
    reference_observed_at: datetime
    execution_ready_at: datetime


class PaperCostModelV01:
    def __init__(self, assumptions: PaperCostAssumptions):
        self.assumptions = assumptions

    @staticmethod
    def _require_aware(value: datetime) -> None:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")

    @staticmethod
    def _ceil_bps(notional_lamports: int, bps: int) -> int:
        if notional_lamports <= 0:
            raise ValueError("notional_lamports must be > 0")
        if bps < 0:
            raise ValueError("bps must be >= 0")
        return (notional_lamports * bps + BPS_DENOMINATOR - 1) // BPS_DENOMINATOR

    def _side_values(self, side: CostSide) -> tuple[int, int, int, int]:
        a = self.assumptions
        if side is CostSide.ENTRY:
            return (
                a.entry_fee_bps,
                a.entry_slippage_bps,
                a.entry_price_impact_bps,
                a.entry_latency_ms,
            )
        if side is CostSide.EXIT:
            return (
                a.exit_fee_bps,
                a.exit_slippage_bps,
                a.exit_price_impact_bps,
                a.exit_latency_ms,
            )
        raise ValueError(f"unsupported side: {side}")

    def execution_ready_at(self, observed_at: datetime, side: CostSide) -> datetime:
        self._require_aware(observed_at)
        _, _, _, latency_ms = self._side_values(side)
        return observed_at + timedelta(milliseconds=latency_ms)

    def adjust_price(self, reference_price: RationalPrice, side: CostSide) -> RationalPrice:
        _, slippage_bps, impact_bps, _ = self._side_values(side)
        friction_bps = slippage_bps + impact_bps
        factor = (
            Fraction(BPS_DENOMINATOR + friction_bps, BPS_DENOMINATOR)
            if side is CostSide.ENTRY
            else Fraction(BPS_DENOMINATOR - friction_bps, BPS_DENOMINATOR)
        )
        return RationalPrice.from_fraction(reference_price.identity, reference_price.fraction * factor)

    def quote_side(
        self,
        *,
        side: CostSide,
        reference_price: RationalPrice,
        notional_lamports: int,
        reference_observed_at: datetime,
    ) -> SideCostQuote:
        self._require_aware(reference_observed_at)
        if notional_lamports <= 0:
            raise ValueError("notional_lamports must be > 0")

        fee_bps, slippage_bps, impact_bps, latency_ms = self._side_values(side)
        variable_fee = self._ceil_bps(notional_lamports, fee_bps)
        base_fee = self.assumptions.base_network_fee_lamports_per_tx
        priority_fee = self.assumptions.priority_fee_lamports_per_tx
        explicit_cost = variable_fee + base_fee + priority_fee
        execution_price = self.adjust_price(reference_price, side)
        ready_at = reference_observed_at + timedelta(milliseconds=latency_ms)

        return SideCostQuote(
            side=side,
            assumption_set_id=self.assumptions.assumption_set_id,
            model_fingerprint=self.assumptions.fingerprint,
            reference_price=reference_price,
            simulated_execution_price=execution_price,
            notional_lamports=notional_lamports,
            variable_fee_bps=fee_bps,
            variable_fee_lamports=variable_fee,
            base_network_fee_lamports=base_fee,
            priority_fee_lamports=priority_fee,
            explicit_cost_lamports=explicit_cost,
            slippage_bps=slippage_bps,
            price_impact_bps=impact_bps,
            total_price_friction_bps=slippage_bps + impact_bps,
            latency_ms=latency_ms,
            reference_observed_at=reference_observed_at,
            execution_ready_at=ready_at,
        )


def canonical_quote_digest(quotes: list[SideCostQuote]) -> str:
    rows: list[dict[str, object]] = []
    for quote in quotes:
        rows.append(
            {
                "side": quote.side.value,
                "assumption_set_id": quote.assumption_set_id,
                "model_fingerprint": quote.model_fingerprint,
                "reference_price_identity": quote.reference_price.identity,
                "reference_price_numerator_raw": quote.reference_price.numerator_raw,
                "reference_price_denominator_raw": quote.reference_price.denominator_raw,
                "execution_price_identity": quote.simulated_execution_price.identity,
                "execution_price_numerator_raw": quote.simulated_execution_price.numerator_raw,
                "execution_price_denominator_raw": quote.simulated_execution_price.denominator_raw,
                "notional_lamports": quote.notional_lamports,
                "variable_fee_bps": quote.variable_fee_bps,
                "variable_fee_lamports": quote.variable_fee_lamports,
                "base_network_fee_lamports": quote.base_network_fee_lamports,
                "priority_fee_lamports": quote.priority_fee_lamports,
                "explicit_cost_lamports": quote.explicit_cost_lamports,
                "slippage_bps": quote.slippage_bps,
                "price_impact_bps": quote.price_impact_bps,
                "total_price_friction_bps": quote.total_price_friction_bps,
                "latency_ms": quote.latency_ms,
                "reference_observed_at": quote.reference_observed_at.isoformat(timespec="microseconds"),
                "execution_ready_at": quote.execution_ready_at.isoformat(timespec="microseconds"),
            }
        )
    body = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()
