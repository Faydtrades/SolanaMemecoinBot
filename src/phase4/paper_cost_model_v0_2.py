from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from fractions import Fraction


MODEL_SCHEMA_VERSION = "phase4_paper_cost_model_v0.2"
ENGINE_VERSION = "PaperCostModelV02"
BPS_DENOMINATOR = 10_000
LAMPORTS_PER_SOL = 1_000_000_000


class CostSide(StrEnum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"


class PriorityFeeSource(StrEnum):
    LIVE_OBSERVED = "LIVE_OBSERVED"
    LOCKED_FALLBACK = "LOCKED_FALLBACK"


class FillDecision(StrEnum):
    FILLED = "FILLED"
    REJECTED_SLIPPAGE = "REJECTED_SLIPPAGE"


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
class PaperCostBaseline:
    """Versioned Phase-4 paper-execution assumptions.

    Important semantics:
    - slippage_cap_* is a rejection tolerance, NOT a fixed PnL haircut;
    - price impact is supplied at runtime from market/venue state, not invented here;
    - priority fee accepts a live observation when available and otherwise uses the
      locked fallback;
    - builder tip is a separately audited execution overhead;
    - interface_fee_bps is zero for the direct bot baseline (Terminal/Padre UI fees
      are not silently charged to the bot).
    """

    assumption_set_id: str
    scope: str
    reference_position_lamports: int
    default_venue_fee_bps: int
    base_fee_lamports_per_signature: int
    signatures_per_tx: int
    priority_fee_fallback_lamports_per_tx: int
    builder_tip_lamports_per_tx: int
    interface_fee_bps: int
    entry_slippage_cap_bps: int
    exit_slippage_cap_bps: int
    entry_latency_ms: int
    exit_latency_ms: int
    priority_fee_policy: str
    price_impact_policy: str
    latency_calibration_status: str

    def __post_init__(self) -> None:
        if not self.assumption_set_id:
            raise ValueError("assumption_set_id is required")
        if not self.scope:
            raise ValueError("scope is required")
        if self.reference_position_lamports <= 0:
            raise ValueError("reference_position_lamports must be > 0")
        if self.signatures_per_tx <= 0:
            raise ValueError("signatures_per_tx must be > 0")

        non_negative_ints = (
            "default_venue_fee_bps",
            "base_fee_lamports_per_signature",
            "priority_fee_fallback_lamports_per_tx",
            "builder_tip_lamports_per_tx",
            "interface_fee_bps",
            "entry_slippage_cap_bps",
            "exit_slippage_cap_bps",
            "entry_latency_ms",
            "exit_latency_ms",
        )
        for name in non_negative_ints:
            value = getattr(self, name)
            if not isinstance(value, int):
                raise TypeError(f"{name} must be int")
            if value < 0:
                raise ValueError(f"{name} must be >= 0")

        if self.default_venue_fee_bps >= BPS_DENOMINATOR:
            raise ValueError("default_venue_fee_bps must be < 100%")
        if self.interface_fee_bps >= BPS_DENOMINATOR:
            raise ValueError("interface_fee_bps must be < 100%")
        if self.entry_slippage_cap_bps >= BPS_DENOMINATOR:
            raise ValueError("entry_slippage_cap_bps must be < 100%")
        if self.exit_slippage_cap_bps >= BPS_DENOMINATOR:
            raise ValueError("exit_slippage_cap_bps must be < 100%")
        if not self.priority_fee_policy or not self.price_impact_policy:
            raise ValueError("cost policies are required")
        if not self.latency_calibration_status:
            raise ValueError("latency_calibration_status is required")

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
class ManualTerminalReference:
    """Audit-only copy of the user's manual Terminal preset.

    This does NOT drive the bot baseline automatically.
    """

    reference_id: str
    priority_fee_lamports_per_tx: int
    tip_lamports_per_tx: int
    buy_slippage_cap_bps: int
    sell_slippage_cap_bps: int
    mev_mode: str

    def __post_init__(self) -> None:
        if not self.reference_id or not self.mev_mode:
            raise ValueError("manual reference id / mev mode required")
        for name in (
            "priority_fee_lamports_per_tx",
            "tip_lamports_per_tx",
            "buy_slippage_cap_bps",
            "sell_slippage_cap_bps",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative int")


@dataclass(frozen=True, slots=True)
class ExplicitCostBreakdown:
    side: CostSide
    assumption_set_id: str
    model_fingerprint: str
    notional_lamports: int
    venue_fee_bps: int
    venue_fee_lamports: int
    interface_fee_bps: int
    interface_fee_lamports: int
    base_network_fee_lamports: int
    priority_fee_lamports: int
    priority_fee_source: PriorityFeeSource
    builder_tip_lamports: int
    total_explicit_cost_lamports: int


@dataclass(frozen=True, slots=True)
class FillEvaluation:
    side: CostSide
    assumption_set_id: str
    model_fingerprint: str
    signal_reference_price: RationalPrice
    market_price_at_ready: RationalPrice
    simulated_execution_price: RationalPrice
    price_impact_bps: int
    signed_market_move_bps: int
    adverse_slippage_bps: int
    slippage_cap_bps: int
    decision: FillDecision
    rejection_reason: str | None
    signal_observed_at: datetime
    execution_ready_at: datetime
    market_observed_at: datetime


class PaperCostModelV02:
    """Deterministic Phase-4 cost/fill mechanics using a calibrated baseline."""

    def __init__(self, baseline: PaperCostBaseline):
        self.baseline = baseline

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

    @staticmethod
    def _trunc_fraction(value: Fraction) -> int:
        # Python // rounds toward -inf; we want deterministic truncation toward zero.
        if value >= 0:
            return value.numerator // value.denominator
        positive = -value
        return -(positive.numerator // positive.denominator)

    @staticmethod
    def _ceil_positive_fraction(value: Fraction) -> int:
        if value <= 0:
            return 0
        return (value.numerator + value.denominator - 1) // value.denominator

    def _latency_ms(self, side: CostSide) -> int:
        if side is CostSide.ENTRY:
            return self.baseline.entry_latency_ms
        if side is CostSide.EXIT:
            return self.baseline.exit_latency_ms
        raise ValueError(f"unsupported side: {side}")

    def _slippage_cap_bps(self, side: CostSide) -> int:
        if side is CostSide.ENTRY:
            return self.baseline.entry_slippage_cap_bps
        if side is CostSide.EXIT:
            return self.baseline.exit_slippage_cap_bps
        raise ValueError(f"unsupported side: {side}")

    def execution_ready_at(self, signal_observed_at: datetime, side: CostSide) -> datetime:
        self._require_aware(signal_observed_at)
        return signal_observed_at + timedelta(milliseconds=self._latency_ms(side))

    def explicit_costs(
        self,
        *,
        side: CostSide,
        notional_lamports: int,
        observed_priority_fee_lamports: int | None = None,
        venue_fee_bps: int | None = None,
    ) -> ExplicitCostBreakdown:
        if notional_lamports <= 0:
            raise ValueError("notional_lamports must be > 0")
        if observed_priority_fee_lamports is not None and observed_priority_fee_lamports < 0:
            raise ValueError("observed_priority_fee_lamports must be >= 0")

        effective_venue_bps = (
            self.baseline.default_venue_fee_bps
            if venue_fee_bps is None
            else venue_fee_bps
        )
        if not isinstance(effective_venue_bps, int) or not (0 <= effective_venue_bps < BPS_DENOMINATOR):
            raise ValueError("venue_fee_bps must be an int in [0, 10000)")

        if observed_priority_fee_lamports is None:
            priority = self.baseline.priority_fee_fallback_lamports_per_tx
            priority_source = PriorityFeeSource.LOCKED_FALLBACK
        else:
            priority = observed_priority_fee_lamports
            priority_source = PriorityFeeSource.LIVE_OBSERVED

        venue_fee = self._ceil_bps(notional_lamports, effective_venue_bps)
        interface_fee = self._ceil_bps(notional_lamports, self.baseline.interface_fee_bps)
        base_fee = self.baseline.base_fee_lamports_per_signature * self.baseline.signatures_per_tx
        tip = self.baseline.builder_tip_lamports_per_tx
        total = venue_fee + interface_fee + base_fee + priority + tip

        return ExplicitCostBreakdown(
            side=side,
            assumption_set_id=self.baseline.assumption_set_id,
            model_fingerprint=self.baseline.fingerprint,
            notional_lamports=notional_lamports,
            venue_fee_bps=effective_venue_bps,
            venue_fee_lamports=venue_fee,
            interface_fee_bps=self.baseline.interface_fee_bps,
            interface_fee_lamports=interface_fee,
            base_network_fee_lamports=base_fee,
            priority_fee_lamports=priority,
            priority_fee_source=priority_source,
            builder_tip_lamports=tip,
            total_explicit_cost_lamports=total,
        )

    def evaluate_fill(
        self,
        *,
        side: CostSide,
        signal_reference_price: RationalPrice,
        market_price_at_ready: RationalPrice,
        price_impact_bps: int,
        signal_observed_at: datetime,
        market_observed_at: datetime,
    ) -> FillEvaluation:
        self._require_aware(signal_observed_at)
        self._require_aware(market_observed_at)
        if signal_reference_price.identity != market_price_at_ready.identity:
            raise ValueError("price identities must match")
        if not isinstance(price_impact_bps, int) or not (0 <= price_impact_bps < BPS_DENOMINATOR):
            raise ValueError("price_impact_bps must be an int in [0, 10000)")

        ready_at = self.execution_ready_at(signal_observed_at, side)
        if market_observed_at < ready_at:
            raise ValueError("market observation precedes execution_ready_at")

        impact_factor = (
            Fraction(BPS_DENOMINATOR + price_impact_bps, BPS_DENOMINATOR)
            if side is CostSide.ENTRY
            else Fraction(BPS_DENOMINATOR - price_impact_bps, BPS_DENOMINATOR)
        )
        execution_fraction = market_price_at_ready.fraction * impact_factor
        execution_price = RationalPrice.from_fraction(
            signal_reference_price.identity,
            execution_fraction,
        )

        market_move = (
            (market_price_at_ready.fraction / signal_reference_price.fraction) - 1
        ) * BPS_DENOMINATOR
        signed_market_move_bps = self._trunc_fraction(market_move)

        if side is CostSide.ENTRY:
            adverse_fraction = (
                (execution_fraction / signal_reference_price.fraction) - 1
            ) * BPS_DENOMINATOR
        else:
            adverse_fraction = (
                1 - (execution_fraction / signal_reference_price.fraction)
            ) * BPS_DENOMINATOR
        adverse_slippage_bps = self._ceil_positive_fraction(adverse_fraction)

        cap = self._slippage_cap_bps(side)
        if side is CostSide.ENTRY:
            allowed = execution_fraction <= (
                signal_reference_price.fraction
                * Fraction(BPS_DENOMINATOR + cap, BPS_DENOMINATOR)
            )
        else:
            allowed = execution_fraction >= (
                signal_reference_price.fraction
                * Fraction(BPS_DENOMINATOR - cap, BPS_DENOMINATOR)
            )

        decision = FillDecision.FILLED if allowed else FillDecision.REJECTED_SLIPPAGE
        rejection_reason = None if allowed else "SLIPPAGE_CAP_EXCEEDED"

        return FillEvaluation(
            side=side,
            assumption_set_id=self.baseline.assumption_set_id,
            model_fingerprint=self.baseline.fingerprint,
            signal_reference_price=signal_reference_price,
            market_price_at_ready=market_price_at_ready,
            simulated_execution_price=execution_price,
            price_impact_bps=price_impact_bps,
            signed_market_move_bps=signed_market_move_bps,
            adverse_slippage_bps=adverse_slippage_bps,
            slippage_cap_bps=cap,
            decision=decision,
            rejection_reason=rejection_reason,
            signal_observed_at=signal_observed_at,
            execution_ready_at=ready_at,
            market_observed_at=market_observed_at,
        )


def canonical_digest(*, baseline: PaperCostBaseline, costs: list[ExplicitCostBreakdown], fills: list[FillEvaluation]) -> str:
    payload: dict[str, object] = {
        "baseline": json.loads(baseline.canonical_json()),
        "costs": [],
        "fills": [],
    }

    cost_rows: list[dict[str, object]] = []
    for cost in costs:
        row = asdict(cost)
        row["side"] = cost.side.value
        row["priority_fee_source"] = cost.priority_fee_source.value
        cost_rows.append(row)
    payload["costs"] = cost_rows

    fill_rows: list[dict[str, object]] = []
    for fill in fills:
        fill_rows.append(
            {
                "side": fill.side.value,
                "assumption_set_id": fill.assumption_set_id,
                "model_fingerprint": fill.model_fingerprint,
                "signal_price": [fill.signal_reference_price.identity, fill.signal_reference_price.numerator_raw, fill.signal_reference_price.denominator_raw],
                "market_price": [fill.market_price_at_ready.identity, fill.market_price_at_ready.numerator_raw, fill.market_price_at_ready.denominator_raw],
                "execution_price": [fill.simulated_execution_price.identity, fill.simulated_execution_price.numerator_raw, fill.simulated_execution_price.denominator_raw],
                "price_impact_bps": fill.price_impact_bps,
                "signed_market_move_bps": fill.signed_market_move_bps,
                "adverse_slippage_bps": fill.adverse_slippage_bps,
                "slippage_cap_bps": fill.slippage_cap_bps,
                "decision": fill.decision.value,
                "rejection_reason": fill.rejection_reason,
                "signal_observed_at": fill.signal_observed_at.isoformat(timespec="microseconds"),
                "execution_ready_at": fill.execution_ready_at.isoformat(timespec="microseconds"),
                "market_observed_at": fill.market_observed_at.isoformat(timespec="microseconds"),
            }
        )
    payload["fills"] = fill_rows

    body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()
