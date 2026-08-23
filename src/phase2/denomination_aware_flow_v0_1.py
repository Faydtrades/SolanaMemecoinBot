from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_EVEN, getcontext
from enum import Enum
from hashlib import sha256
import json
from typing import Optional, Sequence

from .models_v0_1 import (
    DataQualityState,
    EventType,
    IdentityState,
    IngestionSource,
    LifecycleState,
    MigrationState,
    PriceStructureState,
)
from .quote_aware_price_v0_1 import (
    PriceIdentity,
    PricePathKind,
    QuoteAwareNormalizedEvent,
    QuoteAwarePriceEngineV01,
)

getcontext().prec = 60

SCHEMA_VERSION = "DAF-0.1"
MARKET_STATE_SCHEMA_VERSION = "MS-DAF-0.1"
FLOW_SCALE_PPM = 1_000_000

WINDOWS_US: dict[str, int] = {
    "1s": 1_000_000,
    "3s": 3_000_000,
    "10s": 10_000_000,
    "30s": 30_000_000,
    "60s": 60_000_000,
}


class FlowCompatibility(str, Enum):
    COMPATIBLE = "COMPATIBLE"
    UNAVAILABLE = "UNAVAILABLE"
    IDENTITY_CONFLICT = "IDENTITY_CONFLICT"


@dataclass(frozen=True, slots=True)
class DenominationAwareFlowPoint:
    identity: PriceIdentity
    raw_amount: int | None
    reserve_raw: int | None
    reserve_fraction_ppm: int | None
    signed_reserve_fraction_ppm: int | None
    event_key: str
    observed_at_us: int
    ingest_seq: int


@dataclass(frozen=True, slots=True)
class DenominationAwareWindowFeatures:
    window_id: str
    start_at_us: int | None
    end_at_us: int
    event_count: int
    trade_count: int
    buys: int
    sells: int

    unique_buyers: int
    unique_sellers: int
    active_wallets: int

    price_identity: PriceIdentity
    open_price_proxy: Decimal | None
    high_price_proxy: Decimal | None
    low_price_proxy: Decimal | None
    close_price_proxy: Decimal | None
    price_change_bps: int | None
    high_low_range_bps: int | None

    # Raw denomination-preserving volumes.  Only one denomination is expected to
    # be authoritative for a stable token price identity, but both are retained
    # for auditability and exact Phase-1 provenance.
    sol_buy_volume_lamports: int
    sol_sell_volume_lamports: int
    quote_buy_volume_raw: int
    quote_sell_volume_raw: int

    flow_identity: PriceIdentity
    flow_compatibility: FlowCompatibility
    buy_flow_raw: int
    sell_flow_raw: int
    net_flow_raw: int
    normalized_flow_event_count: int
    flow_unavailable_event_count: int
    buy_flow_reserve_ppm: int
    sell_flow_reserve_ppm: int
    net_flow_reserve_ppm: int

    trades_per_second: Decimal
    buy_rate_per_second: Decimal
    sell_rate_per_second: Decimal


@dataclass(frozen=True, slots=True)
class DenominationAwareMarketCoreState:
    current_price_proxy: Decimal | None
    price_identity: PriceIdentity
    virtual_sol_reserve_lamports: int | None
    virtual_quote_reserve_raw: int | None
    virtual_token_reserve_raw: int | None
    real_sol_reserve_lamports: int | None
    real_quote_reserve_raw: int | None
    real_token_reserve_raw: int | None


@dataclass(frozen=True, slots=True)
class DenominationAwareMarketState:
    schema_version: str
    mint: str
    state_seq: int
    as_of_observed_at_us: int
    triggering_event_key: str
    identity: IdentityState
    market: DenominationAwareMarketCoreState
    price_structure: PriceStructureState
    windows: dict[str, DenominationAwareWindowFeatures]
    lifecycle: LifecycleState
    data_quality: DataQualityState
    flow_identity_conflict_seen: bool


@dataclass(slots=True)
class _TokenAccumulator:
    mint: str
    known_events: list[QuoteAwareNormalizedEvent] = field(default_factory=list)
    state_seq: int = 0
    last_order_key: tuple[int, int] | None = None
    token_created_at_us: int | None = None
    first_tradable_event_at_us: int | None = None
    canonical_identity: PriceIdentity | None = None
    first_tradable_price_proxy: Decimal | None = None
    last_fresh_priced_event: QuoteAwareNormalizedEvent | None = None
    launch_seen: bool = False
    creator: str | None = None
    flow_identity_conflict_seen: bool = False


def _canonical_json(value: object) -> str:
    def conv(v: object):
        if isinstance(v, Enum):
            return v.value
        if isinstance(v, Decimal):
            return str(v)
        if hasattr(v, "__dataclass_fields__"):
            return {k: conv(getattr(v, k)) for k in v.__dataclass_fields__}
        if isinstance(v, dict):
            return {str(k): conv(x) for k, x in v.items()}
        if isinstance(v, (list, tuple, set)):
            return [conv(x) for x in v]
        return v

    return json.dumps(conv(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def stable_sha256(value: object) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _same_identity(a: PriceIdentity, b: PriceIdentity) -> bool:
    return a.path == b.path and a.quote_mint == b.quote_mint


def _bps(current: Decimal, reference: Decimal) -> int | None:
    if reference == 0:
        return None
    raw = ((current / reference) - Decimal(1)) * Decimal(10_000)
    return int(raw.quantize(Decimal("1"), rounding=ROUND_HALF_EVEN))


def _ratio_ppm(amount: int | None, reserve: int | None) -> int | None:
    """Dimensionless reserve-normalized flow proxy in parts-per-million.

    The numerator and denominator are always raw units of the SAME denomination:
      SOL path   -> lamports / virtual_sol_reserve_lamports
      QUOTE path -> quote_amount_raw / virtual_quote_reserve_raw

    Decimal scaling therefore cancels.  This is a causal reserve-relative flow
    proxy, not an exchange-rate conversion and not a claim of identical economic
    value across denominations.
    """

    if amount is None or reserve is None or amount < 0 or reserve <= 0:
        return None
    raw = (Decimal(amount) / Decimal(reserve)) * Decimal(FLOW_SCALE_PPM)
    return int(raw.quantize(Decimal("1"), rounding=ROUND_HALF_EVEN))


def flow_point(event: QuoteAwareNormalizedEvent, identity: PriceIdentity) -> DenominationAwareFlowPoint:
    base = event.base
    if identity.path == PricePathKind.SOL:
        amount = base.sol_amount_lamports
        reserve = base.virtual_sol_reserve_lamports
    elif identity.path == PricePathKind.QUOTE:
        if event.quote_mint != identity.quote_mint:
            return DenominationAwareFlowPoint(
                identity=PriceIdentity(PricePathKind.UNAVAILABLE, event.quote_mint),
                raw_amount=None,
                reserve_raw=None,
                reserve_fraction_ppm=None,
                signed_reserve_fraction_ppm=None,
                event_key=base.event_key,
                observed_at_us=base.observed_at_us,
                ingest_seq=base.ingest_seq,
            )
        amount = event.quote_amount_raw
        reserve = event.virtual_quote_reserve_raw
    else:
        amount = reserve = None

    ppm = _ratio_ppm(amount, reserve)
    signed = None
    if ppm is not None:
        signed = ppm if base.event_type == EventType.BUY else -ppm
    return DenominationAwareFlowPoint(
        identity=identity,
        raw_amount=amount,
        reserve_raw=reserve,
        reserve_fraction_ppm=ppm,
        signed_reserve_fraction_ppm=signed,
        event_key=base.event_key,
        observed_at_us=base.observed_at_us,
        ingest_seq=base.ingest_seq,
    )


class DenominationAwareFeatureEngineV01:
    """Causal BOT_TRUTH feature engine with price + flow denomination identity.

    It is additive to the already validated FeatureEngineV02 / quote-aware price
    work.  SOL raw lamport flow remains available exactly as before, quote raw flow
    remains quote-denominated, and a separate dimensionless reserve-relative flow
    feature is computed for strategy compatibility across stable price identities.

    Important: `net_flow_reserve_ppm` is NOT SOL-equivalent volume.  It is a
    reserve-relative pressure proxy and must receive its own parameter search.
    """

    schema_version = MARKET_STATE_SCHEMA_VERSION

    def __init__(self) -> None:
        self._tokens: dict[str, _TokenAccumulator] = {}

    @staticmethod
    def _causal_order(event: QuoteAwareNormalizedEvent) -> tuple[int, int]:
        return (event.base.observed_at_us, event.base.ingest_seq)

    @staticmethod
    def _price_point(event: QuoteAwareNormalizedEvent):
        return QuoteAwarePriceEngineV01.point(event)

    def process(self, event: QuoteAwareNormalizedEvent) -> DenominationAwareMarketState:
        base = event.base
        acc = self._tokens.setdefault(base.mint, _TokenAccumulator(mint=base.mint))
        order_key = self._causal_order(event)
        if acc.last_order_key is not None and order_key <= acc.last_order_key:
            raise ValueError(
                f"Non-monotonic observed processing order for {base.mint}: "
                f"{order_key} <= {acc.last_order_key}"
            )
        acc.last_order_key = order_key
        acc.known_events.append(event)
        acc.state_seq += 1

        if base.event_type == EventType.LAUNCH:
            acc.launch_seen = True
            if base.creator:
                acc.creator = base.creator
            if acc.token_created_at_us is None:
                acc.token_created_at_us = base.event_at_us

        if base.event_type in (EventType.BUY, EventType.SELL):
            point = self._price_point(event)
            if acc.first_tradable_event_at_us is None:
                acc.first_tradable_event_at_us = base.observed_at_us
                if point.price_proxy is not None:
                    acc.canonical_identity = point.identity
                    acc.first_tradable_price_proxy = point.price_proxy

            if point.price_proxy is not None:
                if acc.canonical_identity is None:
                    acc.canonical_identity = point.identity
                    if acc.first_tradable_price_proxy is None:
                        acc.first_tradable_price_proxy = point.price_proxy
                elif not _same_identity(acc.canonical_identity, point.identity):
                    acc.flow_identity_conflict_seen = True

                if (
                    acc.canonical_identity is not None
                    and _same_identity(acc.canonical_identity, point.identity)
                    and base.source != IngestionSource.GAP_RECOVERY
                ):
                    acc.last_fresh_priced_event = event

        return self._build_state(acc, event)

    def _tradable_known_since_t0(self, acc: _TokenAccumulator) -> list[QuoteAwareNormalizedEvent]:
        t0 = acc.first_tradable_event_at_us
        if t0 is None:
            return []
        return [
            e
            for e in acc.known_events
            if e.base.event_type in (EventType.BUY, EventType.SELL)
            and e.base.observed_at_us >= t0
        ]

    def _build_window(
        self,
        acc: _TokenAccumulator,
        window_id: str,
        events: list[QuoteAwareNormalizedEvent],
        end_at_us: int,
        start_at_us: int | None,
        duration_us: int | None,
    ) -> DenominationAwareWindowFeatures:
        if start_at_us is None:
            selected: list[QuoteAwareNormalizedEvent] = []
        elif duration_us is None:
            selected = [e for e in events if start_at_us <= e.base.observed_at_us <= end_at_us]
        else:
            selected = [
                e
                for e in events
                if e.base.source != IngestionSource.GAP_RECOVERY
                and start_at_us < e.base.observed_at_us <= end_at_us
            ]
        selected.sort(key=self._causal_order)

        buys = [e for e in selected if e.base.event_type == EventType.BUY]
        sells = [e for e in selected if e.base.event_type == EventType.SELL]
        trades = buys + sells

        unique_buyers = {e.base.user for e in buys if e.base.user}
        unique_sellers = {e.base.user for e in sells if e.base.user}
        active_wallets = unique_buyers | unique_sellers

        sol_buy = sum(e.base.sol_amount_lamports or 0 for e in buys)
        sol_sell = sum(e.base.sol_amount_lamports or 0 for e in sells)
        quote_buy = sum(e.quote_amount_raw or 0 for e in buys)
        quote_sell = sum(e.quote_amount_raw or 0 for e in sells)

        canonical = acc.canonical_identity or PriceIdentity(PricePathKind.UNAVAILABLE, None)
        price_pairs: list[tuple[QuoteAwareNormalizedEvent, Decimal]] = []
        flow_points: list[DenominationAwareFlowPoint] = []
        unavailable_flow = 0
        identity_conflict = acc.flow_identity_conflict_seen

        for event in selected:
            p = self._price_point(event)
            if p.price_proxy is not None:
                if canonical.path == PricePathKind.UNAVAILABLE:
                    canonical = p.identity
                if _same_identity(canonical, p.identity):
                    price_pairs.append((event, p.price_proxy))
                else:
                    identity_conflict = True

            fp = flow_point(event, canonical)
            if fp.reserve_fraction_ppm is None:
                unavailable_flow += 1
            elif _same_identity(canonical, fp.identity):
                flow_points.append(fp)
            else:
                identity_conflict = True
                unavailable_flow += 1

        open_p = price_pairs[0][1] if price_pairs else None
        close_p = price_pairs[-1][1] if price_pairs else None
        high_p = max((p for _, p in price_pairs), default=None)
        low_p = min((p for _, p in price_pairs), default=None)
        price_change_bps = _bps(close_p, open_p) if open_p is not None and close_p is not None else None
        high_low_range_bps = _bps(high_p, low_p) if high_p is not None and low_p is not None else None

        buy_flow_raw = sum(fp.raw_amount or 0 for fp in flow_points if fp.signed_reserve_fraction_ppm is not None and fp.signed_reserve_fraction_ppm >= 0)
        sell_flow_raw = sum(fp.raw_amount or 0 for fp in flow_points if fp.signed_reserve_fraction_ppm is not None and fp.signed_reserve_fraction_ppm < 0)
        buy_ppm = sum(fp.reserve_fraction_ppm or 0 for fp in flow_points if fp.signed_reserve_fraction_ppm is not None and fp.signed_reserve_fraction_ppm >= 0)
        sell_ppm = sum(fp.reserve_fraction_ppm or 0 for fp in flow_points if fp.signed_reserve_fraction_ppm is not None and fp.signed_reserve_fraction_ppm < 0)

        if identity_conflict:
            compatibility = FlowCompatibility.IDENTITY_CONFLICT
        elif canonical.path == PricePathKind.UNAVAILABLE or len(flow_points) == 0:
            compatibility = FlowCompatibility.UNAVAILABLE
        else:
            compatibility = FlowCompatibility.COMPATIBLE

        if start_at_us is None:
            seconds = Decimal(0)
        else:
            seconds = Decimal(max(0, end_at_us - start_at_us)) / Decimal(1_000_000)
        if seconds > 0:
            trade_rate = Decimal(len(trades)) / seconds
            buy_rate = Decimal(len(buys)) / seconds
            sell_rate = Decimal(len(sells)) / seconds
        else:
            trade_rate = buy_rate = sell_rate = Decimal(0)

        return DenominationAwareWindowFeatures(
            window_id=window_id,
            start_at_us=start_at_us,
            end_at_us=end_at_us,
            event_count=len(selected),
            trade_count=len(trades),
            buys=len(buys),
            sells=len(sells),
            unique_buyers=len(unique_buyers),
            unique_sellers=len(unique_sellers),
            active_wallets=len(active_wallets),
            price_identity=canonical,
            open_price_proxy=open_p,
            high_price_proxy=high_p,
            low_price_proxy=low_p,
            close_price_proxy=close_p,
            price_change_bps=price_change_bps,
            high_low_range_bps=high_low_range_bps,
            sol_buy_volume_lamports=sol_buy,
            sol_sell_volume_lamports=sol_sell,
            quote_buy_volume_raw=quote_buy,
            quote_sell_volume_raw=quote_sell,
            flow_identity=canonical,
            flow_compatibility=compatibility,
            buy_flow_raw=buy_flow_raw,
            sell_flow_raw=sell_flow_raw,
            net_flow_raw=buy_flow_raw - sell_flow_raw,
            normalized_flow_event_count=len(flow_points),
            flow_unavailable_event_count=unavailable_flow,
            buy_flow_reserve_ppm=buy_ppm,
            sell_flow_reserve_ppm=sell_ppm,
            net_flow_reserve_ppm=buy_ppm - sell_ppm,
            trades_per_second=trade_rate,
            buy_rate_per_second=buy_rate,
            sell_rate_per_second=sell_rate,
        )

    def _build_state(
        self, acc: _TokenAccumulator, trigger: QuoteAwareNormalizedEvent
    ) -> DenominationAwareMarketState:
        base = trigger.base
        t_decision = base.observed_at_us
        tradable = self._tradable_known_since_t0(acc)
        canonical = acc.canonical_identity or PriceIdentity(PricePathKind.UNAVAILABLE, None)

        latest = acc.last_fresh_priced_event
        current_price: Decimal | None = None
        if latest is not None:
            p = self._price_point(latest)
            if _same_identity(canonical, p.identity):
                current_price = p.price_proxy

        if current_price is None:
            comparable_fresh = []
            for e in tradable:
                if e.base.source == IngestionSource.GAP_RECOVERY:
                    continue
                p = self._price_point(e)
                if p.price_proxy is not None and _same_identity(canonical, p.identity):
                    comparable_fresh.append((e, p.price_proxy))
            if comparable_fresh:
                latest, current_price = max(comparable_fresh, key=lambda ep: self._causal_order(ep[0]))

        price_pairs = []
        for e in tradable:
            p = self._price_point(e)
            if p.price_proxy is not None and _same_identity(canonical, p.identity):
                price_pairs.append((e, p.price_proxy))

        if price_pairs:
            high_event, high_price = max(price_pairs, key=lambda ep: (ep[1], -ep[0].base.event_at_us))
            low_event, low_price = min(price_pairs, key=lambda ep: (ep[1], ep[0].base.event_at_us))
        else:
            high_event = low_event = None
            high_price = low_price = None

        first_price = acc.first_tradable_price_proxy
        ret = _bps(current_price, first_price) if current_price is not None and first_price is not None else None
        dd = _bps(current_price, high_price) if current_price is not None and high_price is not None else None

        windows: dict[str, DenominationAwareWindowFeatures] = {}
        for window_id, width_us in WINDOWS_US.items():
            windows[window_id] = self._build_window(
                acc,
                window_id,
                tradable,
                t_decision,
                t_decision - width_us,
                width_us,
            )
        windows["since_t0"] = self._build_window(
            acc,
            "since_t0",
            tradable,
            t_decision,
            acc.first_tradable_event_at_us,
            None,
        )

        age_ms = (
            max(0, (t_decision - acc.first_tradable_event_at_us) // 1_000)
            if acc.first_tradable_event_at_us is not None
            else None
        )
        gap_seen = any(e.base.source == IngestionSource.GAP_RECOVERY for e in acc.known_events)
        state_valid = (
            base.source != IngestionSource.GAP_RECOVERY
            and not acc.flow_identity_conflict_seen
            and canonical.path != PricePathKind.UNAVAILABLE
        )

        latest_price_event = latest
        latest_quote_reserve = latest_price_event.virtual_quote_reserve_raw if latest_price_event else None
        latest_real_quote = latest_price_event.real_quote_reserve_raw if latest_price_event else None

        return DenominationAwareMarketState(
            schema_version=self.schema_version,
            mint=acc.mint,
            state_seq=acc.state_seq,
            as_of_observed_at_us=t_decision,
            triggering_event_key=base.event_key,
            identity=IdentityState(
                token_created_at_us=acc.token_created_at_us,
                first_tradable_event_at_us=acc.first_tradable_event_at_us,
                age_ms=age_ms,
            ),
            market=DenominationAwareMarketCoreState(
                current_price_proxy=current_price,
                price_identity=canonical,
                virtual_sol_reserve_lamports=(
                    latest_price_event.base.virtual_sol_reserve_lamports if latest_price_event else None
                ),
                virtual_quote_reserve_raw=latest_quote_reserve,
                virtual_token_reserve_raw=(
                    latest_price_event.base.virtual_token_reserve_raw if latest_price_event else None
                ),
                real_sol_reserve_lamports=(
                    latest_price_event.base.real_sol_reserve_lamports if latest_price_event else None
                ),
                real_quote_reserve_raw=latest_real_quote,
                real_token_reserve_raw=(
                    latest_price_event.base.real_token_reserve_raw if latest_price_event else None
                ),
            ),
            price_structure=PriceStructureState(
                first_price_proxy=first_price,
                high_since_t0=high_price,
                high_since_t0_at_us=high_event.base.event_at_us if high_event else None,
                low_since_t0=low_price,
                low_since_t0_at_us=low_event.base.event_at_us if low_event else None,
                return_from_t0_bps=ret,
                drawdown_from_high_bps=dd,
            ),
            windows=windows,
            lifecycle=LifecycleState(
                launch_seen=acc.launch_seen,
                creator=acc.creator,
                migration_state=MigrationState.PUMP,
            ),
            data_quality=DataQualityState(
                last_event_at_us=base.event_at_us,
                last_observed_at_us=base.observed_at_us,
                event_count_since_t0=len(tradable),
                known_gap_affecting_state=gap_seen,
                state_valid=state_valid,
            ),
            flow_identity_conflict_seen=acc.flow_identity_conflict_seen,
        )


def entry_window_events(
    events: Sequence[QuoteAwareNormalizedEvent], *, max_age_ms: int = 300_000
) -> list[QuoteAwareNormalizedEvent]:
    trades = [e for e in events if e.base.event_type in (EventType.BUY, EventType.SELL)]
    if not trades:
        return []
    t0 = trades[0].base.observed_at_us
    deadline = t0 + max_age_ms * 1_000
    return [
        e
        for e in events
        if e.base.event_type in (EventType.LAUNCH, EventType.BUY, EventType.SELL)
        and e.base.observed_at_us <= deadline
    ]
