from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_EVEN, getcontext
from typing import Optional

from .models_v0_1 import (
    DataQualityState,
    EventType,
    IdentityState,
    IngestionSource,
    LifecycleState,
    MarketCoreState,
    MarketState,
    MigrationState,
    NormalizedMarketEvent,
    PriceStructureState,
    WindowFeatures,
)

getcontext().prec = 50

WINDOWS_US: dict[str, int] = {
    "1s": 1_000_000,
    "3s": 3_000_000,
    "10s": 10_000_000,
    "30s": 30_000_000,
    "60s": 60_000_000,
}


@dataclass(slots=True)
class _TokenAccumulator:
    mint: str
    known_events: list[NormalizedMarketEvent] = field(default_factory=list)
    state_seq: int = 0
    last_order_key: Optional[tuple[int, int]] = None
    token_created_at_us: Optional[int] = None
    # Locked Phase-0 meaning: t=0 is when the first tradable event was OBSERVED.
    first_tradable_event_at_us: Optional[int] = None
    first_tradable_price_proxy: Optional[Decimal] = None
    last_fresh_priced_event: Optional[NormalizedMarketEvent] = None
    launch_seen: bool = False
    creator: Optional[str] = None


class FeatureEngineV02:
    """Causal BOT_TRUTH Feature Engine with explicit observation-clock semantics.

    v0.2 corrects an implementation mismatch discovered by real-data harness v0.1:
    Phase 0 defines t=0 as the first *observed* tradable event, but FeatureEngineV01
    anchored t=0 and short windows to Pump's embedded event timestamp. Real data shows
    that embedded timestamp can differ materially from WebSocket receipt time.

    BOT_TRUTH v0.2 rules:
    * Processing order is (observed_at_us, ingest_seq).
    * t=0 is observed_at_us of the first BUY/SELL the bot observes.
    * 1s/3s/10s/30s/60s flow windows use observed_at_us for fresh live/replay events.
    * GAP_RECOVERY events are known after recovery and may enrich since_t0/history,
      but never masquerade as fresh short-window flow and never replace current price.
    * A state triggered directly by GAP_RECOVERY is marked state_valid=False; the next
      fresh observation can produce a valid state using the now-known history.
    """

    schema_version = "MS-0.2"

    def __init__(self) -> None:
        self._tokens: dict[str, _TokenAccumulator] = {}

    @staticmethod
    def _price_proxy(event: NormalizedMarketEvent) -> Optional[Decimal]:
        v_sol = event.virtual_sol_reserve_lamports
        v_tok = event.virtual_token_reserve_raw
        if v_sol is None or v_tok is None or v_sol <= 0 or v_tok <= 0:
            return None
        return Decimal(v_sol) / Decimal(v_tok)

    @staticmethod
    def _bps(current: Decimal, reference: Decimal) -> Optional[int]:
        if reference == 0:
            return None
        raw = ((current / reference) - Decimal(1)) * Decimal(10_000)
        return int(raw.quantize(Decimal("1"), rounding=ROUND_HALF_EVEN))

    @staticmethod
    def _causal_order(event: NormalizedMarketEvent) -> tuple[int, int]:
        return (event.observed_at_us, event.ingest_seq)

    @staticmethod
    def _market_order(event: NormalizedMarketEvent) -> tuple[int, int]:
        return (event.event_at_us, event.ingest_seq)

    def process(self, event: NormalizedMarketEvent) -> MarketState:
        acc = self._tokens.setdefault(event.mint, _TokenAccumulator(mint=event.mint))
        order_key = self._causal_order(event)
        if acc.last_order_key is not None and order_key <= acc.last_order_key:
            raise ValueError(
                f"Non-monotonic observed processing order for {event.mint}: "
                f"{order_key} <= {acc.last_order_key}"
            )
        acc.last_order_key = order_key
        acc.known_events.append(event)
        acc.state_seq += 1

        if event.event_type == EventType.LAUNCH:
            acc.launch_seen = True
            if event.creator:
                acc.creator = event.creator
            if acc.token_created_at_us is None:
                acc.token_created_at_us = event.event_at_us

        if event.event_type in (EventType.BUY, EventType.SELL):
            if acc.first_tradable_event_at_us is None:
                acc.first_tradable_event_at_us = event.observed_at_us
                acc.first_tradable_price_proxy = self._price_proxy(event)

            # Recovered historical events must not move the bot's current price
            # backwards. Fresh sources include live, replay and synthetic tests.
            if event.source != IngestionSource.GAP_RECOVERY and self._price_proxy(event) is not None:
                acc.last_fresh_priced_event = event

        return self._build_state(acc, event)

    def _tradable_known_since_t0(self, acc: _TokenAccumulator) -> list[NormalizedMarketEvent]:
        t0 = acc.first_tradable_event_at_us
        if t0 is None:
            return []
        return [
            e for e in acc.known_events
            if e.event_type in (EventType.BUY, EventType.SELL)
            and e.observed_at_us >= t0
        ]

    def _build_window(
        self,
        window_id: str,
        events: list[NormalizedMarketEvent],
        end_at_us: int,
        start_at_us: Optional[int],
        duration_us: Optional[int],
    ) -> WindowFeatures:
        if start_at_us is None:
            selected: list[NormalizedMarketEvent] = []
        elif duration_us is None:  # since_t0, causal observation clock
            selected = [e for e in events if start_at_us <= e.observed_at_us <= end_at_us]
        else:  # short BOT_TRUTH windows are (start, end] and fresh-source only
            selected = [
                e for e in events
                if e.source != IngestionSource.GAP_RECOVERY
                and start_at_us < e.observed_at_us <= end_at_us
            ]

        selected.sort(key=self._causal_order)
        buys = [e for e in selected if e.event_type == EventType.BUY]
        sells = [e for e in selected if e.event_type == EventType.SELL]
        trades = buys + sells

        buy_volume = sum(e.sol_amount_lamports or 0 for e in buys)
        sell_volume = sum(e.sol_amount_lamports or 0 for e in sells)
        total_volume = buy_volume + sell_volume
        net_flow = buy_volume - sell_volume

        unique_buyers = {e.user for e in buys if e.user}
        unique_sellers = {e.user for e in sells if e.user}
        active_wallets = unique_buyers | unique_sellers

        priced = [(e, self._price_proxy(e)) for e in selected]
        priced = [(e, p) for e, p in priced if p is not None]
        open_p = priced[0][1] if priced else None
        close_p = priced[-1][1] if priced else None
        high_p = max((p for _, p in priced), default=None)
        low_p = min((p for _, p in priced), default=None)

        price_change_bps = (
            self._bps(close_p, open_p) if open_p is not None and close_p is not None else None
        )
        high_low_range_bps = (
            self._bps(high_p, low_p) if high_p is not None and low_p is not None else None
        )

        if start_at_us is None:
            seconds = Decimal(0)
        else:
            seconds = Decimal(max(0, end_at_us - start_at_us)) / Decimal(1_000_000)

        if seconds > 0:
            trade_rate = Decimal(len(trades)) / seconds
            buy_rate = Decimal(len(buys)) / seconds
            sell_rate = Decimal(len(sells)) / seconds
            volume_rate = Decimal(total_volume) / seconds
        else:
            trade_rate = buy_rate = sell_rate = volume_rate = Decimal(0)

        return WindowFeatures(
            window_id=window_id,
            start_at_us=start_at_us,
            end_at_us=end_at_us,
            event_count=len(selected),
            trade_count=len(trades),
            buys=len(buys),
            sells=len(sells),
            buy_volume_lamports=buy_volume,
            sell_volume_lamports=sell_volume,
            total_volume_lamports=total_volume,
            net_flow_lamports=net_flow,
            unique_buyers=len(unique_buyers),
            unique_sellers=len(unique_sellers),
            active_wallets=len(active_wallets),
            open_price_proxy=open_p,
            high_price_proxy=high_p,
            low_price_proxy=low_p,
            close_price_proxy=close_p,
            price_change_bps=price_change_bps,
            high_low_range_bps=high_low_range_bps,
            trades_per_second=trade_rate,
            buy_rate_per_second=buy_rate,
            sell_rate_per_second=sell_rate,
            volume_rate_lamports_per_second=volume_rate,
        )

    def _build_state(self, acc: _TokenAccumulator, trigger: NormalizedMarketEvent) -> MarketState:
        t_decision = trigger.observed_at_us
        tradable = self._tradable_known_since_t0(acc)

        latest_priced_event = acc.last_fresh_priced_event
        if latest_priced_event is None:
            priced_known = [e for e in tradable if self._price_proxy(e) is not None]
            latest_priced_event = max(priced_known, key=self._causal_order) if priced_known else None
        current_price = self._price_proxy(latest_priced_event) if latest_priced_event else None
        first_price = acc.first_tradable_price_proxy

        priced_events = [(e, self._price_proxy(e)) for e in tradable]
        priced_events = [(e, p) for e, p in priced_events if p is not None]
        if priced_events:
            high_event, high_price = max(priced_events, key=lambda ep: (ep[1], -ep[0].event_at_us))
            low_event, low_price = min(priced_events, key=lambda ep: (ep[1], ep[0].event_at_us))
        else:
            high_event = low_event = None
            high_price = low_price = None

        return_bps = (
            self._bps(current_price, first_price)
            if current_price is not None and first_price is not None else None
        )
        drawdown_bps = (
            self._bps(current_price, high_price)
            if current_price is not None and high_price is not None else None
        )

        windows: dict[str, WindowFeatures] = {}
        for window_id, width_us in WINDOWS_US.items():
            windows[window_id] = self._build_window(
                window_id, tradable, t_decision, t_decision - width_us, width_us
            )
        windows["since_t0"] = self._build_window(
            "since_t0", tradable, t_decision, acc.first_tradable_event_at_us, None
        )

        age_ms = (
            max(0, (t_decision - acc.first_tradable_event_at_us) // 1_000)
            if acc.first_tradable_event_at_us is not None else None
        )

        gap_seen = any(e.source == IngestionSource.GAP_RECOVERY for e in acc.known_events)
        event_count_since_t0 = len(tradable)

        return MarketState(
            schema_version=self.schema_version,
            mint=acc.mint,
            state_seq=acc.state_seq,
            as_of_observed_at_us=t_decision,
            triggering_event_key=trigger.event_key,
            identity=IdentityState(
                token_created_at_us=acc.token_created_at_us,
                first_tradable_event_at_us=acc.first_tradable_event_at_us,
                age_ms=age_ms,
            ),
            market=MarketCoreState(
                current_price_proxy=current_price,
                virtual_sol_reserve_lamports=(
                    latest_priced_event.virtual_sol_reserve_lamports if latest_priced_event else None
                ),
                virtual_token_reserve_raw=(
                    latest_priced_event.virtual_token_reserve_raw if latest_priced_event else None
                ),
                real_sol_reserve_lamports=(
                    latest_priced_event.real_sol_reserve_lamports if latest_priced_event else None
                ),
                real_token_reserve_raw=(
                    latest_priced_event.real_token_reserve_raw if latest_priced_event else None
                ),
            ),
            price_structure=PriceStructureState(
                first_price_proxy=first_price,
                high_since_t0=high_price,
                high_since_t0_at_us=high_event.event_at_us if high_event else None,
                low_since_t0=low_price,
                low_since_t0_at_us=low_event.event_at_us if low_event else None,
                return_from_t0_bps=return_bps,
                drawdown_from_high_bps=drawdown_bps,
            ),
            windows=windows,
            lifecycle=LifecycleState(
                launch_seen=acc.launch_seen,
                creator=acc.creator,
                migration_state=MigrationState.PUMP,
            ),
            data_quality=DataQualityState(
                last_event_at_us=trigger.event_at_us,
                last_observed_at_us=trigger.observed_at_us,
                event_count_since_t0=event_count_since_t0,
                known_gap_affecting_state=gap_seen,
                state_valid=trigger.source != IngestionSource.GAP_RECOVERY,
            ),
        )
