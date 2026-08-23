from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Optional


class EventType(str, Enum):
    LAUNCH = "LAUNCH"
    BUY = "BUY"
    SELL = "SELL"


class IngestionSource(str, Enum):
    LIVE_WS = "LIVE_WS"
    GAP_RECOVERY = "GAP_RECOVERY"
    REPLAY = "REPLAY"
    SYNTHETIC_TEST = "SYNTHETIC_TEST"


class ConfirmationState(str, Enum):
    PROCESSED = "PROCESSED"
    CONFIRMED = "CONFIRMED"
    FINALIZED = "FINALIZED"


class MigrationState(str, Enum):
    UNKNOWN = "UNKNOWN"
    PUMP = "PUMP"
    MIGRATED = "MIGRATED"


@dataclass(frozen=True, slots=True)
class NormalizedMarketEvent:
    schema_version: str
    event_key: str
    ingest_seq: int
    mint: str
    event_type: EventType
    event_at_us: int
    observed_at_us: int
    slot: int
    signature: str
    event_index: int
    user: Optional[str] = None
    creator: Optional[str] = None
    sol_amount_lamports: Optional[int] = None
    token_amount_raw: Optional[int] = None
    virtual_sol_reserve_lamports: Optional[int] = None
    virtual_token_reserve_raw: Optional[int] = None
    real_sol_reserve_lamports: Optional[int] = None
    real_token_reserve_raw: Optional[int] = None
    source: IngestionSource = IngestionSource.LIVE_WS
    confirmation_state: Optional[ConfirmationState] = None


@dataclass(frozen=True, slots=True)
class WindowFeatures:
    window_id: str
    start_at_us: Optional[int]
    end_at_us: int
    event_count: int
    trade_count: int
    buys: int
    sells: int
    buy_volume_lamports: int
    sell_volume_lamports: int
    total_volume_lamports: int
    net_flow_lamports: int
    unique_buyers: int
    unique_sellers: int
    active_wallets: int
    open_price_proxy: Optional[Decimal]
    high_price_proxy: Optional[Decimal]
    low_price_proxy: Optional[Decimal]
    close_price_proxy: Optional[Decimal]
    price_change_bps: Optional[int]
    high_low_range_bps: Optional[int]
    trades_per_second: Decimal
    buy_rate_per_second: Decimal
    sell_rate_per_second: Decimal
    volume_rate_lamports_per_second: Decimal


@dataclass(frozen=True, slots=True)
class IdentityState:
    token_created_at_us: Optional[int]
    # None is valid before the first observed tradable BUY/SELL event.
    first_tradable_event_at_us: Optional[int]
    age_ms: Optional[int]


@dataclass(frozen=True, slots=True)
class MarketCoreState:
    current_price_proxy: Optional[Decimal]
    virtual_sol_reserve_lamports: Optional[int]
    virtual_token_reserve_raw: Optional[int]
    real_sol_reserve_lamports: Optional[int]
    real_token_reserve_raw: Optional[int]


@dataclass(frozen=True, slots=True)
class PriceStructureState:
    first_price_proxy: Optional[Decimal]
    high_since_t0: Optional[Decimal]
    high_since_t0_at_us: Optional[int]
    low_since_t0: Optional[Decimal]
    low_since_t0_at_us: Optional[int]
    return_from_t0_bps: Optional[int]
    drawdown_from_high_bps: Optional[int]


@dataclass(frozen=True, slots=True)
class LifecycleState:
    launch_seen: bool
    creator: Optional[str]
    migration_state: MigrationState


@dataclass(frozen=True, slots=True)
class DataQualityState:
    last_event_at_us: int
    last_observed_at_us: int
    event_count_since_t0: int
    known_gap_affecting_state: bool
    state_valid: bool


@dataclass(frozen=True, slots=True)
class MarketState:
    schema_version: str
    mint: str
    state_seq: int
    as_of_observed_at_us: int
    triggering_event_key: str
    identity: IdentityState
    market: MarketCoreState
    price_structure: PriceStructureState
    windows: dict[str, WindowFeatures]
    lifecycle: LifecycleState
    data_quality: DataQualityState


class FirstPullbackState(str, Enum):
    DISCOVERED = "DISCOVERED"
    WAITING_FOR_IMPULSE = "WAITING_FOR_IMPULSE"
    IMPULSE_CONFIRMED = "IMPULSE_CONFIRMED"
    WAITING_FOR_PULLBACK = "WAITING_FOR_PULLBACK"
    PULLBACK_ACTIVE = "PULLBACK_ACTIVE"
    WAITING_FOR_RESPONSE = "WAITING_FOR_RESPONSE"
    WAITING_FOR_RECLAIM = "WAITING_FOR_RECLAIM"
    CANDIDATE_SIGNAL = "CANDIDATE_SIGNAL"
    TRADE = "TRADE"
    REJECT = "REJECT"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"


@dataclass(frozen=True, slots=True)
class MarketPoint:
    event_key: str
    observed_at_us: int
    ingest_seq: int
    price_proxy: Decimal


@dataclass(slots=True)
class StrategyRunState:
    schema_version: str
    run_id: str
    mint: str
    strategy_name: str
    strategy_version: str
    parameter_set_id: str
    current_state: FirstPullbackState
    started_at_us: int
    last_transition_at_us: int
    impulse_start_ref: Optional[MarketPoint] = None
    impulse_high_ref: Optional[MarketPoint] = None
    pullback_low_ref: Optional[MarketPoint] = None
    response_ref: Optional[MarketPoint] = None
    reclaim_ref: Optional[MarketPoint] = None
    signal_emitted: bool = False
    finished: bool = False
    final_outcome: Optional[FirstPullbackState] = None


@dataclass(frozen=True, slots=True)
class CandidateSignal:
    signal_id: str
    run_id: str
    mint: str
    strategy_version: str
    parameter_set_id: str
    generated_at_us: int
    signal_type: str
    reference_price_proxy: Optional[Decimal]
    triggering_event_key: str
    reason_code: str


@dataclass(frozen=True, slots=True)
class StrategyEvaluation:
    evaluation_id: str
    run_id: str
    mint: str
    evaluated_at_us: int
    previous_state: FirstPullbackState
    current_state: FirstPullbackState
    transition_occurred: bool
    reason_code: str
    filters_passed: tuple[str, ...]
    filters_failed: tuple[str, ...]
    signal_type: str
    candidate_signal: Optional[CandidateSignal]
    audit_snapshot: dict[str, object]


@dataclass(frozen=True, slots=True)
class FirstPullbackParameterSet:
    parameter_set_id: str
    strategy_version: str
    max_entry_age_ms: int
    impulse_parameters: dict[str, int | Decimal | str | bool]
    pullback_parameters: dict[str, int | Decimal | str | bool]
    buyer_response_parameters: dict[str, int | Decimal | str | bool]
    reclaim_parameters: dict[str, int | Decimal | str | bool]
    runaway_entry_parameters: dict[str, int | Decimal | str | bool]
