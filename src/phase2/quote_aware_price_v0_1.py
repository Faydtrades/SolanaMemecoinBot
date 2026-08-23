from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_EVEN, getcontext
from enum import Enum
from hashlib import sha256
import json
import sqlite3
from typing import Iterable, Mapping, Sequence

from .models_v0_1 import EventType, IngestionSource, NormalizedMarketEvent
from .phase1_readonly_adapter_v0_1 import (
    BOT_TRUTH_SOURCE_PREFIXES,
    Phase1ReadOnlyAdapterV01,
)

getcontext().prec = 60

SCHEMA_VERSION = "QAP-0.1"
ENTRY_WINDOW_MS = 300_000


class PricePathKind(str, Enum):
    SOL = "SOL"
    QUOTE = "QUOTE"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class QuoteAwareNormalizedEvent:
    """Additive Phase-2 view over the existing NME-0.1 event.

    We intentionally do not overwrite NME-0.1 yet.  Phase 1 already persisted
    quote_mint / quote_amount_raw / virtual_quote_reserves / real_quote_reserves;
    this wrapper validates their semantics before they become a locked core model.
    """

    base: NormalizedMarketEvent
    quote_mint: str | None
    quote_amount_raw: int | None
    virtual_quote_reserve_raw: int | None
    real_quote_reserve_raw: int | None


@dataclass(frozen=True, slots=True)
class PriceIdentity:
    path: PricePathKind
    quote_mint: str | None

    @property
    def label(self) -> str:
        if self.path == PricePathKind.SOL:
            return "SOL_NATIVE"
        if self.path == PricePathKind.QUOTE:
            return f"QUOTE:{self.quote_mint or 'UNKNOWN'}"
        return "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class PricePoint:
    identity: PriceIdentity
    price_proxy: Decimal | None
    numerator_reserve_raw: int | None
    token_reserve_raw: int | None
    event_key: str
    observed_at_us: int
    ingest_seq: int


@dataclass(frozen=True, slots=True)
class QuoteAwareMarketState:
    schema_version: str
    mint: str
    state_seq: int
    as_of_observed_at_us: int
    triggering_event_key: str
    first_tradable_observed_at_us: int | None
    price_identity: PriceIdentity
    current_price_proxy: Decimal | None
    first_price_proxy: Decimal | None
    high_since_t0: Decimal | None
    low_since_t0: Decimal | None
    return_from_t0_bps: int | None
    drawdown_from_high_bps: int | None
    path_conflict_seen: bool
    priced_trade_count: int
    unpriced_trade_count: int
    sol_buy_volume_lamports: int
    sol_sell_volume_lamports: int
    quote_buy_volume_raw: int
    quote_sell_volume_raw: int
    quote_flow_mint: str | None
    quote_flow_mint_conflict: bool
    # FirstPullbackStrategyV01 currently interprets response net flow as lamports.
    # A quote-priced token is therefore price-compatible but NOT flow-compatible
    # with strategy v1.0 until its flow parameters become denomination-aware.
    strategy_v01_flow_compatible: bool
    state_valid: bool


@dataclass(slots=True)
class _Accumulator:
    mint: str
    state_seq: int = 0
    last_order_key: tuple[int, int] | None = None
    first_tradable_observed_at_us: int | None = None
    canonical_identity: PriceIdentity | None = None
    first_price_proxy: Decimal | None = None
    last_fresh_price_point: PricePoint | None = None
    comparable_points: list[PricePoint] | None = None
    path_conflict_seen: bool = False
    priced_trade_count: int = 0
    unpriced_trade_count: int = 0
    sol_buy_volume_lamports: int = 0
    sol_sell_volume_lamports: int = 0
    quote_buy_volume_raw: int = 0
    quote_sell_volume_raw: int = 0
    quote_flow_mint: str | None = None
    quote_flow_mint_conflict: bool = False

    def __post_init__(self) -> None:
        if self.comparable_points is None:
            self.comparable_points = []


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value))
    except Exception:
        return None


def _row_dict(row: Mapping[str, object] | sqlite3.Row) -> dict[str, object]:
    return dict(row)


def canonical_json(value: object) -> str:
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
    return sha256(canonical_json(value).encode("utf-8")).hexdigest()


class Phase1QuoteAwareAdapterV01:
    """Read-only additive adapter that preserves all Phase-1 quote fields."""

    schema_version = "P1QAA-0.1"
    REQUIRED_QUOTE_COLUMNS = {
        "quote_mint",
        "quote_amount_raw",
        "virtual_quote_reserves",
        "real_quote_reserves",
    }

    @classmethod
    def validate_schema(cls, conn: sqlite3.Connection) -> None:
        Phase1ReadOnlyAdapterV01.validate_schema(conn)
        columns = Phase1ReadOnlyAdapterV01.table_columns(conn, "pump_events")
        missing = sorted(cls.REQUIRED_QUOTE_COLUMNS - columns)
        if missing:
            raise ValueError(f"pump_events is missing quote-aware columns: {missing}")

    @classmethod
    def row_to_event(cls, row: sqlite3.Row) -> tuple[QuoteAwareNormalizedEvent | None, str | None]:
        base_result = Phase1ReadOnlyAdapterV01.row_to_event(row)
        if base_result.event is None:
            return None, base_result.skip_reason
        try:
            return (
                QuoteAwareNormalizedEvent(
                    base=base_result.event,
                    quote_mint=str(row["quote_mint"]) if row["quote_mint"] is not None else None,
                    quote_amount_raw=_optional_int(row["quote_amount_raw"]),
                    virtual_quote_reserve_raw=_optional_int(row["virtual_quote_reserves"]),
                    real_quote_reserve_raw=_optional_int(row["real_quote_reserves"]),
                ),
                None,
            )
        except Exception as exc:
            return None, f"QUOTE_NORMALIZATION_ERROR:{type(exc).__name__}:{exc}"

    @classmethod
    def fetch_all_bot_truth_rows(cls, conn: sqlite3.Connection) -> list[sqlite3.Row]:
        where_source = "(" + " OR ".join(
            "source_decoded_file LIKE ?" for _ in BOT_TRUTH_SOURCE_PREFIXES
        ) + ")"
        source_args = tuple(prefix + "%" for prefix in BOT_TRUTH_SOURCE_PREFIXES)
        return conn.execute(
            f"""
            SELECT rowid AS p1_rowid, *
            FROM pump_events
            WHERE mint IS NOT NULL
              AND event_type IN ('LAUNCH','BUY','SELL')
              AND pump_timestamp IS NOT NULL
              AND decoded_at_utc IS NOT NULL
              AND slot IS NOT NULL
              AND {where_source}
            ORDER BY decoded_at_utc, rowid
            """,
            source_args,
        ).fetchall()

    @classmethod
    def normalize_rows(
        cls, rows: Iterable[sqlite3.Row]
    ) -> tuple[list[QuoteAwareNormalizedEvent], dict[str, int]]:
        events: list[QuoteAwareNormalizedEvent] = []
        skipped: Counter[str] = Counter()
        for row in rows:
            event, reason = cls.row_to_event(row)
            if event is not None:
                events.append(event)
            else:
                skipped[reason or "UNKNOWN"] += 1
        events.sort(key=lambda e: (e.base.observed_at_us, e.base.ingest_seq))
        return events, dict(sorted(skipped.items()))


class QuoteAwarePriceEngineV01:
    """Deterministic price-path validation engine.

    Absolute proxy units are intentionally raw reserve ratios.  For relative
    return/drawdown calculations, fixed decimal scaling cancels as long as a token
    stays on the same PriceIdentity.  Cross-path comparisons are forbidden.
    """

    schema_version = "QAMS-0.1"

    def __init__(self) -> None:
        self._acc: dict[str, _Accumulator] = {}

    @staticmethod
    def point(event: QuoteAwareNormalizedEvent) -> PricePoint:
        base = event.base
        v_tok = base.virtual_token_reserve_raw
        v_sol = base.virtual_sol_reserve_lamports
        v_quote = event.virtual_quote_reserve_raw

        if v_tok is None or v_tok <= 0:
            identity = PriceIdentity(PricePathKind.UNAVAILABLE, None)
            return PricePoint(identity, None, None, v_tok, base.event_key, base.observed_at_us, base.ingest_seq)
        if v_sol is not None and v_sol > 0:
            identity = PriceIdentity(PricePathKind.SOL, None)
            return PricePoint(
                identity,
                Decimal(v_sol) / Decimal(v_tok),
                v_sol,
                v_tok,
                base.event_key,
                base.observed_at_us,
                base.ingest_seq,
            )
        if v_quote is not None and v_quote > 0 and event.quote_mint:
            identity = PriceIdentity(PricePathKind.QUOTE, event.quote_mint)
            return PricePoint(
                identity,
                Decimal(v_quote) / Decimal(v_tok),
                v_quote,
                v_tok,
                base.event_key,
                base.observed_at_us,
                base.ingest_seq,
            )
        identity = PriceIdentity(PricePathKind.UNAVAILABLE, event.quote_mint)
        return PricePoint(identity, None, None, v_tok, base.event_key, base.observed_at_us, base.ingest_seq)

    @staticmethod
    def _bps(current: Decimal, reference: Decimal) -> int | None:
        if reference == 0:
            return None
        raw = ((current / reference) - Decimal(1)) * Decimal(10_000)
        return int(raw.quantize(Decimal("1"), rounding=ROUND_HALF_EVEN))

    @staticmethod
    def _same_identity(a: PriceIdentity, b: PriceIdentity) -> bool:
        return a.path == b.path and a.quote_mint == b.quote_mint

    def process(self, event: QuoteAwareNormalizedEvent) -> QuoteAwareMarketState:
        base = event.base
        acc = self._acc.setdefault(base.mint, _Accumulator(mint=base.mint))
        key = (base.observed_at_us, base.ingest_seq)
        if acc.last_order_key is not None and key <= acc.last_order_key:
            raise ValueError(f"Non-monotonic quote-aware order for {base.mint}: {key} <= {acc.last_order_key}")
        acc.last_order_key = key
        acc.state_seq += 1

        is_trade = base.event_type in (EventType.BUY, EventType.SELL)
        if is_trade and acc.first_tradable_observed_at_us is None:
            acc.first_tradable_observed_at_us = base.observed_at_us

        if is_trade:
            p = self.point(event)
            if p.price_proxy is None:
                acc.unpriced_trade_count += 1
            else:
                acc.priced_trade_count += 1
                if acc.canonical_identity is None:
                    acc.canonical_identity = p.identity
                    acc.first_price_proxy = p.price_proxy
                elif not self._same_identity(acc.canonical_identity, p.identity):
                    acc.path_conflict_seen = True

                if acc.canonical_identity is not None and self._same_identity(acc.canonical_identity, p.identity):
                    assert acc.comparable_points is not None
                    acc.comparable_points.append(p)
                    if base.source != IngestionSource.GAP_RECOVERY:
                        acc.last_fresh_price_point = p

            sol_amount = base.sol_amount_lamports or 0
            quote_amount = event.quote_amount_raw or 0
            if base.event_type == EventType.BUY:
                if sol_amount > 0:
                    acc.sol_buy_volume_lamports += sol_amount
                if quote_amount > 0:
                    acc.quote_buy_volume_raw += quote_amount
            else:
                if sol_amount > 0:
                    acc.sol_sell_volume_lamports += sol_amount
                if quote_amount > 0:
                    acc.quote_sell_volume_raw += quote_amount

            if quote_amount > 0 and event.quote_mint:
                if acc.quote_flow_mint is None:
                    acc.quote_flow_mint = event.quote_mint
                elif acc.quote_flow_mint != event.quote_mint:
                    acc.quote_flow_mint_conflict = True

        identity = acc.canonical_identity or PriceIdentity(PricePathKind.UNAVAILABLE, None)
        current = acc.last_fresh_price_point.price_proxy if acc.last_fresh_price_point else None
        points = acc.comparable_points or []
        first = acc.first_price_proxy
        high = max((p.price_proxy for p in points if p.price_proxy is not None), default=None)
        low = min((p.price_proxy for p in points if p.price_proxy is not None), default=None)
        ret = self._bps(current, first) if current is not None and first is not None else None
        dd = self._bps(current, high) if current is not None and high is not None else None

        state_valid = base.source != IngestionSource.GAP_RECOVERY and not acc.path_conflict_seen
        return QuoteAwareMarketState(
            schema_version=self.schema_version,
            mint=base.mint,
            state_seq=acc.state_seq,
            as_of_observed_at_us=base.observed_at_us,
            triggering_event_key=base.event_key,
            first_tradable_observed_at_us=acc.first_tradable_observed_at_us,
            price_identity=identity,
            current_price_proxy=current,
            first_price_proxy=first,
            high_since_t0=high,
            low_since_t0=low,
            return_from_t0_bps=ret,
            drawdown_from_high_bps=dd,
            path_conflict_seen=acc.path_conflict_seen,
            priced_trade_count=acc.priced_trade_count,
            unpriced_trade_count=acc.unpriced_trade_count,
            sol_buy_volume_lamports=acc.sol_buy_volume_lamports,
            sol_sell_volume_lamports=acc.sol_sell_volume_lamports,
            quote_buy_volume_raw=acc.quote_buy_volume_raw,
            quote_sell_volume_raw=acc.quote_sell_volume_raw,
            quote_flow_mint=acc.quote_flow_mint,
            quote_flow_mint_conflict=acc.quote_flow_mint_conflict,
            strategy_v01_flow_compatible=identity.path == PricePathKind.SOL,
            state_valid=state_valid,
        )


def group_events(events: Sequence[QuoteAwareNormalizedEvent]) -> dict[str, list[QuoteAwareNormalizedEvent]]:
    grouped: dict[str, list[QuoteAwareNormalizedEvent]] = defaultdict(list)
    for event in events:
        grouped[event.base.mint].append(event)
    for values in grouped.values():
        values.sort(key=lambda e: (e.base.observed_at_us, e.base.ingest_seq))
    return dict(grouped)


def entry_window_events(
    events: Sequence[QuoteAwareNormalizedEvent], *, entry_window_ms: int = ENTRY_WINDOW_MS
) -> list[QuoteAwareNormalizedEvent]:
    trades = [e for e in events if e.base.event_type in (EventType.BUY, EventType.SELL)]
    if not trades:
        return []
    t0 = trades[0].base.observed_at_us
    deadline = t0 + entry_window_ms * 1_000
    return [
        e for e in events
        if e.base.event_type in (EventType.BUY, EventType.SELL)
        and t0 <= e.base.observed_at_us <= deadline
    ]


def classify_token_path(events: Sequence[QuoteAwareNormalizedEvent]) -> dict[str, object]:
    points = [QuoteAwarePriceEngineV01.point(e) for e in events]
    available = [p for p in points if p.price_proxy is not None]
    identities = {(p.identity.path.value, p.identity.quote_mint) for p in available}
    sol_points = sum(p.identity.path == PricePathKind.SOL for p in available)
    quote_points = sum(p.identity.path == PricePathKind.QUOTE for p in available)
    unavailable = len(points) - len(available)

    if not available:
        token_class = "UNAVAILABLE"
    elif len(identities) == 1 and next(iter(identities))[0] == PricePathKind.SOL.value:
        token_class = "SOL_ONLY"
    elif len(identities) == 1 and next(iter(identities))[0] == PricePathKind.QUOTE.value:
        token_class = "QUOTE_ONLY"
    else:
        token_class = "MIXED_PRICE_IDENTITY"

    quote_mints = sorted({p.identity.quote_mint for p in available if p.identity.path == PricePathKind.QUOTE and p.identity.quote_mint})
    return {
        "token_class": token_class,
        "trade_events": len(points),
        "priceable_events": len(available),
        "sol_priceable_events": sol_points,
        "quote_priceable_events": quote_points,
        "unavailable_events": unavailable,
        "quote_mints": quote_mints,
        "identity_count": len(identities),
    }


def build_full_universe_validation(
    events: Sequence[QuoteAwareNormalizedEvent],
    *, entry_window_ms: int = ENTRY_WINDOW_MS,
) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, object]]:
    grouped = group_events(events)
    token_rows: list[dict[str, object]] = []
    quote_event_rows: list[dict[str, object]] = []
    classes: Counter[str] = Counter()
    quote_mint_tokens: dict[str, set[str]] = defaultdict(set)
    quote_mint_events: Counter[str] = Counter()
    sol_parity_failures = 0
    quote_amount_positive = 0
    quote_amount_missing_or_zero = 0
    quote_path_sol_amount_positive = 0
    quote_path_sol_amount_zero = 0
    old_sol_unpriceable = 0
    rescued_by_quote = 0
    path_conflict_tokens = 0
    quote_flow_conflict_tokens = 0
    deterministic_state_digests: list[str] = []

    for mint, token_events in sorted(grouped.items(), key=lambda kv: kv[0]):
        win = entry_window_events(token_events, entry_window_ms=entry_window_ms)
        if not win:
            continue
        cls = classify_token_path(win)
        classes[str(cls["token_class"])] += 1
        old_unpriceable = int(cls["sol_priceable_events"]) == 0
        if old_unpriceable:
            old_sol_unpriceable += 1
        if old_unpriceable and cls["token_class"] == "QUOTE_ONLY":
            rescued_by_quote += 1

        engine = QuoteAwarePriceEngineV01()
        final_state: QuoteAwareMarketState | None = None
        for event in win:
            p = QuoteAwarePriceEngineV01.point(event)
            if p.identity.path == PricePathKind.SOL and p.price_proxy is not None:
                expected = Decimal(event.base.virtual_sol_reserve_lamports) / Decimal(event.base.virtual_token_reserve_raw)
                if p.price_proxy != expected:
                    sol_parity_failures += 1
            if p.identity.path == PricePathKind.QUOTE and p.price_proxy is not None:
                qmint = p.identity.quote_mint
                if qmint:
                    quote_mint_tokens[qmint].add(mint)
                    quote_mint_events[qmint] += 1
                qamt = event.quote_amount_raw or 0
                if qamt > 0:
                    quote_amount_positive += 1
                else:
                    quote_amount_missing_or_zero += 1
                samt = event.base.sol_amount_lamports or 0
                if samt > 0:
                    quote_path_sol_amount_positive += 1
                else:
                    quote_path_sol_amount_zero += 1
                quote_event_rows.append(
                    {
                        "mint": mint,
                        "event_key": event.base.event_key,
                        "event_type": event.base.event_type.value,
                        "observed_at_us": event.base.observed_at_us,
                        "quote_mint": event.quote_mint,
                        "quote_amount_raw": event.quote_amount_raw,
                        "sol_amount_lamports": event.base.sol_amount_lamports,
                        "virtual_quote_reserve_raw": event.virtual_quote_reserve_raw,
                        "virtual_sol_reserve_lamports": event.base.virtual_sol_reserve_lamports,
                        "virtual_token_reserve_raw": event.base.virtual_token_reserve_raw,
                        "price_proxy_raw_ratio": str(p.price_proxy),
                    }
                )
            final_state = engine.process(event)

        assert final_state is not None
        if final_state.path_conflict_seen:
            path_conflict_tokens += 1
        if final_state.quote_flow_mint_conflict:
            quote_flow_conflict_tokens += 1
        deterministic_state_digests.append(stable_sha256(final_state))

        token_rows.append(
            {
                "mint": mint,
                "token_class": cls["token_class"],
                "trade_events_5m": cls["trade_events"],
                "priceable_events_5m": cls["priceable_events"],
                "sol_priceable_events_5m": cls["sol_priceable_events"],
                "quote_priceable_events_5m": cls["quote_priceable_events"],
                "unavailable_events_5m": cls["unavailable_events"],
                "quote_mints": ",".join(cls["quote_mints"]),
                "old_sol_unpriceable": old_unpriceable,
                "quote_rescued": old_unpriceable and cls["token_class"] == "QUOTE_ONLY",
                "path_conflict_seen": final_state.path_conflict_seen,
                "quote_flow_mint_conflict": final_state.quote_flow_mint_conflict,
                "price_identity": final_state.price_identity.label,
                "first_price_proxy": None if final_state.first_price_proxy is None else str(final_state.first_price_proxy),
                "current_price_proxy": None if final_state.current_price_proxy is None else str(final_state.current_price_proxy),
                "return_from_t0_bps": final_state.return_from_t0_bps,
                "drawdown_from_high_bps": final_state.drawdown_from_high_bps,
                "strategy_v01_flow_compatible": final_state.strategy_v01_flow_compatible,
                "sol_buy_volume_lamports": final_state.sol_buy_volume_lamports,
                "sol_sell_volume_lamports": final_state.sol_sell_volume_lamports,
                "quote_buy_volume_raw": final_state.quote_buy_volume_raw,
                "quote_sell_volume_raw": final_state.quote_sell_volume_raw,
                "quote_flow_mint": final_state.quote_flow_mint,
            }
        )

    summary = {
        "tokens_analyzed": len(token_rows),
        "token_class_counts": dict(sorted(classes.items())),
        "old_sol_unpriceable_tokens": old_sol_unpriceable,
        "rescued_by_quote_path_tokens": rescued_by_quote,
        "remaining_unavailable_or_mixed_tokens": sum(
            classes.get(k, 0) for k in ("UNAVAILABLE", "MIXED_PRICE_IDENTITY")
        ),
        "enhanced_price_coverage_tokens": sum(
            classes.get(k, 0) for k in ("SOL_ONLY", "QUOTE_ONLY")
        ),
        "quote_mint_token_counts": {k: len(v) for k, v in sorted(quote_mint_tokens.items())},
        "quote_mint_event_counts": dict(sorted(quote_mint_events.items())),
        "quote_priceable_events": len(quote_event_rows),
        "quote_amount_positive_events": quote_amount_positive,
        "quote_amount_missing_or_zero_events": quote_amount_missing_or_zero,
        "quote_path_sol_amount_positive_events": quote_path_sol_amount_positive,
        "quote_path_sol_amount_zero_events": quote_path_sol_amount_zero,
        "sol_price_proxy_parity_failures": sol_parity_failures,
        "path_conflict_tokens": path_conflict_tokens,
        "quote_flow_mint_conflict_tokens": quote_flow_conflict_tokens,
        "strategy_v01_quote_flow_compatible": False,
        "relative_price_bps_supported_for_quote_path": True,
        "absolute_human_price_units_resolved": False,
        "deterministic_state_digest": stable_sha256(deterministic_state_digests),
    }
    return token_rows, quote_event_rows, summary
