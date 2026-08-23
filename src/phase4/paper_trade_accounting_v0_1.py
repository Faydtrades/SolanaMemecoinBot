from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from fractions import Fraction
from typing import Any


SCHEMA_VERSION = "phase4_paper_trade_accounting_v0.1"
ENGINE_VERSION = "PaperTradeAccountingV01"
ACCOUNTING_MODEL_ID = "P4-TRADE-ACCOUNTING-0001"
BPS_DENOMINATOR = 10_000
LAMPORTS_PER_SOL = 1_000_000_000
LOCKED_TRACKS = ("FINAL-A", "FINAL-B", "SENS-C")

ACCOUNTING_POLICY = {
    "model_id": ACCOUNTING_MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "engine_version": ENGINE_VERSION,
    "trade_unit": "ONE_SIGNAL_X_ONE_TRACK_COMPLETE_ENTRY_TO_EXIT_LIFECYCLE",
    "principal_basis": "FILLED_SIZE_LAMPORTS",
    "gross_pnl": "GROSS_EXIT_PROCEEDS_MINUS_FILLED_ENTRY_PRINCIPAL",
    "net_pnl": "GROSS_EXIT_PROCEEDS_MINUS_ENTRY_PRINCIPAL_MINUS_ENTRY_EXPLICIT_COST_MINUS_EXIT_EXPLICIT_COST",
    "net_return_bps_basis": "FILLED_ENTRY_PRINCIPAL",
    "net_return_rounding": "HALF_AWAY_FROM_ZERO",
    "expectancy": "ARITHMETIC_MEAN_NET_PNL_PER_COMPLETED_TRADE",
    "profit_factor": "SUM_POSITIVE_NET_PNL_DIV_ABS_SUM_NEGATIVE_NET_PNL",
    "winrate": "WINS_DIV_ALL_COMPLETED_TRADES_BREAKEVENS_REPORTED_SEPARATELY",
    "drawdown_scope": "REALIZED_CLOSED_EQUITY_DIAGNOSTIC_ONLY",
    "locked_mtm_drawdown": "UNAVAILABLE_WITHOUT_PERSISTED_CAUSAL_MARK_TO_MARKET_EQUITY_STREAM",
    "cross_track_portfolio_aggregation": "PROHIBITED_ALTERNATIVE_EXIT_TRACKS_ARE_EVALUATED_SEPARATELY",
    "strategy_skip_audit": "UNAVAILABLE_FROM_PAPER_EXECUTION_DB_ALONE",
}

ACCOUNTING_FINGERPRINT = hashlib.sha256(
    json.dumps(ACCOUNTING_POLICY, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()


@dataclass(frozen=True, slots=True)
class CompletedPaperTrade:
    signal_key: str
    mint: str
    track_id: str
    exit_variant: str
    strategy_version: str
    parameter_set_id: str
    assumption_set_id: str
    cost_model_fingerprint: str
    exit_impact_model_id: str
    exit_impact_model_fingerprint: str
    paper_order_id: str
    paper_position_id: str
    entry_route_id: str
    exit_route_id: str
    open_at: str
    closed_at: str
    holding_time_us: int
    entry_principal_lamports: int
    entry_explicit_cost_lamports: int
    gross_exit_proceeds_lamports: int
    exit_explicit_cost_lamports: int
    gross_execution_pnl_lamports: int
    net_pnl_lamports: int
    net_return_bps: int
    outcome: str
    entry_price_impact_bps: int
    entry_adverse_slippage_bps: int
    exit_price_impact_bps: int
    exit_adverse_slippage_bps: int
    exit_reason: str


@dataclass(frozen=True, slots=True)
class ProfitFactor:
    state: str
    numerator_profit_lamports: int
    denominator_loss_lamports: int

    @property
    def fraction(self) -> Fraction | None:
        if self.denominator_loss_lamports == 0:
            return None
        return Fraction(self.numerator_profit_lamports, self.denominator_loss_lamports)


@dataclass(frozen=True, slots=True)
class TrackAccounting:
    track_id: str
    completed_trades: int
    wins: int
    losses: int
    breakevens: int
    net_pnl_lamports: int
    gross_execution_pnl_lamports: int
    total_entry_explicit_cost_lamports: int
    total_exit_explicit_cost_lamports: int
    total_explicit_cost_lamports: int
    expectancy_numerator_lamports: int
    expectancy_denominator_trades: int
    net_return_bps_sum: int
    average_net_return_bps_numerator: int
    average_net_return_bps_denominator: int
    winrate_bps: int
    average_winner_numerator_lamports: int | None
    average_winner_denominator: int | None
    average_loser_numerator_lamports: int | None
    average_loser_denominator: int | None
    profit_factor: ProfitFactor
    realized_closed_equity_max_drawdown_lamports: int
    realized_closed_equity_peak_lamports: int
    realized_closed_equity_final_lamports: int
    average_holding_time_us_numerator: int
    average_holding_time_us_denominator: int
    median_holding_time_us: int
    mtm_max_drawdown_status: str


@dataclass(frozen=True, slots=True)
class ExecutionRejectionAudit:
    rejected_entry_routes: int
    rejected_exit_attempts_total: int
    rejected_exit_attempts_by_track: tuple[tuple[str, int], ...]
    filled_exit_attempts_total: int
    entry_route_total: int
    exit_attempt_total: int
    strategy_skip_audit_status: str


@dataclass(frozen=True, slots=True)
class AccountingReport:
    schema_version: str
    engine_version: str
    accounting_model_id: str
    accounting_fingerprint: str
    tracks: tuple[TrackAccounting, ...]
    rejection_audit: ExecutionRejectionAudit
    completed_trade_count: int
    cross_track_portfolio_aggregation_status: str
    mtm_drawdown_status: str


def _require_aware_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(str(value))
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"timestamp must be timezone-aware: {value!r}")
    return dt.astimezone(timezone.utc)


def _duration_us(start: str, end: str) -> int:
    a = _require_aware_iso(start)
    b = _require_aware_iso(end)
    if b < a:
        raise ValueError("closed_at precedes open_at")
    delta = b - a
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def _round_half_away_from_zero(value: Fraction) -> int:
    sign = -1 if value < 0 else 1
    value = abs(value)
    q, r = divmod(value.numerator, value.denominator)
    if 2 * r >= value.denominator:
        q += 1
    return sign * q


def _median_int(values: list[int]) -> int:
    if not values:
        raise ValueError("median requires at least one value")
    s = sorted(values)
    n = len(s)
    if n % 2:
        return s[n // 2]
    # Deterministic integer midpoint, half-away-from-zero. Holding times are >=0.
    return _round_half_away_from_zero(Fraction(s[n // 2 - 1] + s[n // 2], 2))


def _required_columns(conn: sqlite3.Connection, table: str, required: set[str]) -> None:
    cols = {str(r[1]) for r in conn.execute(f'PRAGMA table_info("{table}")')}
    missing = sorted(required - cols)
    if missing:
        raise RuntimeError(f"{table} missing required columns: {missing}")


def validate_source_schema(conn: sqlite3.Connection) -> None:
    _required_columns(conn, "paper_entry_routes", {
        "route_id", "candidate_id", "state", "assumption_set_id", "model_fingerprint",
        "price_impact_bps", "adverse_slippage_bps", "total_entry_explicit_cost_lamports",
    })
    _required_columns(conn, "paper_orders", {
        "paper_order_id", "signal_key", "mint", "track_id", "exit_variant",
        "strategy_version", "parameter_set_id", "state", "filled_size_lamports",
    })
    _required_columns(conn, "paper_positions", {
        "paper_position_id", "paper_order_id", "state", "open_at", "closed_at",
    })
    _required_columns(conn, "paper_exit_execution_routes", {
        "route_id", "paper_position_id", "track_id", "exit_reason", "state",
        "cost_model_fingerprint", "exit_impact_model_id", "exit_impact_model_fingerprint",
        "price_impact_bps", "adverse_slippage_bps", "gross_exit_proceeds_lamports",
        "total_exit_explicit_cost_lamports",
    })
    _required_columns(conn, "paper_exit_execution_attempts", {
        "route_id", "decision",
    })


def load_completed_trades(conn: sqlite3.Connection) -> list[CompletedPaperTrade]:
    validate_source_schema(conn)
    rows = conn.execute(
        """
        SELECT
            o.signal_key,
            o.mint,
            o.track_id,
            o.exit_variant,
            o.strategy_version,
            o.parameter_set_id,
            o.paper_order_id,
            o.filled_size_lamports,
            p.paper_position_id,
            p.open_at,
            p.closed_at,
            er.route_id AS entry_route_id,
            er.assumption_set_id,
            er.model_fingerprint AS cost_model_fingerprint,
            er.price_impact_bps AS entry_price_impact_bps,
            er.adverse_slippage_bps AS entry_adverse_slippage_bps,
            er.total_entry_explicit_cost_lamports,
            xr.route_id AS exit_route_id,
            xr.exit_reason,
            xr.exit_impact_model_id,
            xr.exit_impact_model_fingerprint,
            xr.price_impact_bps AS exit_price_impact_bps,
            xr.adverse_slippage_bps AS exit_adverse_slippage_bps,
            xr.gross_exit_proceeds_lamports,
            xr.total_exit_explicit_cost_lamports
        FROM paper_orders o
        JOIN paper_positions p
          ON p.paper_order_id = o.paper_order_id
        JOIN paper_entry_routes er
          ON er.candidate_id = o.signal_key
        JOIN paper_exit_execution_routes xr
          ON xr.paper_position_id = p.paper_position_id
        WHERE o.state = 'FILLED'
          AND p.state = 'CLOSED'
          AND er.state = 'FILLED'
          AND xr.state = 'FILLED'
        ORDER BY p.closed_at, o.signal_key, o.track_id
        """
    ).fetchall()

    out: list[CompletedPaperTrade] = []
    seen: set[tuple[str, str]] = set()

    for row in rows:
        signal_key = str(row["signal_key"])
        track_id = str(row["track_id"])
        key = (signal_key, track_id)
        if key in seen:
            raise RuntimeError(f"duplicate completed trade join for {key}")
        seen.add(key)

        if track_id not in LOCKED_TRACKS:
            raise RuntimeError(f"unexpected Phase-4 track: {track_id}")

        principal = int(row["filled_size_lamports"])
        entry_cost = int(row["total_entry_explicit_cost_lamports"])
        gross_exit = int(row["gross_exit_proceeds_lamports"])
        exit_cost = int(row["total_exit_explicit_cost_lamports"])
        if principal <= 0 or entry_cost < 0 or gross_exit < 0 or exit_cost < 0:
            raise RuntimeError(f"invalid accounting amounts for {key}")

        gross_pnl = gross_exit - principal
        net_pnl = gross_exit - principal - entry_cost - exit_cost
        net_return_bps = _round_half_away_from_zero(
            Fraction(net_pnl * BPS_DENOMINATOR, principal)
        )
        outcome = "WIN" if net_pnl > 0 else "LOSS" if net_pnl < 0 else "BREAKEVEN"

        out.append(
            CompletedPaperTrade(
                signal_key=signal_key,
                mint=str(row["mint"]),
                track_id=track_id,
                exit_variant=str(row["exit_variant"]),
                strategy_version=str(row["strategy_version"]),
                parameter_set_id=str(row["parameter_set_id"]),
                assumption_set_id=str(row["assumption_set_id"]),
                cost_model_fingerprint=str(row["cost_model_fingerprint"]),
                exit_impact_model_id=str(row["exit_impact_model_id"]),
                exit_impact_model_fingerprint=str(row["exit_impact_model_fingerprint"]),
                paper_order_id=str(row["paper_order_id"]),
                paper_position_id=str(row["paper_position_id"]),
                entry_route_id=str(row["entry_route_id"]),
                exit_route_id=str(row["exit_route_id"]),
                open_at=str(row["open_at"]),
                closed_at=str(row["closed_at"]),
                holding_time_us=_duration_us(str(row["open_at"]), str(row["closed_at"])),
                entry_principal_lamports=principal,
                entry_explicit_cost_lamports=entry_cost,
                gross_exit_proceeds_lamports=gross_exit,
                exit_explicit_cost_lamports=exit_cost,
                gross_execution_pnl_lamports=gross_pnl,
                net_pnl_lamports=net_pnl,
                net_return_bps=net_return_bps,
                outcome=outcome,
                entry_price_impact_bps=int(row["entry_price_impact_bps"]),
                entry_adverse_slippage_bps=int(row["entry_adverse_slippage_bps"]),
                exit_price_impact_bps=int(row["exit_price_impact_bps"]),
                exit_adverse_slippage_bps=int(row["exit_adverse_slippage_bps"]),
                exit_reason=str(row["exit_reason"]),
            )
        )

    return out


def _profit_factor(values: list[int]) -> ProfitFactor:
    profit = sum(v for v in values if v > 0)
    loss = -sum(v for v in values if v < 0)
    if loss == 0 and profit > 0:
        state = "INFINITE_NO_LOSSES"
    elif loss == 0 and profit == 0:
        state = "UNDEFINED_NO_WINS_OR_LOSSES"
    elif profit == 0:
        state = "ZERO_NO_WINS"
    else:
        state = "FINITE"
    return ProfitFactor(state, profit, loss)


def _realized_closed_equity_drawdown(values: list[int]) -> tuple[int, int, int]:
    equity = 0
    peak = 0
    max_dd = 0
    max_peak = 0
    for pnl in values:
        equity += pnl
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd
            max_peak = peak
    return max_dd, max_peak, equity


def aggregate_track(track_id: str, trades: list[CompletedPaperTrade]) -> TrackAccounting:
    ordered = sorted(
        [t for t in trades if t.track_id == track_id],
        key=lambda t: (_require_aware_iso(t.closed_at), t.signal_key),
    )
    if not ordered:
        return TrackAccounting(
            track_id=track_id,
            completed_trades=0,
            wins=0,
            losses=0,
            breakevens=0,
            net_pnl_lamports=0,
            gross_execution_pnl_lamports=0,
            total_entry_explicit_cost_lamports=0,
            total_exit_explicit_cost_lamports=0,
            total_explicit_cost_lamports=0,
            expectancy_numerator_lamports=0,
            expectancy_denominator_trades=0,
            net_return_bps_sum=0,
            average_net_return_bps_numerator=0,
            average_net_return_bps_denominator=0,
            winrate_bps=0,
            average_winner_numerator_lamports=None,
            average_winner_denominator=None,
            average_loser_numerator_lamports=None,
            average_loser_denominator=None,
            profit_factor=ProfitFactor("UNDEFINED_NO_WINS_OR_LOSSES", 0, 0),
            realized_closed_equity_max_drawdown_lamports=0,
            realized_closed_equity_peak_lamports=0,
            realized_closed_equity_final_lamports=0,
            average_holding_time_us_numerator=0,
            average_holding_time_us_denominator=0,
            median_holding_time_us=0,
            mtm_max_drawdown_status="UNAVAILABLE_NO_PERSISTED_CAUSAL_MTM_STREAM",
        )

    values = [t.net_pnl_lamports for t in ordered]
    winners = [v for v in values if v > 0]
    losers = [v for v in values if v < 0]
    breakevens = sum(v == 0 for v in values)
    n = len(values)
    holding = [t.holding_time_us for t in ordered]
    max_dd, peak, final = _realized_closed_equity_drawdown(values)

    return TrackAccounting(
        track_id=track_id,
        completed_trades=n,
        wins=len(winners),
        losses=len(losers),
        breakevens=breakevens,
        net_pnl_lamports=sum(values),
        gross_execution_pnl_lamports=sum(t.gross_execution_pnl_lamports for t in ordered),
        total_entry_explicit_cost_lamports=sum(t.entry_explicit_cost_lamports for t in ordered),
        total_exit_explicit_cost_lamports=sum(t.exit_explicit_cost_lamports for t in ordered),
        total_explicit_cost_lamports=sum(
            t.entry_explicit_cost_lamports + t.exit_explicit_cost_lamports for t in ordered
        ),
        expectancy_numerator_lamports=sum(values),
        expectancy_denominator_trades=n,
        net_return_bps_sum=sum(t.net_return_bps for t in ordered),
        average_net_return_bps_numerator=sum(t.net_return_bps for t in ordered),
        average_net_return_bps_denominator=n,
        winrate_bps=_round_half_away_from_zero(Fraction(len(winners) * BPS_DENOMINATOR, n)),
        average_winner_numerator_lamports=(sum(winners) if winners else None),
        average_winner_denominator=(len(winners) if winners else None),
        average_loser_numerator_lamports=(sum(losers) if losers else None),
        average_loser_denominator=(len(losers) if losers else None),
        profit_factor=_profit_factor(values),
        realized_closed_equity_max_drawdown_lamports=max_dd,
        realized_closed_equity_peak_lamports=peak,
        realized_closed_equity_final_lamports=final,
        average_holding_time_us_numerator=sum(holding),
        average_holding_time_us_denominator=n,
        median_holding_time_us=_median_int(holding),
        mtm_max_drawdown_status="UNAVAILABLE_NO_PERSISTED_CAUSAL_MTM_STREAM",
    )


def load_rejection_audit(conn: sqlite3.Connection) -> ExecutionRejectionAudit:
    validate_source_schema(conn)
    entry_total = int(conn.execute("SELECT COUNT(*) FROM paper_entry_routes").fetchone()[0])
    entry_rejected = int(
        conn.execute("SELECT COUNT(*) FROM paper_entry_routes WHERE state='REJECTED'").fetchone()[0]
    )
    exit_total = int(conn.execute("SELECT COUNT(*) FROM paper_exit_execution_attempts").fetchone()[0])
    exit_rejected = int(
        conn.execute(
            "SELECT COUNT(*) FROM paper_exit_execution_attempts WHERE decision='REJECTED_SLIPPAGE'"
        ).fetchone()[0]
    )
    exit_filled = int(
        conn.execute(
            "SELECT COUNT(*) FROM paper_exit_execution_attempts WHERE decision='FILLED'"
        ).fetchone()[0]
    )
    rows = conn.execute(
        """
        SELECT xr.track_id, COUNT(*) AS n
        FROM paper_exit_execution_attempts xa
        JOIN paper_exit_execution_routes xr ON xr.route_id = xa.route_id
        WHERE xa.decision='REJECTED_SLIPPAGE'
        GROUP BY xr.track_id
        ORDER BY xr.track_id
        """
    ).fetchall()
    by_track = tuple((str(r[0]), int(r[1])) for r in rows)
    return ExecutionRejectionAudit(
        rejected_entry_routes=entry_rejected,
        rejected_exit_attempts_total=exit_rejected,
        rejected_exit_attempts_by_track=by_track,
        filled_exit_attempts_total=exit_filled,
        entry_route_total=entry_total,
        exit_attempt_total=exit_total,
        strategy_skip_audit_status="UNAVAILABLE_FROM_PAPER_EXECUTION_DB_ALONE",
    )


def build_report(conn: sqlite3.Connection) -> AccountingReport:
    trades = load_completed_trades(conn)
    tracks = tuple(aggregate_track(track, trades) for track in LOCKED_TRACKS)
    return AccountingReport(
        schema_version=SCHEMA_VERSION,
        engine_version=ENGINE_VERSION,
        accounting_model_id=ACCOUNTING_MODEL_ID,
        accounting_fingerprint=ACCOUNTING_FINGERPRINT,
        tracks=tracks,
        rejection_audit=load_rejection_audit(conn),
        completed_trade_count=len(trades),
        cross_track_portfolio_aggregation_status="PROHIBITED_ALTERNATIVE_TRACKS_REPORTED_SEPARATELY",
        mtm_drawdown_status="UNAVAILABLE_NO_PERSISTED_CAUSAL_MTM_STREAM",
    )


def canonical_report_digest(report: AccountingReport) -> str:
    payload = asdict(report)
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()
