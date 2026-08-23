from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import StrEnum
from fractions import Fraction
from typing import Any, Iterable

from .paper_cost_model_v0_2 import CostSide, PaperCostModelV02
from .runtime_exit_price_impact_v0_1 import RuntimeExitPriceImpactV01


SCHEMA_VERSION = "phase4_paper_runtime_observability_v0.1"
ENGINE_VERSION = "PaperRuntimeObservabilityV01"
MODEL_ID = "P4-RUNTIME-OBSERVABILITY-0001"
LOCKED_TRACKS = ("FINAL-A", "FINAL-B", "SENS-C")

POLICY = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "engine_version": ENGINE_VERSION,
    "strategy_evaluation_key": "RUN_ID_X_ROLE_X_INGEST_SEQ",
    "terminal_decision_key": "CALLER_SUPPLIED_DETERMINISTIC_TOKEN_OR_ROLE_DECISION_KEY",
    "position_mark_key": "PAPER_POSITION_X_INGEST_SEQ",
    "track_equity_key": "TRACK_X_INGEST_SEQ_X_SOURCE_EVENT_KEY",
    "mtm_policy": "NET_IMMEDIATE_LIQUIDATION_VALUE_AT_CAUSAL_MARK",
    "mtm_exit_latency": "NOT_APPLIED_TO_MARK_TO_MARKET_MARK",
    "mtm_exit_impact": "P4_RUNTIME_EXIT_PRICE_IMPACT_AT_CURRENT_VIRTUAL_TOKEN_RESERVE",
    "mtm_exit_cost": "P4_COST_BASELINE_EXIT_EXPLICIT_COST_AT_ESTIMATED_GROSS_PROCEEDS",
    "mtm_slippage_cap": "NOT_USED_AS_MARK_REJECTION_GATE",
    "mae_mfe_basis": "CAUSAL_MARKET_PRICE_RETURN_VS_SIMULATED_ENTRY_EXECUTION_PRICE",
    "drawdown_basis": "TRACK_EQUITY_PNL_STREAM_PEAK_TO_TROUGH_WITH_ZERO_START_ANCHOR",
    "gap_recovery_marks": "EXCLUDED",
    "cross_track_aggregation": "PROHIBITED_ALTERNATIVE_EXIT_TRACKS_REPORTED_SEPARATELY",
    "strategy_skip_audit": "PERSIST_EXACT_EVALUATIONS_PLUS_EXPLICIT_TERMINAL_TRADE_OR_SKIP_DECISION",
}

MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(POLICY, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()


class TradeOrSkip(StrEnum):
    TRADE = "TRADE"
    SKIP = "SKIP"


class ObservabilityDeterminismConflict(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class StrategyEvaluationAuditInput:
    run_id: str
    role: str
    mint: str
    strategy_version: str
    parameter_set_id: str
    ingest_seq: int
    evaluated_at_us: int
    current_state: str
    transition_occurred: bool
    reason_code: str
    filters_passed: tuple[str, ...]
    filters_failed: tuple[str, ...]
    signal_type: str | None
    candidate_signal_id: str | None
    audit_snapshot: dict[str, Any]


@dataclass(frozen=True, slots=True)
class TerminalDecisionAuditInput:
    decision_key: str
    decision_scope: str
    run_id: str | None
    role: str | None
    mint: str
    strategy_version: str
    parameter_set_id: str | None
    decision_at_us: int
    trade_or_skip: TradeOrSkip
    decision_reason: str
    candidate_signal_id: str | None
    paper_entry_route_id: str | None
    source_evaluation_id: str | None


@dataclass(frozen=True, slots=True)
class PositionMarkObservation:
    mint: str
    observed_at: datetime
    ingest_seq: int
    source_event_key: str
    price_identity: str
    price_numerator_raw: int
    price_denominator_raw: int
    current_virtual_token_reserve_raw: int
    is_gap_recovery: bool = False


@dataclass(frozen=True, slots=True)
class PositionMtmMark:
    mark_id: str
    paper_position_id: str
    signal_key: str
    mint: str
    track_id: str
    observed_at: datetime
    ingest_seq: int
    source_event_key: str
    price_identity: str
    market_price_numerator_raw: int
    market_price_denominator_raw: int
    current_virtual_token_reserve_raw: int
    derived_token_input_raw: int
    exit_price_impact_bps: int
    simulated_liquidation_price_numerator_raw: int
    simulated_liquidation_price_denominator_raw: int
    gross_market_value_lamports: int
    gross_liquidation_proceeds_lamports: int
    entry_principal_lamports: int
    entry_explicit_cost_lamports: int
    estimated_exit_explicit_cost_lamports: int
    market_return_bps: int
    net_mtm_pnl_lamports: int


@dataclass(frozen=True, slots=True)
class TrackEquityMark:
    equity_mark_id: str
    track_id: str
    observed_at: datetime
    ingest_seq: int
    source_event_key: str
    realized_closed_pnl_lamports: int
    open_position_count: int
    open_position_net_mtm_pnl_lamports: int
    equity_pnl_lamports: int
    open_position_details: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class PositionExcursionSummary:
    paper_position_id: str
    signal_key: str
    track_id: str
    mint: str
    marks: int
    mae_bps: int
    mfe_bps: int
    min_net_mtm_pnl_lamports: int
    max_net_mtm_pnl_lamports: int
    first_ingest_seq: int
    last_ingest_seq: int


@dataclass(frozen=True, slots=True)
class TrackMtmSummary:
    track_id: str
    equity_marks: int
    max_drawdown_lamports: int
    peak_equity_pnl_lamports: int
    trough_equity_pnl_lamports: int
    final_equity_pnl_lamports: int


@dataclass(frozen=True, slots=True)
class StrategyDecisionSummary:
    evaluation_rows: int
    terminal_decisions: int
    trades: int
    skips: int
    skip_reasons: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class ObservabilityReport:
    schema_version: str
    engine_version: str
    model_id: str
    model_fingerprint: str
    positions: tuple[PositionExcursionSummary, ...]
    tracks: tuple[TrackMtmSummary, ...]
    strategy: StrategyDecisionSummary
    cross_track_aggregation_status: str


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)


def _dt_text(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


def _dt_parse(value: str) -> datetime:
    return _utc(datetime.fromisoformat(value))


def _hash_id(prefix: str, *parts: object) -> str:
    body = "\x1f".join(str(x) for x in parts)
    return f"{prefix}-{hashlib.sha256(body.encode('utf-8')).hexdigest()[:32]}"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _fingerprint(value: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _round_half_away_from_zero(value: Fraction) -> int:
    sign = -1 if value < 0 else 1
    v = abs(value)
    q, r = divmod(v.numerator, v.denominator)
    if 2 * r >= v.denominator:
        q += 1
    return sign * q


def _floor_fraction(value: Fraction) -> int:
    if value < 0:
        raise ValueError("value must be >= 0")
    return value.numerator // value.denominator


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_strategy_evaluations (
            evaluation_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            role TEXT NOT NULL,
            mint TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            parameter_set_id TEXT NOT NULL,
            ingest_seq INTEGER NOT NULL,
            evaluated_at_us INTEGER NOT NULL,
            current_state TEXT NOT NULL,
            transition_occurred INTEGER NOT NULL,
            reason_code TEXT NOT NULL,
            filters_passed_json TEXT NOT NULL,
            filters_failed_json TEXT NOT NULL,
            signal_type TEXT,
            candidate_signal_id TEXT,
            audit_snapshot_json TEXT NOT NULL,
            content_fingerprint TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(run_id, role, ingest_seq)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_terminal_trade_decisions (
            decision_id TEXT PRIMARY KEY,
            decision_key TEXT NOT NULL UNIQUE,
            decision_scope TEXT NOT NULL,
            run_id TEXT,
            role TEXT,
            mint TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            parameter_set_id TEXT,
            decision_at_us INTEGER NOT NULL,
            trade_or_skip TEXT NOT NULL CHECK(trade_or_skip IN ('TRADE','SKIP')),
            decision_reason TEXT NOT NULL,
            candidate_signal_id TEXT,
            paper_entry_route_id TEXT,
            source_evaluation_id TEXT,
            content_fingerprint TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_position_mtm_marks (
            mark_id TEXT PRIMARY KEY,
            paper_position_id TEXT NOT NULL,
            signal_key TEXT NOT NULL,
            mint TEXT NOT NULL,
            track_id TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            ingest_seq INTEGER NOT NULL,
            source_event_key TEXT NOT NULL,
            observation_fingerprint TEXT NOT NULL,
            price_identity TEXT NOT NULL,
            market_price_numerator_raw TEXT NOT NULL,
            market_price_denominator_raw TEXT NOT NULL,
            current_virtual_token_reserve_raw TEXT NOT NULL,
            derived_token_input_raw TEXT NOT NULL,
            exit_price_impact_bps INTEGER NOT NULL,
            simulated_liquidation_price_numerator_raw TEXT NOT NULL,
            simulated_liquidation_price_denominator_raw TEXT NOT NULL,
            gross_market_value_lamports INTEGER NOT NULL,
            gross_liquidation_proceeds_lamports INTEGER NOT NULL,
            entry_principal_lamports INTEGER NOT NULL,
            entry_explicit_cost_lamports INTEGER NOT NULL,
            estimated_exit_explicit_cost_lamports INTEGER NOT NULL,
            market_return_bps INTEGER NOT NULL,
            net_mtm_pnl_lamports INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(paper_position_id, ingest_seq)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_track_equity_marks (
            equity_mark_id TEXT PRIMARY KEY,
            track_id TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            ingest_seq INTEGER NOT NULL,
            source_event_key TEXT NOT NULL,
            realized_closed_pnl_lamports INTEGER NOT NULL,
            open_position_count INTEGER NOT NULL,
            open_position_net_mtm_pnl_lamports INTEGER NOT NULL,
            equity_pnl_lamports INTEGER NOT NULL,
            open_position_details_json TEXT NOT NULL,
            content_fingerprint TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(track_id, ingest_seq, source_event_key)
        )
        """
    )
    conn.commit()


class PaperRuntimeObservabilityV01:
    def __init__(
        self,
        conn: sqlite3.Connection,
        cost_model: PaperCostModelV02,
        *,
        impact_provider: type[RuntimeExitPriceImpactV01] = RuntimeExitPriceImpactV01,
    ) -> None:
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.cost_model = cost_model
        self.impact_provider = impact_provider
        _create_schema(conn)

    def record_strategy_evaluation(
        self,
        item: StrategyEvaluationAuditInput,
    ) -> str:
        if not item.run_id or not item.role or not item.mint:
            raise ValueError("run_id/role/mint required")
        if item.ingest_seq < 0 or item.evaluated_at_us < 0:
            raise ValueError("ingest_seq/evaluated_at_us must be >= 0")

        payload = {
            "run_id": item.run_id,
            "role": item.role,
            "mint": item.mint,
            "strategy_version": item.strategy_version,
            "parameter_set_id": item.parameter_set_id,
            "ingest_seq": int(item.ingest_seq),
            "evaluated_at_us": int(item.evaluated_at_us),
            "current_state": item.current_state,
            "transition_occurred": bool(item.transition_occurred),
            "reason_code": item.reason_code,
            "filters_passed": list(item.filters_passed),
            "filters_failed": list(item.filters_failed),
            "signal_type": item.signal_type,
            "candidate_signal_id": item.candidate_signal_id,
            "audit_snapshot": item.audit_snapshot,
        }
        fp = _fingerprint(payload)
        evaluation_id = _hash_id(
            "PSE", SCHEMA_VERSION, item.run_id, item.role, item.ingest_seq
        )
        existing = self.conn.execute(
            "SELECT content_fingerprint FROM paper_strategy_evaluations WHERE evaluation_id=?",
            (evaluation_id,),
        ).fetchone()
        if existing is not None:
            if str(existing["content_fingerprint"]) != fp:
                raise ObservabilityDeterminismConflict(
                    f"strategy evaluation replay conflict: {evaluation_id}"
                )
            return evaluation_id

        self.conn.execute(
            """
            INSERT INTO paper_strategy_evaluations (
                evaluation_id, run_id, role, mint, strategy_version,
                parameter_set_id, ingest_seq, evaluated_at_us, current_state,
                transition_occurred, reason_code, filters_passed_json,
                filters_failed_json, signal_type, candidate_signal_id,
                audit_snapshot_json, content_fingerprint, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                evaluation_id, item.run_id, item.role, item.mint,
                item.strategy_version, item.parameter_set_id, int(item.ingest_seq),
                int(item.evaluated_at_us), item.current_state,
                1 if item.transition_occurred else 0, item.reason_code,
                _canonical_json(list(item.filters_passed)),
                _canonical_json(list(item.filters_failed)),
                item.signal_type, item.candidate_signal_id,
                _canonical_json(item.audit_snapshot), fp,
                datetime.now(timezone.utc).isoformat(timespec="microseconds"),
            ),
        )
        self.conn.commit()
        return evaluation_id

    def record_terminal_decision(
        self,
        item: TerminalDecisionAuditInput,
    ) -> str:
        if not item.decision_key or not item.decision_scope or not item.mint:
            raise ValueError("decision_key/decision_scope/mint required")
        if item.decision_at_us < 0:
            raise ValueError("decision_at_us must be >= 0")
        trade_or_skip = TradeOrSkip(item.trade_or_skip)
        if trade_or_skip is TradeOrSkip.TRADE and not item.candidate_signal_id:
            raise ValueError("TRADE terminal decision requires candidate_signal_id")
        if trade_or_skip is TradeOrSkip.SKIP and not item.decision_reason:
            raise ValueError("SKIP terminal decision requires decision_reason")

        payload = {
            **asdict(item),
            "trade_or_skip": trade_or_skip.value,
        }
        fp = _fingerprint(payload)
        decision_id = _hash_id("PTD", SCHEMA_VERSION, item.decision_key)
        existing = self.conn.execute(
            "SELECT content_fingerprint FROM paper_terminal_trade_decisions WHERE decision_key=?",
            (item.decision_key,),
        ).fetchone()
        if existing is not None:
            if str(existing["content_fingerprint"]) != fp:
                raise ObservabilityDeterminismConflict(
                    f"terminal decision replay conflict: {item.decision_key}"
                )
            return decision_id

        self.conn.execute(
            """
            INSERT INTO paper_terminal_trade_decisions (
                decision_id, decision_key, decision_scope, run_id, role, mint,
                strategy_version, parameter_set_id, decision_at_us, trade_or_skip,
                decision_reason, candidate_signal_id, paper_entry_route_id,
                source_evaluation_id, content_fingerprint, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                decision_id, item.decision_key, item.decision_scope, item.run_id,
                item.role, item.mint, item.strategy_version, item.parameter_set_id,
                int(item.decision_at_us), trade_or_skip.value, item.decision_reason,
                item.candidate_signal_id, item.paper_entry_route_id,
                item.source_evaluation_id, fp,
                datetime.now(timezone.utc).isoformat(timespec="microseconds"),
            ),
        )
        self.conn.commit()
        return decision_id

    def _position_lineage(self, paper_position_id: str) -> sqlite3.Row:
        row = self.conn.execute(
            """
            SELECT
                p.paper_position_id, p.signal_key, p.mint, p.track_id,
                p.price_identity, p.state, p.open_at, p.closed_at,
                p.filled_size_lamports, p.entry_price_numerator_raw,
                p.entry_price_denominator_raw,
                er.total_entry_explicit_cost_lamports
            FROM paper_positions p
            JOIN paper_orders o ON o.paper_order_id = p.paper_order_id
            JOIN paper_entry_routes er ON er.candidate_id = o.signal_key
            WHERE p.paper_position_id=?
            """,
            (paper_position_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"unknown paper_position_id: {paper_position_id}")
        return row

    def record_position_mark(
        self,
        paper_position_id: str,
        observation: PositionMarkObservation,
        *,
        observed_priority_fee_lamports: int | None = None,
        venue_fee_bps: int | None = None,
    ) -> PositionMtmMark | None:
        if observation.is_gap_recovery:
            return None
        if observation.ingest_seq < 0:
            raise ValueError("ingest_seq must be >= 0")
        if observation.price_numerator_raw <= 0 or observation.price_denominator_raw <= 0:
            raise ValueError("market price must be > 0")
        if observation.current_virtual_token_reserve_raw <= 0:
            raise ValueError("current_virtual_token_reserve_raw must be > 0")

        lineage = self._position_lineage(paper_position_id)
        if str(lineage["mint"]) != observation.mint:
            return None
        if str(lineage["price_identity"]) != observation.price_identity:
            return None

        observed_at = _utc(observation.observed_at)
        open_at = _dt_parse(str(lineage["open_at"]))
        if observed_at < open_at:
            return None
        if lineage["closed_at"] is not None:
            closed_at = _dt_parse(str(lineage["closed_at"]))
            if observed_at > closed_at:
                return None

        existing_latest = self.conn.execute(
            """
            SELECT ingest_seq FROM paper_position_mtm_marks
            WHERE paper_position_id=? ORDER BY ingest_seq DESC LIMIT 1
            """,
            (paper_position_id,),
        ).fetchone()

        obs_payload = {
            "mint": observation.mint,
            "observed_at": _dt_text(observed_at),
            "ingest_seq": int(observation.ingest_seq),
            "source_event_key": observation.source_event_key,
            "price_identity": observation.price_identity,
            "price_numerator_raw": int(observation.price_numerator_raw),
            "price_denominator_raw": int(observation.price_denominator_raw),
            "current_virtual_token_reserve_raw": int(observation.current_virtual_token_reserve_raw),
        }
        obs_fp = _fingerprint(obs_payload)
        mark_id = _hash_id("PMM", SCHEMA_VERSION, paper_position_id, observation.ingest_seq)

        existing = self.conn.execute(
            "SELECT * FROM paper_position_mtm_marks WHERE mark_id=?",
            (mark_id,),
        ).fetchone()
        if existing is not None:
            if str(existing["observation_fingerprint"]) != obs_fp:
                raise ObservabilityDeterminismConflict(
                    f"position mark replay conflict: {mark_id}"
                )
            return self._mark_from_row(existing)

        if existing_latest is not None and int(observation.ingest_seq) < int(existing_latest["ingest_seq"]):
            return None

        principal = int(lineage["filled_size_lamports"])
        entry_num = int(lineage["entry_price_numerator_raw"])
        entry_den = int(lineage["entry_price_denominator_raw"])
        entry_cost = int(lineage["total_entry_explicit_cost_lamports"])

        impact = self.impact_provider.for_position(
            price_identity=observation.price_identity,
            filled_size_lamports=principal,
            entry_price_numerator_raw=entry_num,
            entry_price_denominator_raw=entry_den,
            current_virtual_token_reserve_raw=int(observation.current_virtual_token_reserve_raw),
        )
        if not impact.available or impact.derived_token_input_raw is None or impact.router_price_impact_bps is None:
            return None

        market_price = Fraction(
            int(observation.price_numerator_raw), int(observation.price_denominator_raw)
        )
        entry_price = Fraction(entry_num, entry_den)
        token_input = int(impact.derived_token_input_raw)
        impact_factor = Fraction(10_000 - int(impact.router_price_impact_bps), 10_000)
        liquidation_price = market_price * impact_factor

        gross_market_value = _floor_fraction(Fraction(token_input) * market_price)
        gross_liquidation = _floor_fraction(Fraction(token_input) * liquidation_price)
        if gross_liquidation <= 0:
            return None

        exit_costs = self.cost_model.explicit_costs(
            side=CostSide.EXIT,
            notional_lamports=gross_liquidation,
            observed_priority_fee_lamports=observed_priority_fee_lamports,
            venue_fee_bps=venue_fee_bps,
        )
        net_mtm = (
            gross_liquidation
            - principal
            - entry_cost
            - int(exit_costs.total_explicit_cost_lamports)
        )
        market_return_bps = _round_half_away_from_zero(
            ((market_price / entry_price) - 1) * 10_000
        )

        canonical_liq = liquidation_price
        self.conn.execute(
            """
            INSERT INTO paper_position_mtm_marks (
                mark_id, paper_position_id, signal_key, mint, track_id,
                observed_at, ingest_seq, source_event_key, observation_fingerprint,
                price_identity, market_price_numerator_raw, market_price_denominator_raw,
                current_virtual_token_reserve_raw, derived_token_input_raw,
                exit_price_impact_bps, simulated_liquidation_price_numerator_raw,
                simulated_liquidation_price_denominator_raw, gross_market_value_lamports,
                gross_liquidation_proceeds_lamports, entry_principal_lamports,
                entry_explicit_cost_lamports, estimated_exit_explicit_cost_lamports,
                market_return_bps, net_mtm_pnl_lamports, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                mark_id, paper_position_id, str(lineage["signal_key"]),
                observation.mint, str(lineage["track_id"]), _dt_text(observed_at),
                int(observation.ingest_seq), observation.source_event_key, obs_fp,
                observation.price_identity, str(market_price.numerator),
                str(market_price.denominator), str(observation.current_virtual_token_reserve_raw),
                str(token_input), int(impact.router_price_impact_bps),
                str(canonical_liq.numerator), str(canonical_liq.denominator),
                gross_market_value, gross_liquidation, principal, entry_cost,
                int(exit_costs.total_explicit_cost_lamports), market_return_bps,
                net_mtm, datetime.now(timezone.utc).isoformat(timespec="microseconds"),
            ),
        )
        self.conn.commit()
        row = self.conn.execute(
            "SELECT * FROM paper_position_mtm_marks WHERE mark_id=?", (mark_id,)
        ).fetchone()
        assert row is not None
        return self._mark_from_row(row)

    def record_track_equity_mark(
        self,
        *,
        track_id: str,
        observed_at: datetime,
        ingest_seq: int,
        source_event_key: str,
        realized_closed_pnl_lamports: int,
        open_position_net_mtm: Iterable[tuple[str, int]],
    ) -> TrackEquityMark:
        if track_id not in LOCKED_TRACKS:
            raise ValueError(f"unsupported track_id: {track_id}")
        if ingest_seq < 0:
            raise ValueError("ingest_seq must be >= 0")
        details = tuple(sorted((str(pid), int(pnl)) for pid, pnl in open_position_net_mtm))
        if len({pid for pid, _ in details}) != len(details):
            raise ValueError("duplicate paper_position_id in open_position_net_mtm")
        open_total = sum(pnl for _, pnl in details)
        equity = int(realized_closed_pnl_lamports) + open_total
        observed = _utc(observed_at)
        payload = {
            "track_id": track_id,
            "observed_at": _dt_text(observed),
            "ingest_seq": int(ingest_seq),
            "source_event_key": source_event_key,
            "realized_closed_pnl_lamports": int(realized_closed_pnl_lamports),
            "open_position_details": list(details),
            "equity_pnl_lamports": equity,
        }
        fp = _fingerprint(payload)
        mark_id = _hash_id("PEQ", SCHEMA_VERSION, track_id, ingest_seq, source_event_key)
        existing = self.conn.execute(
            "SELECT * FROM paper_track_equity_marks WHERE equity_mark_id=?", (mark_id,)
        ).fetchone()
        if existing is not None:
            if str(existing["content_fingerprint"]) != fp:
                raise ObservabilityDeterminismConflict(
                    f"track equity replay conflict: {mark_id}"
                )
            return self._equity_from_row(existing)

        self.conn.execute(
            """
            INSERT INTO paper_track_equity_marks (
                equity_mark_id, track_id, observed_at, ingest_seq, source_event_key,
                realized_closed_pnl_lamports, open_position_count,
                open_position_net_mtm_pnl_lamports, equity_pnl_lamports,
                open_position_details_json, content_fingerprint, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                mark_id, track_id, _dt_text(observed), int(ingest_seq), source_event_key,
                int(realized_closed_pnl_lamports), len(details), open_total, equity,
                _canonical_json([list(x) for x in details]), fp,
                datetime.now(timezone.utc).isoformat(timespec="microseconds"),
            ),
        )
        self.conn.commit()
        row = self.conn.execute(
            "SELECT * FROM paper_track_equity_marks WHERE equity_mark_id=?", (mark_id,)
        ).fetchone()
        assert row is not None
        return self._equity_from_row(row)

    @staticmethod
    def _mark_from_row(row: sqlite3.Row) -> PositionMtmMark:
        return PositionMtmMark(
            mark_id=str(row["mark_id"]),
            paper_position_id=str(row["paper_position_id"]),
            signal_key=str(row["signal_key"]),
            mint=str(row["mint"]),
            track_id=str(row["track_id"]),
            observed_at=_dt_parse(str(row["observed_at"])),
            ingest_seq=int(row["ingest_seq"]),
            source_event_key=str(row["source_event_key"]),
            price_identity=str(row["price_identity"]),
            market_price_numerator_raw=int(row["market_price_numerator_raw"]),
            market_price_denominator_raw=int(row["market_price_denominator_raw"]),
            current_virtual_token_reserve_raw=int(row["current_virtual_token_reserve_raw"]),
            derived_token_input_raw=int(row["derived_token_input_raw"]),
            exit_price_impact_bps=int(row["exit_price_impact_bps"]),
            simulated_liquidation_price_numerator_raw=int(row["simulated_liquidation_price_numerator_raw"]),
            simulated_liquidation_price_denominator_raw=int(row["simulated_liquidation_price_denominator_raw"]),
            gross_market_value_lamports=int(row["gross_market_value_lamports"]),
            gross_liquidation_proceeds_lamports=int(row["gross_liquidation_proceeds_lamports"]),
            entry_principal_lamports=int(row["entry_principal_lamports"]),
            entry_explicit_cost_lamports=int(row["entry_explicit_cost_lamports"]),
            estimated_exit_explicit_cost_lamports=int(row["estimated_exit_explicit_cost_lamports"]),
            market_return_bps=int(row["market_return_bps"]),
            net_mtm_pnl_lamports=int(row["net_mtm_pnl_lamports"]),
        )

    @staticmethod
    def _equity_from_row(row: sqlite3.Row) -> TrackEquityMark:
        details = tuple((str(x[0]), int(x[1])) for x in json.loads(str(row["open_position_details_json"])))
        return TrackEquityMark(
            equity_mark_id=str(row["equity_mark_id"]),
            track_id=str(row["track_id"]),
            observed_at=_dt_parse(str(row["observed_at"])),
            ingest_seq=int(row["ingest_seq"]),
            source_event_key=str(row["source_event_key"]),
            realized_closed_pnl_lamports=int(row["realized_closed_pnl_lamports"]),
            open_position_count=int(row["open_position_count"]),
            open_position_net_mtm_pnl_lamports=int(row["open_position_net_mtm_pnl_lamports"]),
            equity_pnl_lamports=int(row["equity_pnl_lamports"]),
            open_position_details=details,
        )


def build_report(conn: sqlite3.Connection) -> ObservabilityReport:
    conn.row_factory = sqlite3.Row
    position_rows = conn.execute(
        """
        SELECT paper_position_id, signal_key, track_id, mint,
               COUNT(*) AS n, MIN(market_return_bps) AS mae,
               MAX(market_return_bps) AS mfe,
               MIN(net_mtm_pnl_lamports) AS min_mtm,
               MAX(net_mtm_pnl_lamports) AS max_mtm,
               MIN(ingest_seq) AS first_seq, MAX(ingest_seq) AS last_seq
        FROM paper_position_mtm_marks
        GROUP BY paper_position_id, signal_key, track_id, mint
        ORDER BY signal_key, track_id, paper_position_id
        """
    ).fetchall()
    positions = tuple(
        PositionExcursionSummary(
            paper_position_id=str(r["paper_position_id"]),
            signal_key=str(r["signal_key"]),
            track_id=str(r["track_id"]),
            mint=str(r["mint"]),
            marks=int(r["n"]),
            mae_bps=int(r["mae"]),
            mfe_bps=int(r["mfe"]),
            min_net_mtm_pnl_lamports=int(r["min_mtm"]),
            max_net_mtm_pnl_lamports=int(r["max_mtm"]),
            first_ingest_seq=int(r["first_seq"]),
            last_ingest_seq=int(r["last_seq"]),
        )
        for r in position_rows
    )

    tracks: list[TrackMtmSummary] = []
    for track_id in LOCKED_TRACKS:
        rows = conn.execute(
            """
            SELECT equity_pnl_lamports FROM paper_track_equity_marks
            WHERE track_id=? ORDER BY ingest_seq, observed_at, equity_mark_id
            """,
            (track_id,),
        ).fetchall()
        if not rows:
            tracks.append(TrackMtmSummary(track_id, 0, 0, 0, 0, 0))
            continue
        peak = 0
        max_dd = 0
        trough = 0
        final = 0
        for row in rows:
            equity = int(row["equity_pnl_lamports"])
            final = equity
            if equity > peak:
                peak = equity
            dd = peak - equity
            if dd > max_dd:
                max_dd = dd
                trough = equity
        tracks.append(
            TrackMtmSummary(
                track_id=track_id,
                equity_marks=len(rows),
                max_drawdown_lamports=max_dd,
                peak_equity_pnl_lamports=peak,
                trough_equity_pnl_lamports=trough,
                final_equity_pnl_lamports=final,
            )
        )

    evaluation_rows = int(conn.execute("SELECT COUNT(*) FROM paper_strategy_evaluations").fetchone()[0])
    terminal = conn.execute(
        "SELECT trade_or_skip, decision_reason, COUNT(*) AS n FROM paper_terminal_trade_decisions GROUP BY trade_or_skip, decision_reason"
    ).fetchall()
    trades = sum(int(r["n"]) for r in terminal if str(r["trade_or_skip"]) == "TRADE")
    skips = sum(int(r["n"]) for r in terminal if str(r["trade_or_skip"]) == "SKIP")
    reasons = tuple(sorted((str(r["decision_reason"]), int(r["n"])) for r in terminal if str(r["trade_or_skip"]) == "SKIP"))
    strategy = StrategyDecisionSummary(
        evaluation_rows=evaluation_rows,
        terminal_decisions=sum(int(r["n"]) for r in terminal),
        trades=trades,
        skips=skips,
        skip_reasons=reasons,
    )

    return ObservabilityReport(
        schema_version=SCHEMA_VERSION,
        engine_version=ENGINE_VERSION,
        model_id=MODEL_ID,
        model_fingerprint=MODEL_FINGERPRINT,
        positions=positions,
        tracks=tuple(tracks),
        strategy=strategy,
        cross_track_aggregation_status="PROHIBITED_ALTERNATIVE_EXIT_TRACKS_REPORTED_SEPARATELY",
    )


def canonical_report_digest(report: ObservabilityReport) -> str:
    body = json.dumps(asdict(report), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()
