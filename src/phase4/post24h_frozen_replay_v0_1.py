from __future__ import annotations

import csv
import hashlib
import io
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path
from typing import Any, Iterable

from .paper_continuous_market_source_v0_2 import (
    ContinuousMarketSourceRecordV02,
    ContinuousMarketSourceV02,
)
from .paper_cost_baseline_v0_1 import P4_COST_BASELINE_0001
from .paper_cost_model_v0_2 import CostSide, FillDecision, PaperCostModelV02, RationalPrice
from .paper_exit_orchestrator_v0_1 import (
    ExitMarketObservation,
    ExitPositionRef,
    ExitReason,
    LOCKED_EXIT_SPECS,
    PaperExitOrchestratorV01,
)
from .runtime_exit_price_impact_v0_1 import MODEL_FINGERPRINT as EXIT_IMPACT_FINGERPRINT
from .runtime_exit_price_impact_v0_1 import MODEL_ID as EXIT_IMPACT_MODEL_ID
from .runtime_exit_price_impact_v0_1 import RuntimeExitPriceImpactV01


MODEL_ID = "P4-POST24H-FROZEN-REPLAY-FOUNDATION-0001"
RUN_ID = "15f8b07e-1b9b-464f-a822-01e3fbb8333e"
SOURCE_ANCHOR = 766_710
FROZEN_WATERMARK = 1_163_003
EXPECTED_SOURCE_ROWS = 396_293
EXPECTED_PHYSICAL_ENTRIES = 635
EXPECTED_COHORT_SHA256 = "167dd087ab516996b8cc174a5a55cc1b7b3d9f7b86970ac01f598cd82c121f52"
EXPECTED_PAPER_SHA256 = "af1449aac08522fcfafad7faf5efa11ee0c1e21c09130e82e049758091d53932"
TRACK_ORDER = {"FINAL-A": 0, "FINAL-B": 1, "SENS-C": 2}
EXPECTED = {
    "FINAL-A": {"positions": 635, "completed": 607, "pending": 28, "FALLBACK": 437,
                "TAKE_PROFIT": 170, "gross_execution_pnl_lamports": 867_281_418,
                "entry_explicit_cost_lamports": 1_429_485_000,
                "exit_explicit_cost_lamports": 1_440_326_321,
                "net_pnl_lamports": -2_002_529_903},
    "FINAL-B": {"positions": 635, "completed": 606, "pending": 29, "FALLBACK": 523,
                "TRAIL": 83, "gross_execution_pnl_lamports": 915_692_495,
                "entry_explicit_cost_lamports": 1_427_130_000,
                "exit_explicit_cost_lamports": 1_438_576_448,
                "net_pnl_lamports": -1_950_013_953},
    "SENS-C": {"positions": 423, "completed": 395, "pending": 28, "FALLBACK": 371,
               "TAKE_PROFIT": 24, "gross_execution_pnl_lamports": 1_127_954_477,
               "entry_explicit_cost_lamports": 930_225_000,
               "exit_explicit_cost_lamports": 944_324_621,
               "net_pnl_lamports": -746_595_144},
}


class ReplayMismatch(RuntimeError):
    pass


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(value: Any) -> str:
    body = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


MODEL_FINGERPRINT = canonical_sha256({
    "model_id": MODEL_ID,
    "run_id": RUN_ID,
    "source_anchor": SOURCE_ANCHOR,
    "frozen_watermark": FROZEN_WATERMARK,
    "accepted_cohort_sha256": EXPECTED_COHORT_SHA256,
    "locked_exit_specs": {track: LOCKED_EXIT_SPECS[track].exit_variant for track in TRACK_ORDER},
    "cost_model_fingerprint": P4_COST_BASELINE_0001.fingerprint,
    "exit_impact_model_id": EXIT_IMPACT_MODEL_ID,
    "exit_impact_fingerprint": EXIT_IMPACT_FINGERPRINT,
})


def _dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ReplayMismatch(f"naive timestamp in frozen input: {value}")
    return parsed.astimezone(timezone.utc)


def is_post_entry_source_row(ref: ExitPositionRef, ingest_seq: int) -> bool:
    return int(ingest_seq) > ref.entry_market_ingest_seq


def _ro(path: str | Path) -> sqlite3.Connection:
    absolute = Path(path).resolve()
    conn = sqlite3.connect(f"file:{absolute.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    if conn.execute("PRAGMA query_only").fetchone()[0] != 1:
        conn.close()
        raise ReplayMismatch(f"query_only unavailable for {absolute}")
    return conn


@dataclass(frozen=True, slots=True)
class FrozenEntry:
    route_id: str
    signal_key: str
    mint: str
    signal_observed_at: str
    signal_ingest_seq: int
    reference_price_identity: str
    reference_price_numerator_raw: int
    reference_price_denominator_raw: int
    entry_observed_at: str
    entry_ingest_seq: int
    source_event_key: str
    principal_lamports: int
    entry_price_numerator_raw: int
    entry_price_denominator_raw: int
    entry_explicit_cost_lamports: int


def load_frozen_entries(paper_db: str | Path, source_db: str | Path) -> tuple[list[FrozenEntry], dict[str, Any]]:
    with _ro(paper_db) as paper:
        rows = paper.execute(
            """
            SELECT route_id, candidate_id AS signal_key, mint, signal_observed_at,
                   signal_ingest_seq, reference_price_identity,
                   reference_price_numerator_raw, reference_price_denominator_raw,
                   selected_market_observed_at AS entry_observed_at,
                   selected_market_ingest_seq AS entry_ingest_seq,
                   selected_source_event_key AS source_event_key,
                   requested_size_lamports AS principal_lamports,
                   simulated_entry_price_numerator_raw AS entry_price_numerator_raw,
                   simulated_entry_price_denominator_raw AS entry_price_denominator_raw,
                   total_entry_explicit_cost_lamports AS entry_explicit_cost_lamports
            FROM paper_entry_routes WHERE state='FILLED'
            ORDER BY candidate_id
            """
        ).fetchall()
        entries = [FrozenEntry(
            route_id=r["route_id"], signal_key=r["signal_key"], mint=r["mint"],
            signal_observed_at=r["signal_observed_at"], signal_ingest_seq=int(r["signal_ingest_seq"]),
            reference_price_identity=r["reference_price_identity"],
            reference_price_numerator_raw=int(r["reference_price_numerator_raw"]),
            reference_price_denominator_raw=int(r["reference_price_denominator_raw"]),
            entry_observed_at=r["entry_observed_at"], entry_ingest_seq=int(r["entry_ingest_seq"]),
            source_event_key=r["source_event_key"], principal_lamports=int(r["principal_lamports"]),
            entry_price_numerator_raw=int(r["entry_price_numerator_raw"]),
            entry_price_denominator_raw=int(r["entry_price_denominator_raw"]),
            entry_explicit_cost_lamports=int(r["entry_explicit_cost_lamports"]),
        ) for r in rows]
        accepted_digest_rows = [dict(r) for r in paper.execute(
            """
            SELECT c.signal_key AS signal_key,
                   c.mint AS mint,
                   c.route_id AS route_id,
                   r.candidate_id AS candidate_id,
                   r.signal_observed_at AS signal_observed_at,
                   r.signal_ingest_seq AS signal_ingest_seq,
                   r.requested_size_lamports AS requested_size_lamports,
                   r.selected_market_observed_at AS selected_market_observed_at,
                   r.selected_market_ingest_seq AS selected_market_ingest_seq,
                   r.selected_source_event_key AS selected_source_event_key,
                   r.simulated_entry_price_numerator_raw AS simulated_entry_price_numerator_raw,
                   r.simulated_entry_price_denominator_raw AS simulated_entry_price_denominator_raw,
                   r.total_entry_explicit_cost_lamports AS total_entry_explicit_cost_lamports
            FROM paper_continuous_signal_contexts_v0_1 AS c
            JOIN paper_entry_routes AS r ON r.route_id = c.route_id
            WHERE r.state='FILLED'
            """
        )]
        position_sets = {
            track: {(r["signal_key"], r["mint"], r["open_at"], int(r["filled_size_lamports"]),
                     int(r["entry_price_numerator_raw"]), int(r["entry_price_denominator_raw"]))
                    for r in paper.execute("SELECT * FROM paper_positions WHERE track_id=?", (track,))}
            for track in TRACK_ORDER
        }

    if len(entries) != EXPECTED_PHYSICAL_ENTRIES or len({e.mint for e in entries}) != EXPECTED_PHYSICAL_ENTRIES:
        raise ReplayMismatch(f"physical cohort mismatch: {len(entries)} entries")
    if position_sets["FINAL-A"] != position_sets["FINAL-B"]:
        raise ReplayMismatch("FINAL-A and FINAL-B entry economics differ")
    if not position_sets["SENS-C"].issubset(position_sets["FINAL-A"]):
        raise ReplayMismatch("SENS-C is not an exact entry subset")

    with _ro(source_db) as source:
        lineage = []
        for entry in entries:
            row = source.execute(
                "SELECT rowid,event_key,mint,decoded_at_utc FROM pump_events WHERE rowid=?",
                (entry.entry_ingest_seq,),
            ).fetchone()
            ok = bool(row and row["mint"] == entry.mint
                      and f"{row['event_key']}:MARKET" == entry.source_event_key
                      and row["decoded_at_utc"] == entry.entry_observed_at
                      and SOURCE_ANCHOR < int(row["rowid"]) <= FROZEN_WATERMARK)
            lineage.append(ok)
    if not all(lineage):
        raise ReplayMismatch(f"normalized entry lineage mismatch: {sum(lineage)}/{len(lineage)}")

    if len(accepted_digest_rows) != EXPECTED_PHYSICAL_ENTRIES:
        raise ReplayMismatch(
            f"accepted digest cohort mismatch: {len(accepted_digest_rows)} rows"
        )
    accepted_digest_rows.sort(key=lambda row: (str(row["signal_key"]), str(row["mint"])))
    accepted_digest_payload = "\n".join(
        json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        for row in accepted_digest_rows
    )
    computed_accepted_digest = hashlib.sha256(
        accepted_digest_payload.encode("utf-8")
    ).hexdigest()
    accepted_digest_exact = computed_accepted_digest == EXPECTED_COHORT_SHA256
    if not accepted_digest_exact:
        raise ReplayMismatch(
            "accepted physical cohort SHA256 mismatch: "
            f"computed={computed_accepted_digest}, expected={EXPECTED_COHORT_SHA256}"
        )

    projection = [asdict(entry) for entry in entries]
    proof = {
        "expected_accepted_cohort_sha256": EXPECTED_COHORT_SHA256,
        "computed_accepted_cohort_sha256": computed_accepted_digest,
        "accepted_cohort_sha256": computed_accepted_digest,
        "accepted_cohort_sha256_exact_match": accepted_digest_exact,
        "accepted_cohort_sha256_reproduced": accepted_digest_exact,
        "transparent_projection_sha256": canonical_sha256(projection),
        "physical_entry_count": len(entries),
        "distinct_mints": len({e.mint for e in entries}),
        "final_a_equals_final_b": True,
        "sens_c_exact_subset": True,
        "normalized_source_lineage_exact": len(entries),
    }
    return entries, proof


def load_frozen_market_paths(
    source_db: str | Path, cohort_mints: set[str]
) -> tuple[dict[str, tuple[ContinuousMarketSourceRecordV02, ...]], dict[str, Any]]:
    source = ContinuousMarketSourceV02(source_db, start_after_p1_rowid=SOURCE_ANCHOR)
    cursor = SOURCE_ANCHOR
    paths: dict[str, list[ContinuousMarketSourceRecordV02]] = defaultdict(list)
    raw = normalized = gaps = 0
    while cursor < FROZEN_WATERMARK:
        limit = min(10_000, FROZEN_WATERMARK - cursor)
        batch = source.fetch_batch(after_p1_rowid=cursor, batch_size=limit)
        if batch.highest_fetched_p1_rowid <= cursor:
            raise ReplayMismatch(f"source cursor failed to advance at {cursor}")
        if batch.highest_fetched_p1_rowid > FROZEN_WATERMARK:
            raise ReplayMismatch("source adapter crossed frozen watermark")
        raw += batch.raw_rows_fetched
        for record in batch.records:
            if not (SOURCE_ANCHOR < record.ingest_seq <= FROZEN_WATERMARK):
                raise ReplayMismatch(f"normalized source boundary violation: {record.ingest_seq}")
            normalized += 1
            gaps += int(record.is_gap_recovery)
            if record.mint in cohort_mints:
                paths[record.mint].append(record)
        cursor = batch.highest_fetched_p1_rowid
    if cursor != FROZEN_WATERMARK or raw != EXPECTED_SOURCE_ROWS:
        raise ReplayMismatch(f"frozen source range mismatch: cursor={cursor}, raw={raw}")
    frozen = {mint: tuple(rows) for mint, rows in paths.items()}
    return frozen, {
        "anchor": SOURCE_ANCHOR, "watermark": FROZEN_WATERMARK,
        "raw_rows": raw, "normalized_rows": normalized,
        "cohort_normalized_rows": sum(map(len, frozen.values())), "gap_rows": gaps,
        "source_model_id": source.model_id,
        "source_model_fingerprint": source.model_fingerprint,
    }


def _load_refs(paper_db: str | Path) -> tuple[list[ExitPositionRef], dict[str, dict[str, Any]]]:
    with _ro(paper_db) as conn:
        rows = conn.execute(
            "SELECT * FROM paper_exit_track_states ORDER BY track_id,paper_position_id"
        ).fetchall()
        actual_intents = {r["paper_position_id"]: dict(r) for r in conn.execute("SELECT * FROM paper_exit_intents")}
    refs = [ExitPositionRef(
        paper_position_id=r["paper_position_id"], paper_order_id=r["paper_order_id"],
        signal_key=r["signal_key"], mint=r["mint"], track_id=r["track_id"],
        exit_variant=r["exit_variant"], price_identity=r["price_identity"],
        signal_observed_at=_dt(r["signal_observed_at"]), signal_ingest_seq=int(r["signal_ingest_seq"]),
        rule_reference_price_numerator_raw=int(r["rule_reference_price_numerator_raw"]),
        rule_reference_price_denominator_raw=int(r["rule_reference_price_denominator_raw"]),
        entry_market_observed_at=_dt(r["entry_market_observed_at"]),
        entry_market_ingest_seq=int(r["entry_market_ingest_seq"]), open_at=_dt(r["open_at"]),
        entry_price_numerator_raw=int(r["entry_price_numerator_raw"]),
        entry_price_denominator_raw=int(r["entry_price_denominator_raw"]),
    ) for r in rows]
    return refs, actual_intents


def replay_intents(
    paper_db: str | Path,
    paths: dict[str, tuple[ContinuousMarketSourceRecordV02, ...]],
) -> tuple[dict[str, Any], dict[str, Any], str]:
    refs, actual = _load_refs(paper_db)
    memory = sqlite3.connect(":memory:")
    orchestrator = PaperExitOrchestratorV01(memory)
    by_mint: dict[str, list[ExitPositionRef]] = defaultdict(list)
    for ref in refs:
        orchestrator.bind_position(ref)
        by_mint[ref.mint].append(ref)
    for mint in sorted(by_mint):
        active = list(by_mint[mint])
        for row in paths.get(mint, ()):
            if not active:
                break
            obs = ExitMarketObservation(
                mint=row.mint, observed_at=row.observed_at, ingest_seq=row.ingest_seq,
                price_identity=row.price_identity, price_numerator_raw=row.price_numerator_raw,
                price_denominator_raw=row.price_denominator_raw,
                is_gap_recovery=row.is_gap_recovery, source_event_key=f"{row.event_key}:MARKET",
            )
            still_active: list[ExitPositionRef] = []
            for ref in active:
                # Live positions did not exist when earlier source rows flowed.
                # Pre-binding all refs offline must not expose that history.
                if not is_post_entry_source_row(ref, row.ingest_seq):
                    still_active.append(ref)
                    continue
                orchestrator.on_market_observation(ref, obs)
                if orchestrator.get_intent_for_position(ref.paper_position_id) is None:
                    still_active.append(ref)
            active = still_active
    end = datetime.max.replace(tzinfo=timezone.utc)
    for ref in refs:
        orchestrator.on_clock(ref, end)

    replay: dict[str, Any] = {}
    mismatches: list[str] = []
    for ref in refs:
        intent = orchestrator.get_intent_for_position(ref.paper_position_id)
        track = orchestrator.get_track(ref.paper_position_id)
        if intent is None or track is None:
            mismatches.append(f"{ref.paper_position_id}:missing replay intent/track")
            continue
        replay[ref.paper_position_id] = (ref, intent, track)
        old = actual.get(ref.paper_position_id)
        if old is None:
            mismatches.append(f"{ref.paper_position_id}:missing actual intent")
            continue
        checks = {
            "reason": (intent.reason.value, old["reason"]),
            "requested_at": (intent.requested_at.isoformat(timespec="microseconds"), old["requested_at"]),
            "trigger_ingest_seq": (intent.trigger_ingest_seq, old["trigger_ingest_seq"]),
            "trigger_source_event_key": (intent.trigger_source_event_key, old["trigger_source_event_key"]),
        }
        for label, pair in checks.items():
            if pair[0] != pair[1]:
                mismatches.append(f"{ref.paper_position_id}:{label}:{pair[0]}!={pair[1]}")
    if mismatches:
        raise ReplayMismatch("intent replay differs from ACTUAL: " + "; ".join(mismatches[:20]))
    digest_rows = [{
        "paper_position_id": pid, "reason": value[1].reason.value,
        "requested_at": value[1].requested_at.isoformat(timespec="microseconds"),
        "trigger_ingest_seq": value[1].trigger_ingest_seq,
    } for pid, value in sorted(replay.items())]
    return replay, {"positions": len(refs), "intent_exact_matches": len(refs)}, canonical_sha256(digest_rows)


def _reference(intent: Any, track: Any) -> tuple[str, datetime, int, int, int]:
    if intent.reason in (ExitReason.TAKE_PROFIT, ExitReason.TRAIL):
        return ("TRIGGER_MARKET", intent.trigger_observed_at, int(intent.trigger_ingest_seq),
                int(intent.trigger_price_numerator_raw), int(intent.trigger_price_denominator_raw))
    if track.last_fresh_at is not None:
        return ("LAST_FRESH_MARKET", track.last_fresh_at, int(track.last_fresh_ingest_seq),
                int(track.last_fresh_price_numerator_raw), int(track.last_fresh_price_denominator_raw))
    return ("RULE_REFERENCE_FALLBACK", track.signal_observed_at, int(track.signal_ingest_seq),
            int(track.rule_reference_price_numerator_raw), int(track.rule_reference_price_denominator_raw))


@dataclass(frozen=True, slots=True)
class ReplayExecutionInputV01:
    signal_key: str
    mint: str
    price_identity: str
    entry_observed_at: datetime
    entry_ingest_seq: int
    open_at: datetime
    entry_principal_lamports: int
    entry_price_numerator_raw: int
    entry_price_denominator_raw: int
    entry_explicit_cost_lamports: int
    exit_reason: str
    requested_at: datetime
    reference_source: str
    reference_observed_at: datetime
    reference_ingest_seq: int
    reference_price_numerator_raw: int
    reference_price_denominator_raw: int


@dataclass(frozen=True, slots=True)
class ReplayExecutionOutcomeV01:
    exit_state: str
    rejection_state: str | None
    attempt_count: int
    fill_source_row: int | None
    fill_source_event_key: str | None
    fill_observed_at: datetime | None
    exit_price_numerator_raw: int | None
    exit_price_denominator_raw: int | None
    gross_exit_proceeds_lamports: int | None
    exit_explicit_cost_lamports: int | None
    gross_execution_pnl_lamports: int | None
    net_pnl_lamports: int | None
    holding_time_us: int | None


def execute_exit_intent_v0_1(
    execution: ReplayExecutionInputV01,
    path: Iterable[ContinuousMarketSourceRecordV02],
    *,
    frozen_watermark: int = FROZEN_WATERMARK,
) -> ReplayExecutionOutcomeV01:
    """Execute one explicit exit intent with the accepted Phase-4 economics.

    This pure research helper has no dependency on ACTUAL exit intent/route
    tables. The T001 verification layer remains responsible for comparing its
    result to persisted ACTUAL truth.
    """
    cost_model = PaperCostModelV02(P4_COST_BASELINE_0001)
    ready = cost_model.execution_ready_at(execution.requested_at, CostSide.EXIT)
    attempts = 0
    selected: dict[str, Any] | None = None
    last_attempt_key: tuple[datetime, int] | None = None
    unique_path: list[ContinuousMarketSourceRecordV02] = []
    seen_source_keys: dict[tuple[datetime, int], ContinuousMarketSourceRecordV02] = {}
    for obs in path:
        if obs.ingest_seq > frozen_watermark:
            raise ReplayMismatch(f"execution source row exceeds frozen watermark: {obs.ingest_seq}")
        source_key = (obs.observed_at, obs.ingest_seq)
        prior = seen_source_keys.get(source_key)
        if prior is not None:
            if prior != obs:
                raise ReplayMismatch(
                    "same execution observed_at + ingest_seq has conflicting source content"
                )
            continue
        seen_source_keys[source_key] = obs
        unique_path.append(obs)
    ordered_path = sorted(unique_path, key=lambda obs: (obs.observed_at, obs.ingest_seq))
    for obs in ordered_path:
        if obs.mint != execution.mint:
            continue
        if obs.is_gap_recovery or obs.price_identity != execution.price_identity:
            continue
        if obs.ingest_seq <= execution.entry_ingest_seq:
            continue
        if (obs.observed_at, obs.ingest_seq) <= (
            execution.entry_observed_at,
            execution.entry_ingest_seq,
        ):
            continue
        if obs.observed_at < ready or (obs.observed_at, obs.ingest_seq) <= (
            execution.reference_observed_at,
            execution.reference_ingest_seq,
        ):
            continue
        observation_key = (obs.observed_at, obs.ingest_seq)
        if last_attempt_key is not None and observation_key < last_attempt_key:
            continue
        impact = RuntimeExitPriceImpactV01.for_position(
            price_identity=execution.price_identity,
            filled_size_lamports=execution.entry_principal_lamports,
            entry_price_numerator_raw=execution.entry_price_numerator_raw,
            entry_price_denominator_raw=execution.entry_price_denominator_raw,
            current_virtual_token_reserve_raw=obs.current_virtual_token_reserve_raw,
        )
        if not impact.available:
            continue
        fill = cost_model.evaluate_fill(
            side=CostSide.EXIT,
            signal_reference_price=RationalPrice(
                execution.price_identity,
                execution.reference_price_numerator_raw,
                execution.reference_price_denominator_raw,
            ),
            market_price_at_ready=RationalPrice(
                execution.price_identity,
                obs.price_numerator_raw,
                obs.price_denominator_raw,
            ),
            price_impact_bps=int(impact.router_price_impact_bps),
            signal_observed_at=execution.requested_at,
            market_observed_at=obs.observed_at,
        )
        attempts += 1
        last_attempt_key = observation_key
        if fill.decision is FillDecision.REJECTED_SLIPPAGE:
            continue
        gross = (
            int(impact.derived_token_input_raw)
            * fill.simulated_execution_price.numerator_raw
        ) // fill.simulated_execution_price.denominator_raw
        costs = cost_model.explicit_costs(side=CostSide.EXIT, notional_lamports=gross)
        selected = {
            "fill_source_row": obs.ingest_seq,
            "fill_source_event_key": f"{obs.event_key}:MARKET",
            "fill_observed_at": obs.observed_at,
            "exit_price_numerator_raw": fill.simulated_execution_price.numerator_raw,
            "exit_price_denominator_raw": fill.simulated_execution_price.denominator_raw,
            "gross_exit_proceeds_lamports": gross,
            "exit_explicit_cost_lamports": costs.total_explicit_cost_lamports,
        }
        break

    if selected is None:
        return ReplayExecutionOutcomeV01(
            exit_state="UNRESOLVED",
            rejection_state="REJECTED_SLIPPAGE" if attempts else "NO_CAUSAL_FILL",
            attempt_count=attempts,
            fill_source_row=None,
            fill_source_event_key=None,
            fill_observed_at=None,
            exit_price_numerator_raw=None,
            exit_price_denominator_raw=None,
            gross_exit_proceeds_lamports=None,
            exit_explicit_cost_lamports=None,
            gross_execution_pnl_lamports=None,
            net_pnl_lamports=None,
            holding_time_us=None,
        )

    gross_exit = int(selected["gross_exit_proceeds_lamports"])
    exit_cost = int(selected["exit_explicit_cost_lamports"])
    fill_at = selected["fill_observed_at"]
    if not isinstance(fill_at, datetime):
        raise ReplayMismatch("selected fill has no datetime observation")
    return ReplayExecutionOutcomeV01(
        exit_state="FILLED",
        rejection_state="REJECTED_THEN_FILLED" if attempts > 1 else None,
        attempt_count=attempts,
        fill_source_row=int(selected["fill_source_row"]),
        fill_source_event_key=str(selected["fill_source_event_key"]),
        fill_observed_at=fill_at,
        exit_price_numerator_raw=int(selected["exit_price_numerator_raw"]),
        exit_price_denominator_raw=int(selected["exit_price_denominator_raw"]),
        gross_exit_proceeds_lamports=gross_exit,
        exit_explicit_cost_lamports=exit_cost,
        gross_execution_pnl_lamports=gross_exit - execution.entry_principal_lamports,
        net_pnl_lamports=(
            gross_exit
            - execution.entry_principal_lamports
            - execution.entry_explicit_cost_lamports
            - exit_cost
        ),
        holding_time_us=int((fill_at - execution.open_at).total_seconds() * 1_000_000),
    )


def replay_executions(
    paper_db: str | Path,
    replay: dict[str, Any],
    paths: dict[str, tuple[ContinuousMarketSourceRecordV02, ...]],
) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    with _ro(paper_db) as conn:
        actual_routes = {r["paper_position_id"]: dict(r) for r in conn.execute("SELECT * FROM paper_exit_execution_routes")}
        actual_attempts = Counter(r["paper_position_id"] for r in conn.execute("SELECT paper_position_id FROM paper_exit_execution_attempts"))
        entry_economics = {
            r["candidate_id"]: (int(r["requested_size_lamports"]), int(r["total_entry_explicit_cost_lamports"]))
            for r in conn.execute(
                "SELECT candidate_id,requested_size_lamports,total_entry_explicit_cost_lamports "
                "FROM paper_entry_routes WHERE state='FILLED'"
            )
        }

    output: list[dict[str, Any]] = []
    mismatches: list[str] = []
    for position_id, (ref, intent, track) in replay.items():
        source, ref_at, ref_seq, ref_num, ref_den = _reference(intent, track)
        principal, e_cost = entry_economics[ref.signal_key]
        outcome = execute_exit_intent_v0_1(
            ReplayExecutionInputV01(
                signal_key=ref.signal_key,
                mint=ref.mint,
                price_identity=ref.price_identity,
                entry_observed_at=ref.entry_market_observed_at,
                entry_ingest_seq=ref.entry_market_ingest_seq,
                open_at=ref.open_at,
                entry_principal_lamports=principal,
                entry_price_numerator_raw=ref.entry_price_numerator_raw,
                entry_price_denominator_raw=ref.entry_price_denominator_raw,
                entry_explicit_cost_lamports=e_cost,
                exit_reason=intent.reason.value,
                requested_at=intent.requested_at,
                reference_source=source,
                reference_observed_at=ref_at,
                reference_ingest_seq=ref_seq,
                reference_price_numerator_raw=ref_num,
                reference_price_denominator_raw=ref_den,
            ),
            paths.get(ref.mint, ()),
        )

        actual = actual_routes.get(position_id)
        if actual is None:
            mismatches.append(f"{position_id}:missing actual route")
            continue
        actual_filled = actual["state"] == "FILLED"
        if actual_filled != (outcome.exit_state == "FILLED"):
            mismatches.append(f"{position_id}:fill-state replay={outcome.exit_state} actual={actual['state']}")
        if outcome.attempt_count != actual_attempts[position_id]:
            mismatches.append(f"{position_id}:attempts replay={outcome.attempt_count} actual={actual_attempts[position_id]}")
        if outcome.exit_state == "FILLED":
            pairs = {
                "fill_source_row": (outcome.fill_source_row, actual["selected_market_ingest_seq"]),
                "gross": (outcome.gross_exit_proceeds_lamports, actual["gross_exit_proceeds_lamports"]),
                "exit_cost": (outcome.exit_explicit_cost_lamports, actual["total_exit_explicit_cost_lamports"]),
                "price_num": (outcome.exit_price_numerator_raw, int(actual["simulated_exit_price_numerator_raw"])),
                "price_den": (outcome.exit_price_denominator_raw, int(actual["simulated_exit_price_denominator_raw"])),
            }
            for label, pair in pairs.items():
                if pair[0] != pair[1]:
                    mismatches.append(f"{position_id}:{label}:{pair[0]}!={pair[1]}")

        row = {
            "track_id": ref.track_id, "exit_variant": ref.exit_variant,
            "signal_key": ref.signal_key, "mint": ref.mint, "paper_position_id": position_id,
            "replay_exit_state": outcome.exit_state,
            "exit_reason": intent.reason.value,
            "exit_requested_at": intent.requested_at.isoformat(timespec="microseconds"),
            "trigger_source_row": intent.trigger_ingest_seq,
            "fill_source_row": outcome.fill_source_row,
            "trigger_observed_at": None if intent.trigger_observed_at is None else intent.trigger_observed_at.isoformat(timespec="microseconds"),
            "fill_observed_at": None if outcome.fill_observed_at is None else outcome.fill_observed_at.isoformat(timespec="microseconds"),
            "execution_rejection_state": outcome.rejection_state,
            "execution_attempt_count": outcome.attempt_count,
            "entry_principal_lamports": principal,
            "entry_explicit_cost_lamports": e_cost,
            "gross_exit_proceeds_lamports": outcome.gross_exit_proceeds_lamports,
            "exit_explicit_cost_lamports": outcome.exit_explicit_cost_lamports,
            "gross_execution_pnl_lamports": outcome.gross_execution_pnl_lamports,
            "net_pnl_lamports": outcome.net_pnl_lamports,
            "holding_time_us": outcome.holding_time_us,
            "reference_source": source,
        }
        output.append(row)
    if mismatches:
        raise ReplayMismatch("execution replay differs from ACTUAL: " + "; ".join(mismatches[:30]))
    output.sort(key=lambda r: (TRACK_ORDER[r["track_id"]], r["signal_key"], r["paper_position_id"]))
    digest = canonical_sha256(output)
    summary = summarize(output)
    for track, expected in EXPECTED.items():
        for key, value in expected.items():
            if summary[track].get(key) != value:
                raise ReplayMismatch(f"baseline {track}.{key}: {summary[track].get(key)} != {value}")
    return output, summary, digest


def summarize(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["track_id"]].append(row)
    for track in TRACK_ORDER:
        values = grouped[track]
        closed = [r for r in values if r["replay_exit_state"] == "FILLED"]
        reasons = Counter(r["exit_reason"] for r in closed)
        out[track] = {
            "positions": len(values), "completed": len(closed), "pending": len(values) - len(closed),
            **dict(sorted(reasons.items())),
            "gross_execution_pnl_lamports": sum(r["gross_execution_pnl_lamports"] for r in closed),
            "entry_explicit_cost_lamports": sum(r["entry_explicit_cost_lamports"] for r in closed),
            "exit_explicit_cost_lamports": sum(r["exit_explicit_cost_lamports"] for r in closed),
            "net_pnl_lamports": sum(r["net_pnl_lamports"] for r in closed),
            "execution_attempts": sum(r["execution_attempt_count"] for r in values),
            "slippage_unresolved": sum(r["execution_rejection_state"] == "REJECTED_SLIPPAGE" for r in values),
            "no_causal_fill": sum(r["execution_rejection_state"] == "NO_CAUSAL_FILL" for r in values),
        }
    return out


def csv_text(rows: list[dict[str, Any]]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(rows[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def reproduce(paper_db: str | Path, source_db: str | Path) -> dict[str, Any]:
    if sha256_file(paper_db) != EXPECTED_PAPER_SHA256:
        raise ReplayMismatch("accepted paper DB SHA256 mismatch")
    entries, cohort = load_frozen_entries(paper_db, source_db)
    paths, source = load_frozen_market_paths(source_db, {entry.mint for entry in entries})
    replay, intent_gate, intent_digest = replay_intents(paper_db, paths)
    rows, summary, replay_digest = replay_executions(paper_db, replay, paths)
    # A second deterministic policy/execution pass reuses only immutable loaded paths.
    replay2, _, intent_digest2 = replay_intents(paper_db, paths)
    rows2, summary2, replay_digest2 = replay_executions(paper_db, replay2, paths)
    if (intent_digest, replay_digest, summary) != (intent_digest2, replay_digest2, summary2) or rows != rows2:
        raise ReplayMismatch("deterministic rerun mismatch")
    return {
        "model_id": MODEL_ID, "model_fingerprint": MODEL_FINGERPRINT,
        "run_id": RUN_ID, "cohort": cohort, "source": source,
        "intent_gate": intent_gate, "intent_digest": intent_digest,
        "replay_digest": replay_digest, "deterministic_rerun": "EXACT",
        "summary": summary, "rows": rows,
    }
