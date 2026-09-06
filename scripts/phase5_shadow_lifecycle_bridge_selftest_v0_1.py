from __future__ import annotations

import ast
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import phase5_shadow_unsigned_plan_simulation_selftest_v0_1 as fx  # noqa: E402
from phase5.shadow_continuous_source_bridge_v0_1 import (  # noqa: E402
    CONTRACT_SPEC as T004A_CONTRACT_SPEC,
    MODEL_FINGERPRINT as T004A_FINGERPRINT,
    open_continuous_source_bridge,
)
from phase5.shadow_domain_v0_1 import (  # noqa: E402
    MODEL_FINGERPRINT as T001_FINGERPRINT,
    IntentRole,
    IntentSide,
    ShadowDeterminismConflict,
    ShadowState,
)
from phase5.shadow_lifecycle_bridge_v0_1 import (  # noqa: E402
    CONTRACT_SPEC,
    EXPECTED_CLASSIFICATION,
    MODEL_FINGERPRINT,
    MODEL_ID,
    NO_POSITION_CLASSIFICATION,
    ExitPollStatus,
    ExitSourceConflict,
    InventoryConflict,
    deterministic_track_position_id,
    open_shadow_lifecycle_bridge,
)
from phase5.shadow_repository_v0_1 import open_shadow_repository  # noqa: E402
from phase5.shadow_simulation_repository_v0_1 import (  # noqa: E402
    open_simulation_repository,
    record_plan_state,
    record_simulation_state,
)
from phase5.shadow_unsigned_plan_simulation_v0_1 import (  # noqa: E402
    SimulationOutcome,
    run_bounded_simulation,
)
from phase5.shadow_venue_repository_v0_1 import open_venue_repository  # noqa: E402
from phase5.shadow_venue_route_quote_v0_1 import QuotePolicyV01  # noqa: E402


RUN_ID = "phase4-continuous-run-t004b"
BASE_US = fx.BASE_US
BASE_AT = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=BASE_US)
MINT = fx.fx.MINT
EXPECTED_T001 = "506312a81b6d8acc724cb27d6d450086bc3fc4986dcdea4831b998a3a8ec91e3"
EXPECTED_T004A = "83ca764101a325ef9c0cd8c4287e23d80f8e69b1ad5ee34f099ccce3ef470edb"
EXPECTED_MODEL_FINGERPRINT = "a14d1a7b2d959ebcc75583f830f8d444313b694f947a2bebdfe2a59b27b0e7f0"


def text_at(offset_us: int) -> str:
    return (BASE_AT + timedelta(microseconds=offset_us)).isoformat(timespec="microseconds")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def check(name: str, condition: bool, checks: dict[str, bool]) -> None:
    checks[name] = bool(condition)


def expect(error: type[BaseException], callback: Callable[[], Any]) -> bool:
    try:
        callback()
    except error:
        return True
    return False


def crash_once(target_phase: str) -> Callable[[str, Any], None]:
    fired = False

    def hook(phase: str, _source: Any) -> None:
        nonlocal fired
        if phase == target_phase and not fired:
            fired = True
            raise RuntimeError(f"injected crash: {phase}")

    return hook


def exit_row(
    exit_id: str,
    track: str,
    reason: str,
    requested_offset_us: int,
    *,
    triggered: bool,
) -> dict[str, Any]:
    variants = {
        "FINAL-A": "E3_TP10_T15",
        "FINAL-B": "E4_ACT10_GB03_T15",
        "SENS-C": "SENS_TP20_T5",
    }
    return {
        "exit_intent_id": exit_id,
        "paper_position_id": f"paper-position-{track.lower()}",
        "paper_order_id": f"paper-order-{track.lower()}",
        "track_id": track,
        "exit_variant": variants[track],
        "reason": reason,
        "requested_at": text_at(requested_offset_us),
        "trigger_observed_at": text_at(requested_offset_us - 1) if triggered else None,
        "trigger_ingest_seq": 900 + requested_offset_us if triggered else None,
        "trigger_source_event_key": f"trigger:{exit_id}" if triggered else None,
        "trigger_price_numerator_raw": "123" if triggered else None,
        "trigger_price_denominator_raw": "10" if triggered else None,
        "rule_return_bps": 1000 if triggered else None,
        "trail_peak_return_bps": 1300 if reason == "TRAIL" else None,
        "last_fresh_observed_at": text_at(requested_offset_us - 2),
        "last_fresh_ingest_seq": 800 + requested_offset_us,
        "last_fresh_return_bps": 900,
    }


def create_paper_database(
    path: Path,
    candidates: list[dict[str, Any]],
) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE paper_continuous_signal_contexts_v0_1 (
                signal_key TEXT PRIMARY KEY,
                route_id TEXT NOT NULL UNIQUE,
                source_evaluation_id TEXT NOT NULL,
                decision_key TEXT NOT NULL UNIQUE,
                run_id TEXT NOT NULL,
                role TEXT NOT NULL,
                mint TEXT NOT NULL,
                strategy_version TEXT NOT NULL,
                parameter_set_id TEXT NOT NULL,
                source_cursor INTEGER NOT NULL,
                source_event_key TEXT NOT NULL,
                content_fingerprint TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE paper_entry_routes (
                route_id TEXT PRIMARY KEY,
                candidate_id TEXT NOT NULL,
                mint TEXT NOT NULL,
                strategy_version TEXT NOT NULL,
                parameter_set_id TEXT NOT NULL,
                signal_observed_at TEXT NOT NULL,
                signal_ingest_seq INTEGER NOT NULL,
                requested_size_lamports INTEGER NOT NULL,
                state TEXT NOT NULL,
                state_reason TEXT NOT NULL
            );
            CREATE TABLE paper_exit_track_states (
                paper_position_id TEXT PRIMARY KEY,
                paper_order_id TEXT NOT NULL,
                signal_key TEXT NOT NULL,
                mint TEXT NOT NULL,
                track_id TEXT NOT NULL,
                exit_variant TEXT NOT NULL,
                signal_ingest_seq INTEGER NOT NULL,
                state TEXT NOT NULL,
                exit_intent_id TEXT
            );
            CREATE TABLE paper_exit_intents (
                exit_intent_id TEXT PRIMARY KEY,
                paper_position_id TEXT NOT NULL UNIQUE,
                paper_order_id TEXT NOT NULL,
                signal_key TEXT NOT NULL,
                mint TEXT NOT NULL,
                track_id TEXT NOT NULL,
                exit_variant TEXT NOT NULL,
                reason TEXT NOT NULL,
                requested_at TEXT NOT NULL,
                trigger_observed_at TEXT,
                trigger_ingest_seq INTEGER,
                trigger_source_event_key TEXT,
                trigger_price_numerator_raw TEXT,
                trigger_price_denominator_raw TEXT,
                rule_return_bps INTEGER,
                trail_peak_return_bps INTEGER,
                last_fresh_observed_at TEXT,
                last_fresh_ingest_seq INTEGER,
                last_fresh_return_bps INTEGER
            );
            """
        )
        for candidate in candidates:
            signal = candidate["signal_key"]
            cursor = candidate["source_cursor"]
            mint = candidate.get("mint", MINT)
            route_id = f"route-{signal}"
            conn.execute(
                "INSERT INTO paper_continuous_signal_contexts_v0_1 "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    signal,
                    route_id,
                    f"evaluation-{signal}",
                    f"decision-{signal}",
                    candidate.get("run_id", RUN_ID),
                    "CONTROL",
                    mint,
                    "FIRSTPULLBACK-v0.1",
                    "EXP-0012:FINAL",
                    cursor,
                    f"pump_events:{cursor}",
                    f"context-fingerprint-{signal}",
                    text_at(0),
                    text_at(0),
                ),
            )
            conn.execute(
                "INSERT INTO paper_entry_routes VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    route_id,
                    f"candidate-{signal}",
                    mint,
                    "FIRSTPULLBACK-v0.1",
                    "EXP-0012:FINAL",
                    text_at(0),
                    cursor,
                    100_000_000,
                    "FILLED",
                    "SIMULATED_ENTRY_FILLED",
                ),
            )
            for item in candidate.get("exits", []):
                conn.execute(
                    "INSERT INTO paper_exit_track_states VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        item["paper_position_id"],
                        item["paper_order_id"],
                        signal,
                        item.get("track_mint", mint),
                        item.get("track_id_value", item["track_id"]),
                        item["exit_variant"],
                        cursor,
                        "EXIT_INTENT",
                        item["exit_intent_id"],
                    ),
                )
                conn.execute(
                    "INSERT INTO paper_exit_intents VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        item["exit_intent_id"],
                        item["paper_position_id"],
                        item["paper_order_id"],
                        item.get("exit_signal_key", signal),
                        item.get("exit_mint", mint),
                        item["track_id"],
                        item["exit_variant"],
                        item["reason"],
                        item["requested_at"],
                        item["trigger_observed_at"],
                        item["trigger_ingest_seq"],
                        item["trigger_source_event_key"],
                        item["trigger_price_numerator_raw"],
                        item["trigger_price_denominator_raw"],
                        item["rule_return_bps"],
                        item["trail_peak_return_bps"],
                        item["last_fresh_observed_at"],
                        item["last_fresh_ingest_seq"],
                        item["last_fresh_return_bps"],
                    ),
                )
        conn.commit()
    finally:
        conn.close()


def bridge_entries(paper: Path, shadow: Path) -> tuple[str, ...]:
    with open_continuous_source_bridge(paper, shadow) as bridge:
        return bridge.poll_once().created_or_matched_intent_ids


def complete_entry_chain(shadow_path: Path, parent_id: str) -> tuple[Any, Any, Any]:
    shadow = open_shadow_repository(shadow_path)
    parent = shadow.get_intent(parent_id)
    assert parent is not None
    state, global_account = fx.make_pump_state(parent)
    route = fx.decide_route(parent, state, None)
    quote = fx.create_executable_quote(
        parent, state, route, QuotePolicyV01(100)
    )
    actor = fx.PublicShadowActorV01(fx.ACTOR)
    policy = fx.TransactionPlanPolicyV01()
    recipients = fx.decode_verified_recipient_evidence(state, global_account)
    snapshot = fx.build_actor_account_snapshot(
        state,
        actor,
        base_account=None,
        quote_account=None,
        context_slot=state.slot_max,
        observed_at_us=BASE_US + 1,
    )
    plan = fx.build_unsigned_transaction_plan(
        parent,
        state,
        route,
        quote,
        actor,
        policy,
        recipients,
        snapshot,
    )
    shadow.transition(
        parent.intent_id,
        ShadowState.ELIGIBILITY_CHECKED,
        idempotency_key="T004B_ELIGIBILITY",
        effective_at_us=BASE_US + 1,
        reason_code="OFFLINE_SYNTHETIC",
        evidence={},
    )
    venue = open_venue_repository(shadow_path)
    venue.persist_state(state)
    shadow.transition(
        parent.intent_id,
        ShadowState.ROUTE_BOUND,
        idempotency_key="T004B_ROUTE",
        effective_at_us=BASE_US + 2,
        reason_code="OFFLINE_ROUTE",
        evidence={},
    )
    venue.persist_route(route)
    shadow.transition(
        parent.intent_id,
        ShadowState.QUOTE_BOUND,
        idempotency_key="T004B_QUOTE",
        effective_at_us=BASE_US + 3,
        reason_code="OFFLINE_QUOTE",
        evidence={},
    )
    venue.persist_quote(quote)
    simulation = open_simulation_repository(shadow_path)
    record_plan_state(
        shadow, simulation, plan, effective_at_us=BASE_US + 4
    )
    run = run_bounded_simulation(
        fx.fake_rpc(plan, {"err": None, "logs": ["Program log: ok"]}),
        plan,
        observed_at_us=BASE_US + 10,
    )
    record_simulation_state(
        shadow, simulation, plan, run, effective_at_us=BASE_US + 20
    )
    result = run.final_result
    simulation.close()
    venue.close()
    shadow.close()
    return parent, quote, result


def transition_direct(shadow_path: Path, intent_id: str, target: ShadowState) -> None:
    shadow = open_shadow_repository(shadow_path)
    shadow.transition(
        intent_id,
        target,
        idempotency_key=f"T004B_DIRECT_{target.value}",
        effective_at_us=BASE_US + 1,
        reason_code="OFFLINE_SYNTHETIC_TERMINAL",
        evidence={},
    )
    shadow.close()


def transition_fake_completed(shadow_path: Path, intent_id: str) -> None:
    shadow = open_shadow_repository(shadow_path)
    for index, state in enumerate(
        (
            ShadowState.ELIGIBILITY_CHECKED,
            ShadowState.ROUTE_BOUND,
            ShadowState.QUOTE_BOUND,
            ShadowState.PLAN_BUILT,
            ShadowState.SIMULATED,
            ShadowState.COMPLETED,
        ),
        start=1,
    ):
        shadow.transition(
            intent_id,
            state,
            idempotency_key=f"T004B_FAKE_{state.value}",
            effective_at_us=BASE_US + index,
            reason_code="OFFLINE_SYNTHETIC_STATE",
            evidence={},
        )
    shadow.close()


def main() -> int:
    checks: dict[str, bool] = {}
    shadow_root = PROJECT_ROOT / "data" / "shadow"
    shadow_root.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="phase5_t004b_", dir=shadow_root))
    protected = [
        PROJECT_ROOT / "src" / "phase5" / "shadow_domain_v0_1.py",
        PROJECT_ROOT / "src" / "phase5" / "shadow_repository_v0_1.py",
        PROJECT_ROOT / "src" / "phase5" / "shadow_continuous_source_bridge_v0_1.py",
        PROJECT_ROOT / "src" / "phase5" / "shadow_venue_route_quote_v0_1.py",
        PROJECT_ROOT / "src" / "phase5" / "shadow_unsigned_plan_simulation_v0_1.py",
    ]
    protected_before = {str(path): sha256(path) for path in protected}
    try:
        # A. Successful expected inventory and three independent EXIT lanes.
        same_requested = 1_000
        exits = [
            exit_row("exit-c", "SENS-C", "FALLBACK", same_requested, triggered=False),
            exit_row("exit-a", "FINAL-A", "TAKE_PROFIT", same_requested, triggered=True),
            exit_row("exit-b", "FINAL-B", "TRAIL", same_requested, triggered=True),
        ]
        paper_a = root / "paper_a.sqlite3"
        shadow_a = root / "shadow_a.sqlite3"
        create_paper_database(
            paper_a,
            [{"signal_key": "signal-a", "source_cursor": 100, "exits": exits}],
        )
        parent_id = bridge_entries(paper_a, shadow_a)[0]
        parent, quote, simulation_result = complete_entry_chain(shadow_a, parent_id)
        check(
            "A00_REAL_T004A_ENTRY_T002_QUOTE_COMPATIBLE",
            parent.role is IntentRole.ENTRY
            and parent.side is IntentSide.BUY
            and parent.input_asset == "SOL_LAMPORTS"
            and quote.intent_id == parent.intent_id
            and quote.intent_fingerprint == parent.fingerprint,
            checks,
        )
        paper_hash_before = sha256(paper_a)
        with open_shadow_lifecycle_bridge(paper_a, shadow_a) as bridge:
            inventory = bridge.materialize_expected_inventory(
                parent,
                quote,
                simulation_result,
                evidence_at_us=BASE_US + 30,
            )
            assert inventory is not None
            check("A01_SUCCESS_ENTRY_ONE_EXPECTED_INVENTORY", inventory.classification == EXPECTED_CLASSIFICATION and inventory.expected_base_amount == quote.expected_base_amount and bridge.get_inventory(parent.intent_id) == inventory and bridge._conn.execute("SELECT COUNT(*) FROM shadow_t004b_expected_inventory").fetchone()[0] == 1, checks)
            replayed = bridge.materialize_expected_inventory(
                parent,
                quote,
                simulation_result,
                evidence_at_us=BASE_US + 30,
            )
            check("A02_EXACT_INVENTORY_REPLAY_IDEMPOTENT", replayed == inventory and bridge._conn.execute("SELECT COUNT(*) FROM shadow_t004b_expected_inventory").fetchone()[0] == 1, checks)
            check("A03_CONFLICTING_INVENTORY_REPLAY_FAILS_CLOSED", expect(InventoryConflict, lambda: bridge.materialize_expected_inventory(parent, quote, simulation_result, evidence_at_us=BASE_US + 31)), checks)
            ordered = [bridge.poll_exit_once(limit=1) for _ in range(3)]
            check("A04_EQUAL_REQUESTED_AT_ORDERED_BY_PHYSICAL_APPEND", [item.last_exit_intent_id for item in ordered] == ["exit-c", "exit-a", "exit-b"] and [item.last_source_rowid for item in ordered] == [1, 2, 3], checks)
            intent_rows = bridge._conn.execute(
                "SELECT intent_json FROM shadow_execution_intents ORDER BY intent_id"
            ).fetchall()
            intents = [json.loads(str(row[0])) for row in intent_rows]
            buys = [item for item in intents if item["role"] == "ENTRY"]
            sells = [item for item in intents if item["role"] == "EXIT"]
            check("A05_ONE_SHARED_BUY_THREE_EXIT_INTENTS", len(buys) == 1 and len(sells) == 3, checks)
            exit_quotes = []
            for item in sells:
                exit_intent = bridge._shadow.get_intent(item["intent_id"])
                assert exit_intent is not None
                exit_state, _ = fx.make_pump_state(exit_intent)
                exit_route = fx.decide_route(exit_intent, exit_state, None)
                exit_quotes.append(
                    fx.create_executable_quote(
                        exit_intent,
                        exit_state,
                        exit_route,
                        QuotePolicyV01(100),
                    )
                )
            check(
                "A05B_REAL_T004B_EXITS_T002_QUOTE_COMPATIBLE",
                len(exit_quotes) == 3
                and all(item["input_asset"] == "MEME_BASE_UNITS" for item in sells)
                and all(quote.intent_id == item["intent_id"] for quote, item in zip(exit_quotes, sells, strict=True)),
                checks,
            )
            check("A06_ALL_EXITS_SHARE_EXACT_PARENT", {item["parent_entry_intent_id"] for item in sells} == {parent.intent_id}, checks)
            check("A07_EACH_EXIT_USES_FULL_EXPECTED_AMOUNT", {item["input_amount_base_units"] for item in sells} == {inventory.expected_base_amount}, checks)
            states = {
                bridge._shadow.current_state(item["intent_id"]).value for item in sells
            }
            check("A08_PAPER_REASONS_DO_NOT_SET_SHADOW_OUTCOME", states == {"CREATED"}, checks)
            by_decision = {item["exit_decision_id"]: item for item in sells}
            check("A09_FALLBACK_NULL_TRIGGER_SOURCE_KEY", by_decision["exit-c"]["source_event_key"] == "PAPER_EXIT_INTENT:exit-c" and by_decision["exit-c"]["source_ingest_seq"] == 100, checks)
            check("A10_TRIGGER_SOURCE_LINEAGE_PRESERVED", by_decision["exit-a"]["source_event_key"] == "trigger:exit-a" and by_decision["exit-a"]["source_ingest_seq"] == 1900, checks)
            check("A11_POSITION_IDS_TRACK_DISTINCT_DETERMINISTIC", len({item["position_id"] for item in sells}) == 3 and all(item["position_id"] == deterministic_track_position_id(parent.intent_id, item["exit_track_id"]) for item in sells), checks)
            check("A12_EXIT_LIFECYCLE_PRESERVES_PAPER_POSITION", all(item["exit_lifecycle_id"] == f"paper-position-{item['exit_track_id'].lower()}" for item in sells), checks)
            evidence = [
                json.loads(str(row[0]))
                for row in bridge._conn.execute(
                    "SELECT evidence_json FROM shadow_t004b_exit_source_evidence"
                )
            ]
            check("A13_EXIT_VARIANT_REASON_ORDER_EVIDENCE_EXACT", {item["phase4_exit"]["reason"] for item in evidence} == {"TAKE_PROFIT", "TRAIL", "FALLBACK"} and all(item["paper_outcome_is_shadow_outcome"] is False for item in evidence), checks)
            check("A14_REPEATED_EXIT_POLL_NO_DUPLICATE", bridge.poll_exit_once().processed_rows == 0 and bridge._conn.execute("SELECT COUNT(*) FROM shadow_t004b_exit_source_evidence").fetchone()[0] == 3, checks)
            digest = bridge.canonical_digest()
            check("A15_SHADOW_QUICK_CHECK_OK", bridge.quick_check() == "ok", checks)
        check("A16_PHASE4_DB_BYTE_FOR_BYTE_UNCHANGED", sha256(paper_a) == paper_hash_before, checks)
        with open_shadow_lifecycle_bridge(paper_a, shadow_a) as reopened:
            check("A17_RESTART_REOPEN_NO_DUPLICATE", reopened.poll_exit_once().processed_rows == 0 and reopened._conn.execute("SELECT COUNT(*) FROM shadow_execution_intents WHERE role='EXIT'").fetchone()[0] == 3, checks)
            check("A18_RESTART_DIGEST_DETERMINISTIC", reopened.canonical_digest() == digest, checks)

        # B. Negative terminal entries never produce expected inventory.
        for index, terminal in enumerate(
            (ShadowState.REJECTED, ShadowState.EXPIRED, ShadowState.FAILED)
        ):
            paper = root / f"negative_{terminal.value}.paper.sqlite3"
            shadow = root / f"negative_{terminal.value}.shadow.sqlite3"
            create_paper_database(
                paper,
                [{"signal_key": f"negative-{index}", "source_cursor": 200 + index, "exits": []}],
            )
            parent_id = bridge_entries(paper, shadow)[0]
            transition_direct(shadow, parent_id, terminal)
            repo = open_shadow_repository(shadow)
            parent = repo.get_intent(parent_id)
            repo.close()
            assert parent is not None
            with open_shadow_lifecycle_bridge(paper, shadow) as bridge:
                check(f"B0{index + 1}_{terminal.value}_NO_INVENTORY", bridge.materialize_expected_inventory(parent, None, None, evidence_at_us=BASE_US + 2) is None and bridge._conn.execute("SELECT COUNT(*) FROM shadow_t004b_expected_inventory").fetchone()[0] == 0, checks)

        # C. Nonterminal waits; failed terminal records immutable no-position evidence.
        waiting_exit = exit_row("exit-wait", "FINAL-A", "FALLBACK", 2_000, triggered=False)
        paper_c1 = root / "waiting.paper.sqlite3"
        shadow_c1 = root / "waiting.shadow.sqlite3"
        create_paper_database(paper_c1, [{"signal_key": "waiting", "source_cursor": 300, "exits": [waiting_exit]}])
        bridge_entries(paper_c1, shadow_c1)
        with open_shadow_lifecycle_bridge(paper_c1, shadow_c1) as bridge:
            waiting = bridge.poll_exit_once()
            check("C01_NONTERMINAL_WAIT_CURSOR_UNCHANGED", waiting.status is ExitPollStatus.WAITING_FOR_ENTRY_TERMINAL and bridge.cursor() == (0, "", "", None, 0) and bridge._conn.execute("SELECT COUNT(*) FROM shadow_execution_intents WHERE role='EXIT'").fetchone()[0] == 0, checks)

        failed_exit = exit_row("exit-failed", "FINAL-B", "TRAIL", 2_100, triggered=True)
        paper_c2 = root / "failed.paper.sqlite3"
        shadow_c2 = root / "failed.shadow.sqlite3"
        create_paper_database(paper_c2, [{"signal_key": "failed", "source_cursor": 301, "exits": [failed_exit]}])
        failed_parent = bridge_entries(paper_c2, shadow_c2)[0]
        transition_direct(shadow_c2, failed_parent, ShadowState.FAILED)
        with open_shadow_lifecycle_bridge(paper_c2, shadow_c2) as bridge:
            result = bridge.poll_exit_once()
            no_position = bridge._conn.execute(
                "SELECT classification,terminal_parent_state,shadow_exit_intent_id "
                "FROM shadow_t004b_exit_source_evidence"
            ).fetchone()
            check("C02_FAILED_PARENT_NO_POSITION_SAFE_ADVANCE", result.processed_rows == 1 and result.no_position_rows == 1 and tuple(no_position) == (NO_POSITION_CLASSIFICATION, "FAILED", None) and bridge.cursor()[:3] == (1, failed_exit["requested_at"], "exit-failed"), checks)
            check("C03_NO_POSITION_CREATES_NO_SELL", bridge._conn.execute("SELECT COUNT(*) FROM shadow_execution_intents WHERE role='EXIT'").fetchone()[0] == 0, checks)

        no_position_crash_exit = exit_row(
            "exit-failed-crash", "FINAL-A", "FALLBACK", 2_150, triggered=False
        )
        paper_c3 = root / "failed_crash.paper.sqlite3"
        shadow_c3 = root / "failed_crash.shadow.sqlite3"
        create_paper_database(
            paper_c3,
            [
                {
                    "signal_key": "failed-crash",
                    "source_cursor": 302,
                    "exits": [no_position_crash_exit],
                }
            ],
        )
        failed_crash_parent = bridge_entries(paper_c3, shadow_c3)[0]
        transition_direct(shadow_c3, failed_crash_parent, ShadowState.FAILED)
        interrupted = open_shadow_lifecycle_bridge(
            paper_c3,
            shadow_c3,
            fault_hook=crash_once("AFTER_NO_POSITION_EVIDENCE_PERSISTENCE"),
        )
        crashed = expect(RuntimeError, interrupted.poll_exit_once)
        cursor_after_crash = interrupted.cursor()
        evidence_after_crash = interrupted._conn.execute(
            "SELECT COUNT(*) FROM shadow_t004b_exit_source_evidence"
        ).fetchone()[0]
        interrupted.close()
        with open_shadow_lifecycle_bridge(paper_c3, shadow_c3) as recovered:
            recovered_result = recovered.poll_exit_once()
            check(
                "C04_NO_POSITION_EVIDENCE_CRASH_SAFE_REPLAY",
                crashed
                and cursor_after_crash == (0, "", "", None, 0)
                and evidence_after_crash == 1
                and recovered_result.processed_rows == 1
                and recovered_result.no_position_rows == 1
                and recovered._conn.execute(
                    "SELECT COUNT(*) FROM shadow_execution_intents WHERE role='EXIT'"
                ).fetchone()[0]
                == 0,
                checks,
            )

        # D. COMPLETED without accepted inventory fails closed.
        missing_exit = exit_row("exit-missing", "SENS-C", "FALLBACK", 2_200, triggered=False)
        paper_d = root / "missing.paper.sqlite3"
        shadow_d = root / "missing.shadow.sqlite3"
        create_paper_database(paper_d, [{"signal_key": "missing", "source_cursor": 400, "exits": [missing_exit]}])
        missing_parent = bridge_entries(paper_d, shadow_d)[0]
        transition_fake_completed(shadow_d, missing_parent)
        with open_shadow_lifecycle_bridge(paper_d, shadow_d) as bridge:
            check("D01_COMPLETED_WITHOUT_INVENTORY_FAILS_CLOSED", expect(InventoryConflict, bridge.poll_exit_once) and bridge.cursor() == (0, "", "", None, 0), checks)

        # E. Join conflicts and failed-row blocking.
        for index, field in enumerate(("mint", "track", "signal")):
            bad = exit_row(f"exit-joined-{field}", "FINAL-A", "FALLBACK", 3_100 + index, triggered=False)
            paper = root / f"joined_{field}.paper.sqlite3"
            shadow = root / f"joined_{field}.shadow.sqlite3"
            create_paper_database(paper, [{"signal_key": f"joined-{field}", "source_cursor": 510 + index, "exits": [bad]}])
            bridge_entries(paper, shadow)
            writer = sqlite3.connect(paper)
            if field == "mint":
                writer.execute("UPDATE paper_exit_track_states SET mint='different-mint'")
                label = "E01_MINT_CONFLICT_FAILS_CLOSED"
            elif field == "track":
                writer.execute("UPDATE paper_exit_track_states SET track_id='FINAL-B'")
                label = "E02_TRACK_CONFLICT_FAILS_CLOSED"
            else:
                writer.execute("UPDATE paper_exit_track_states SET signal_key='different-signal'")
                label = "E03_SIGNAL_CONFLICT_FAILS_CLOSED"
            writer.commit()
            writer.close()
            with open_shadow_lifecycle_bridge(paper, shadow) as bridge:
                check(label, expect(ExitSourceConflict, bridge.poll_exit_once) and bridge.cursor() == (0, "", "", None, 0), checks)

        first_bad = exit_row("exit-00-bad", "FINAL-A", "FALLBACK", 4_000, triggered=False)
        second_good = exit_row("exit-01-good", "FINAL-B", "TRAIL", 4_001, triggered=True)
        paper_e = root / "failed_row.paper.sqlite3"
        shadow_e = root / "failed_row.shadow.sqlite3"
        create_paper_database(paper_e, [{"signal_key": "failed-row", "source_cursor": 600, "exits": [first_bad, second_good]}])
        bridge_entries(paper_e, shadow_e)
        writer = sqlite3.connect(paper_e)
        writer.execute("UPDATE paper_exit_track_states SET mint='bad' WHERE exit_intent_id='exit-00-bad'")
        writer.commit()
        writer.close()
        with open_shadow_lifecycle_bridge(paper_e, shadow_e) as bridge:
            check("E04_FAILED_ROW_CANNOT_BE_SKIPPED", expect(ExitSourceConflict, bridge.poll_exit_once) and bridge.cursor() == (0, "", "", None, 0) and bridge._conn.execute("SELECT COUNT(*) FROM shadow_t004b_exit_source_evidence").fetchone()[0] == 0, checks)

        # F. Both EXIT durability boundaries recover without duplication.
        for index, phase in enumerate(
            ("AFTER_EXIT_INTENT_REGISTRATION", "AFTER_EXIT_EVIDENCE_PERSISTENCE")
        ):
            item = exit_row(
                f"exit-crash-{index}",
                "FINAL-A",
                "TAKE_PROFIT",
                5_000 + index,
                triggered=True,
            )
            paper = root / f"crash_{index}.paper.sqlite3"
            shadow = root / f"crash_{index}.shadow.sqlite3"
            create_paper_database(
                paper,
                [
                    {
                        "signal_key": f"crash-{index}",
                        "source_cursor": 700 + index,
                        "exits": [item],
                    }
                ],
            )
            parent_id = bridge_entries(paper, shadow)[0]
            parent, quote, simulation_result = complete_entry_chain(shadow, parent_id)
            interrupted = open_shadow_lifecycle_bridge(
                paper,
                shadow,
                fault_hook=crash_once(phase),
            )
            interrupted.materialize_expected_inventory(
                parent,
                quote,
                simulation_result,
                evidence_at_us=BASE_US + 30,
            )
            crashed = expect(RuntimeError, interrupted.poll_exit_once)
            cursor_after_crash = interrupted.cursor()
            exits_after_crash = interrupted._conn.execute(
                "SELECT COUNT(*) FROM shadow_execution_intents WHERE role='EXIT'"
            ).fetchone()[0]
            evidence_after_crash = interrupted._conn.execute(
                "SELECT COUNT(*) FROM shadow_t004b_exit_source_evidence"
            ).fetchone()[0]
            interrupted.close()
            with open_shadow_lifecycle_bridge(paper, shadow) as recovered:
                recovered_result = recovered.poll_exit_once()
                check(
                    f"F0{index + 1}_{phase}_SAFE_REPLAY",
                    crashed
                    and cursor_after_crash == (0, "", "", None, 0)
                    and exits_after_crash == 1
                    and evidence_after_crash == index
                    and recovered_result.processed_rows == 1
                    and recovered._conn.execute(
                        "SELECT COUNT(*) FROM shadow_execution_intents "
                        "WHERE role='EXIT'"
                    ).fetchone()[0]
                    == 1
                    and recovered._conn.execute(
                        "SELECT COUNT(*) FROM shadow_t004b_exit_source_evidence"
                    ).fetchone()[0]
                    == 1,
                    checks,
                )

        # G. Structural capability boundary and accepted modules remain exact.
        production = PROJECT_ROOT / "src" / "phase5" / "shadow_lifecycle_bridge_v0_1.py"
        tree = ast.parse(production.read_text(encoding="utf-8"))
        imports = {
            alias.name.split(".")[0].lower()
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            str(node.module).split(".")[0].lower()
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module
        }
        raw_text = production.read_text(encoding="utf-8").lower()
        flattened_text = raw_text.replace("_", "")
        prohibited = ("keypair", "priv" + "atekey", "sendtransaction", "sendrawtransaction", "broadcast")
        check("G01_NO_NETWORK_SIGNER_BROADCAST_CAPABILITY", not ({"requests", "httpx", "websockets", "solana"} & imports) and not any(item in flattened_text for item in prohibited), checks)
        exit_sql = raw_text.split("_exit_sql =", 1)[1].split('"""', 2)[1]
        check("G02_PHASE4_SQL_READ_ONLY", all(token not in exit_sql for token in ("insert ", "update ", "delete ", "create ", "drop ", "alter ")), checks)
        check("G03_ACCEPTED_FINGERPRINTS_EXACT", T001_FINGERPRINT == EXPECTED_T001 and T004A_FINGERPRINT == EXPECTED_T004A and MODEL_FINGERPRINT == EXPECTED_MODEL_FINGERPRINT and T004A_CONTRACT_SPEC.get("entry_input_asset_unit") == "SOL_LAMPORTS" and CONTRACT_SPEC.get("exit_input_asset_unit") == "MEME_BASE_UNITS", checks)
        nonexistent_shadow = root / "must_not_be_created.sqlite3"
        check("G04_NO_SECOND_SHADOW_DATABASE_CREATED", expect(Exception, lambda: open_shadow_lifecycle_bridge(paper_a, nonexistent_shadow)) and not nonexistent_shadow.exists(), checks)
        protected_after = {str(path): sha256(path) for path in protected}
        check("G05_ACCEPTED_MODULES_UNCHANGED", protected_after == protected_before, checks)
        test_tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
        check(
            "G06_NO_RECONSTRUCTED_INTENT_WORKAROUND",
            not any(
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "replace"
                for node in ast.walk(test_tree)
            )
            and not any(
                isinstance(node, ast.ImportFrom)
                and node.module == "dataclasses"
                and any(alias.name == "replace" for alias in node.names)
                for node in ast.walk(test_tree)
            ),
            checks,
        )
    finally:
        shutil.rmtree(root)

    passed = sum(checks.values())
    print("=" * 104)
    print("PHASE-5 EXPECTED SHADOW INVENTORY + EXIT LIFECYCLE BRIDGE v0.1 SELF-TEST")
    print("=" * 104)
    for name, ok in checks.items():
        print(f"{name}: {'PASS' if ok else 'FAIL'}")
    print(f"MODEL_ID: {MODEL_ID}")
    print(f"MODEL_FINGERPRINT: {MODEL_FINGERPRINT}")
    print(f"CHECKS: {passed}/{len(checks)}")
    print(f"RESULT: {'PASS' if passed == len(checks) else 'FAIL'}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
