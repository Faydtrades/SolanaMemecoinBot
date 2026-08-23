from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import shutil
import sqlite3
import sys
import types
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PHASE4_ROOT = PROJECT_ROOT / "src" / "phase4"
SMOKE_DIR = PROJECT_ROOT / "data" / "paper" / "live_smoke"
SELFTEST_DIR = PROJECT_ROOT / "data" / "selftest"

NS4 = "tradingbot_local_phase4_b1d3"

LOCKED_TRACKS = ("FINAL-A", "FINAL-B", "SENS-C")
LOCKED_BASELINE_ID = "P4-COST-BASELINE-0001"
LOCKED_BASELINE_FINGERPRINT = (
    "9652aeffc792eb48d62348360c6ae804f660e43329195d1f169219b80ccab67c"
)
LOCKED_IMPACT_ID = "P4-RUNTIME-PRICE-IMPACT-0001"
LOCKED_IMPACT_FINGERPRINT = (
    "30d8880aa58d56f70f3baf235f4973a80c94b59724a5ee286b67f60be924d9e1"
)


def bind_namespace(name: str, path: Path) -> None:
    if name in sys.modules:
        return
    pkg = types.ModuleType(name)
    pkg.__path__ = [str(path)]
    pkg.__package__ = name
    sys.modules[name] = pkg


bind_namespace(NS4, PHASE4_ROOT)


def local_import(module_name: str):
    mod = importlib.import_module(f"{NS4}.{module_name}")
    loaded = Path(getattr(mod, "__file__", "")).resolve()
    expected = (PHASE4_ROOT / f"{module_name}.py").resolve()
    if loaded != expected:
        raise RuntimeError(
            f"Binding mismatch {module_name}: loaded={loaded} expected={expected}"
        )
    return mod


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def enum_value(value: Any) -> str:
    return str(getattr(value, "value", value))


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def latest_pair() -> tuple[Path, Path]:
    dbs = sorted(
        SMOKE_DIR.glob("phase4_3b1d2_causal_sol_entry_*.sqlite3"),
        key=lambda p: p.stat().st_mtime_ns,
        reverse=True,
    )
    if not dbs:
        raise FileNotFoundError("No B1D2 paper DB found")

    db = dbs[0]
    stamp = db.stem.removeprefix("phase4_3b1d2_causal_sol_entry_")
    audit = SMOKE_DIR / f"phase4_3b1d2_causal_sol_entry_{stamp}.json"
    if not audit.exists():
        raise FileNotFoundError(f"Missing matching B1D2 audit: {audit}")
    return db, audit


def resolve_locked_baseline(cost_baseline_mod, PaperCostBaseline):
    candidates: list[tuple[str, Any]] = []

    for name, value in vars(cost_baseline_mod).items():
        if isinstance(value, PaperCostBaseline):
            candidates.append((f"object:{name}", value))

    for name, value in vars(cost_baseline_mod).items():
        if not callable(value) or "baseline" not in name.lower():
            continue
        try:
            sig = inspect.signature(value)
        except Exception:
            continue

        required = [
            p
            for p in sig.parameters.values()
            if p.default is inspect.Parameter.empty
            and p.kind
            in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            )
        ]
        if required:
            continue

        try:
            result = value()
        except Exception:
            continue

        if isinstance(result, PaperCostBaseline):
            candidates.append((f"factory:{name}", result))

    matches = [
        (source, baseline)
        for source, baseline in candidates
        if getattr(baseline, "assumption_set_id", None) == LOCKED_BASELINE_ID
        and getattr(baseline, "fingerprint", None) == LOCKED_BASELINE_FINGERPRINT
    ]

    if not matches:
        raise RuntimeError("Could not resolve exact locked Phase-4 cost baseline")

    return matches[0]


def create_router(replay_db: Path, router_mod, cost_model_mod, baseline):
    conn = sqlite3.connect(replay_db, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    cost_model = cost_model_mod.PaperCostModelV02(baseline)
    router = router_mod.PaperEntryRouterV01(conn, cost_model)
    return conn, router


def state_snapshot(router, locked_tracks: tuple[str, ...]) -> dict[str, Any]:
    conn = router.conn

    routes = conn.execute(
        """
        SELECT candidate_id, mint, parameter_set_id, state,
               execution_ready_at, selected_market_ingest_seq,
               selected_market_observed_at, selected_source_event_key
        FROM paper_entry_routes
        ORDER BY candidate_id
        """
    ).fetchall()

    orders = conn.execute(
        """
        SELECT paper_order_id, candidate_id, track_id, state, mint,
               requested_size_lamports, filled_size_lamports,
               simulated_entry_price_numerator_raw,
               simulated_entry_price_denominator_raw
        FROM paper_orders
        ORDER BY track_id, paper_order_id
        """
    ).fetchall()

    positions = router.lifecycle.list_open_positions()

    return {
        "route_count": len(routes),
        "route_states": tuple(str(r["state"]) for r in routes),
        "order_count": len(orders),
        "order_states": tuple(str(r["state"]) for r in orders),
        "order_tracks": tuple(str(r["track_id"]) for r in orders),
        "position_count": len(positions),
        "position_states": tuple(enum_value(p.state) for p in positions),
        "position_tracks_raw": tuple(str(p.track_id) for p in positions),
        "position_tracks_set": frozenset(str(p.track_id) for p in positions),
        "locked_tracks_set": frozenset(locked_tracks),
        "order_ids": tuple(str(r["paper_order_id"]) for r in orders),
        "position_order_ids": tuple(str(p.paper_order_id) for p in positions),
    }


def main() -> int:
    print("=" * 120)
    print("PHASE 4.3B1D3 - FILLED ENTRY RESTART / REPLAY PERSISTENCE v0.1")
    print("=" * 120)

    original_db, audit_path = latest_pair()
    audit = json.loads(audit_path.read_text(encoding="utf-8"))

    router_mod = local_import("paper_entry_router_v0_1")
    cost_model_mod = local_import("paper_cost_model_v0_2")
    cost_baseline_mod = local_import("paper_cost_baseline_v0_1")
    impact_mod = local_import("runtime_price_impact_v0_1")

    if impact_mod.MODEL_ID != LOCKED_IMPACT_ID:
        raise RuntimeError(f"Impact model ID drift: {impact_mod.MODEL_ID}")
    if impact_mod.MODEL_FINGERPRINT != LOCKED_IMPACT_FINGERPRINT:
        raise RuntimeError("Impact model fingerprint drift")

    baseline_source, baseline = resolve_locked_baseline(
        cost_baseline_mod,
        cost_model_mod.PaperCostBaseline,
    )

    locked_tracks = tuple(router_mod.LOCKED_PHASE4_TRACKS)
    if locked_tracks != LOCKED_TRACKS:
        raise RuntimeError(f"Locked track drift: {locked_tracks}")

    candidate_payload = audit.get("candidate_envelope")
    observation_payload = audit.get("entry_market_observation")
    impact_payload = audit.get("runtime_price_impact")

    if not candidate_payload or not observation_payload or not impact_payload:
        raise RuntimeError(
            "B1D2 audit is missing candidate/observation/impact replay payload"
        )

    CandidateSignalEnvelope = router_mod.CandidateSignalEnvelope
    EntryMarketObservation = router_mod.EntryMarketObservation

    candidate = CandidateSignalEnvelope(
        candidate_id=str(candidate_payload["candidate_id"]),
        mint=str(candidate_payload["mint"]),
        strategy_version=str(candidate_payload["strategy_version"]),
        parameter_set_id=str(candidate_payload["parameter_set_id"]),
        signal_observed_at=parse_dt(candidate_payload["signal_observed_at"]),
        signal_ingest_seq=int(candidate_payload["signal_ingest_seq"]),
        price_identity=str(candidate_payload["price_identity"]),
        price_numerator_raw=int(candidate_payload["price_numerator_raw"]),
        price_denominator_raw=int(candidate_payload["price_denominator_raw"]),
    )

    observation = EntryMarketObservation(
        mint=str(observation_payload["mint"]),
        observed_at=parse_dt(observation_payload["observed_at"]),
        ingest_seq=int(observation_payload["ingest_seq"]),
        price_identity=str(observation_payload["price_identity"]),
        price_numerator_raw=int(observation_payload["price_numerator_raw"]),
        price_denominator_raw=int(observation_payload["price_denominator_raw"]),
        is_gap_recovery=bool(observation_payload["is_gap_recovery"]),
        source_event_key=str(observation_payload["source_event_key"]),
    )

    impact_bps = int(impact_payload["router_price_impact_bps"])

    SELFTEST_DIR.mkdir(parents=True, exist_ok=True)
    replay_db = SELFTEST_DIR / (
        original_db.stem + "_b1d3_restart_replay_v0_1.sqlite3"
    )
    if replay_db.exists():
        replay_db.unlink()

    original_hash_before = sha256_file(original_db)
    shutil.copy2(original_db, replay_db)
    copy_hash_initial = sha256_file(replay_db)

    if original_hash_before != copy_hash_initial:
        raise RuntimeError("Replay DB copy is not byte-identical to original")

    print(f"Project root                         : {PROJECT_ROOT}")
    print(f"Original B1D2 paper DB               : {original_db}")
    print(f"Original B1D2 audit                  : {audit_path}")
    print(f"Isolated replay DB                   : {replay_db}")
    print("Original DB opened                   : NO")
    print("Original DB writes                   : NO")
    print("Collector / network / RPC            : NO")
    print("Wallet / signing / live orders       : NO")
    print("Parameter tuning / reselection       : NO")
    print(f"Locked cost baseline                 : {baseline.assumption_set_id}")
    print(f"Cost baseline fingerprint            : {baseline.fingerprint}")
    print(f"Cost baseline source                 : {baseline_source}")
    print(f"Runtime impact model                 : {impact_mod.MODEL_ID}")
    print(f"Runtime impact fingerprint           : {impact_mod.MODEL_FINGERPRINT}")
    print(f"Locked tracks                        : {locked_tracks}")
    print(f"Replay candidate                     : {candidate.candidate_id}")
    print(f"Replay impact                        : {impact_bps} bps")
    print(f"Original SHA256 before               : {original_hash_before}")
    print()

    # ------------------------------------------------------------------
    # Restart #1: reconstruct router/lifecycle from persisted FILLED DB.
    # ------------------------------------------------------------------
    conn1, router1 = create_router(
        replay_db,
        router_mod,
        cost_model_mod,
        baseline,
    )
    try:
        digest_restart1 = router_mod.canonical_entry_router_digest(conn1)
        snap1 = state_snapshot(router1, locked_tracks)
        quick1 = conn1.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        conn1.close()

    # ------------------------------------------------------------------
    # Restart #2: reopen again and verify exactly the same logical state.
    # Then exact-replay CandidateSignal and fill observation.
    # ------------------------------------------------------------------
    conn2, router2 = create_router(
        replay_db,
        router_mod,
        cost_model_mod,
        baseline,
    )
    try:
        digest_before_replay = router_mod.canonical_entry_router_digest(conn2)
        snap2_before = state_snapshot(router2, locked_tracks)

        route_replay = router2.route_candidate(candidate)
        digest_after_route_replay = router_mod.canonical_entry_router_digest(conn2)

        fill_replay = router2.on_market_observation(
            candidate=candidate,
            observation=observation,
            price_impact_bps=impact_bps,
            observed_priority_fee_lamports=None,
            venue_fee_bps=None,
        )
        digest_after_fill_replay = router_mod.canonical_entry_router_digest(conn2)
        snap2_after = state_snapshot(router2, locked_tracks)
        quick2 = conn2.execute("PRAGMA quick_check").fetchone()[0]

        replay_route_state = enum_value(route_replay.route.state)
        replay_fill_route_state = enum_value(fill_replay.route.state)
        replay_order_ids = tuple(
            str(o.paper_order_id) for o in fill_replay.orders
        )
    finally:
        conn2.close()

    # ------------------------------------------------------------------
    # Restart #3: prove replay did not change persisted logical state.
    # ------------------------------------------------------------------
    conn3, router3 = create_router(
        replay_db,
        router_mod,
        cost_model_mod,
        baseline,
    )
    try:
        digest_restart3 = router_mod.canonical_entry_router_digest(conn3)
        snap3 = state_snapshot(router3, locked_tracks)
        quick3 = conn3.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        conn3.close()

    original_hash_after = sha256_file(original_db)

    def snapshot_semantically_valid(snap: dict[str, Any]) -> bool:
        return (
            snap["route_count"] == 1
            and snap["route_states"] == ("FILLED",)
            and snap["order_count"] == 3
            and all(state == "FILLED" for state in snap["order_states"])
            and snap["position_count"] == 3
            and all(state == "OPEN" for state in snap["position_states"])
            and snap["position_tracks_set"] == snap["locked_tracks_set"]
            and frozenset(snap["order_tracks"]) == snap["locked_tracks_set"]
            and len(set(snap["order_ids"])) == 3
            and set(snap["position_order_ids"]) == set(snap["order_ids"])
        )

    checks = {
        "exact_copy_started": copy_hash_initial == original_hash_before,
        "restart1_filled_state_persisted": snapshot_semantically_valid(snap1),
        "restart2_filled_state_persisted": snapshot_semantically_valid(snap2_before),
        "restart1_restart2_digest_equal": (
            digest_restart1 == digest_before_replay
        ),
        "route_replay_stays_filled": replay_route_state == "FILLED",
        "route_replay_digest_unchanged": (
            digest_before_replay == digest_after_route_replay
        ),
        "fill_replay_stays_filled": replay_fill_route_state == "FILLED",
        "fill_replay_digest_unchanged": (
            digest_after_route_replay == digest_after_fill_replay
        ),
        "fill_replay_order_ids_exact": (
            set(replay_order_ids) == set(snap2_before["order_ids"])
            and len(replay_order_ids) == 3
        ),
        "state_unchanged_after_exact_replay": (
            snap2_before == snap2_after
        ),
        "restart3_filled_state_persisted": snapshot_semantically_valid(snap3),
        "restart3_digest_unchanged": (
            digest_restart3 == digest_before_replay
        ),
        "quick_check_restart1": str(quick1).lower() == "ok",
        "quick_check_restart2": str(quick2).lower() == "ok",
        "quick_check_restart3": str(quick3).lower() == "ok",
        "original_hash_unchanged": (
            original_hash_before == original_hash_after
        ),
    }

    print("-" * 120)
    print("PERSISTED STATE")
    print("-" * 120)
    print(f"Restart #1 position order             : {snap1['position_tracks_raw']}")
    print(f"Restart #2 position order             : {snap2_before['position_tracks_raw']}")
    print(f"Restart #3 position order             : {snap3['position_tracks_raw']}")
    print(f"Semantic locked membership            : {sorted(snap3['position_tracks_set'])}")
    print(f"Restart #1 digest                     : {digest_restart1}")
    print(f"Before replay digest                  : {digest_before_replay}")
    print(f"After route replay digest             : {digest_after_route_replay}")
    print(f"After fill replay digest              : {digest_after_fill_replay}")
    print(f"Restart #3 digest                     : {digest_restart3}")
    print()

    print("-" * 120)
    print("VALIDATION")
    print("-" * 120)
    for label, ok in checks.items():
        print(f"{label:<52}: {'PASS' if ok else 'FAIL'}")

    all_ok = all(checks.values())

    print()
    print(f"Original SHA256 after                : {original_hash_after}")
    print("Original B1D2 paper DB touched       : NO")
    print("New paper market action              : NO")
    print("Wallet/signing/live orders           : NO")
    print("Parameter tuning/reselection         : NO")
    print()
    print(f"RESULT: {'PASS' if all_ok else 'CHECK'}")

    if all_ok:
        print(
            "NEXT: Phase 4 live exit-track orchestration foundation using the "
            "existing three OPEN positions and locked FINAL-A / FINAL-B / SENS-C exits. "
            "Do not tune or reselect exits."
        )
        return 0

    print("Do not advance to live exit orchestration.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
