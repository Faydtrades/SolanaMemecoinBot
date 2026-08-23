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

NS4 = "tradingbot_local_phase4_b1d3_v02"

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


def state_snapshot(
    conn: sqlite3.Connection,
    router: Any,
    locked_tracks: tuple[str, ...],
) -> dict[str, Any]:
    """
    B1D3 v0.2 intentionally uses the public router API for route state and
    only real PaperOrder schema fields for order state.

    PaperOrder has signal_key, not candidate_id.
    candidate_id belongs to EntryRoute.
    """
    routes = list(router.list_routes())

    order_rows = conn.execute(
        """
        SELECT
            paper_order_id,
            signal_key,
            mint,
            track_id,
            state,
            requested_size_lamports,
            filled_size_lamports,
            simulated_entry_price_numerator_raw,
            simulated_entry_price_denominator_raw
        FROM paper_orders
        ORDER BY track_id, paper_order_id
        """
    ).fetchall()

    positions = list(router.lifecycle.list_open_positions())

    route_semantic = tuple(
        sorted(
            (
                str(route.route_id),
                str(route.candidate_id),
                str(route.mint),
                str(route.parameter_set_id),
                enum_value(route.state),
                int(route.selected_market_ingest_seq)
                if route.selected_market_ingest_seq is not None
                else None,
                str(route.selected_source_event_key)
                if route.selected_source_event_key is not None
                else None,
            )
            for route in routes
        )
    )

    order_semantic = tuple(
        sorted(
            (
                str(row["paper_order_id"]),
                str(row["signal_key"]),
                str(row["mint"]),
                str(row["track_id"]),
                str(row["state"]),
                int(row["requested_size_lamports"]),
                int(row["filled_size_lamports"])
                if row["filled_size_lamports"] is not None
                else None,
                str(row["simulated_entry_price_numerator_raw"])
                if row["simulated_entry_price_numerator_raw"] is not None
                else None,
                str(row["simulated_entry_price_denominator_raw"])
                if row["simulated_entry_price_denominator_raw"] is not None
                else None,
            )
            for row in order_rows
        )
    )

    position_semantic = tuple(
        sorted(
            (
                str(p.paper_position_id),
                str(p.paper_order_id),
                str(p.signal_key),
                str(p.mint),
                str(p.track_id),
                enum_value(p.state),
                int(p.filled_size_lamports),
                str(p.entry_price_numerator_raw),
                str(p.entry_price_denominator_raw),
            )
            for p in positions
        )
    )

    return {
        "route_count": len(routes),
        "route_states": tuple(sorted(enum_value(r.state) for r in routes)),
        "route_semantic": route_semantic,
        "order_count": len(order_rows),
        "order_states": tuple(sorted(str(r["state"]) for r in order_rows)),
        "order_tracks": tuple(sorted(str(r["track_id"]) for r in order_rows)),
        "order_ids": tuple(sorted(str(r["paper_order_id"]) for r in order_rows)),
        "order_semantic": order_semantic,
        "position_count": len(positions),
        "position_states": tuple(sorted(enum_value(p.state) for p in positions)),
        "position_tracks_raw": tuple(str(p.track_id) for p in positions),
        "position_tracks": tuple(sorted(str(p.track_id) for p in positions)),
        "position_order_ids": tuple(sorted(str(p.paper_order_id) for p in positions)),
        "position_semantic": position_semantic,
        "locked_tracks": tuple(sorted(locked_tracks)),
    }


def snapshot_semantically_valid(
    snap: dict[str, Any],
) -> bool:
    return (
        snap["route_count"] == 1
        and snap["route_states"] == ("FILLED",)
        and snap["order_count"] == 3
        and snap["order_states"] == ("FILLED", "FILLED", "FILLED")
        and snap["position_count"] == 3
        and snap["position_states"] == ("OPEN", "OPEN", "OPEN")
        and snap["order_tracks"] == snap["locked_tracks"]
        and snap["position_tracks"] == snap["locked_tracks"]
        and len(set(snap["order_ids"])) == 3
        and snap["position_order_ids"] == snap["order_ids"]
    )


def main() -> int:
    print("=" * 120)
    print("PHASE 4.3B1D3 - FILLED ENTRY RESTART / REPLAY PERSISTENCE v0.2")
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
        original_db.stem + "_b1d3_restart_replay_v0_2.sqlite3"
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
    print("Schema correction                    : PaperOrder.signal_key; candidate_id only on EntryRoute")
    print()

    # Restart #1
    conn1, router1 = create_router(
        replay_db,
        router_mod,
        cost_model_mod,
        baseline,
    )
    try:
        digest_restart1 = router_mod.canonical_entry_router_digest(conn1)
        snap1 = state_snapshot(conn1, router1, locked_tracks)
        quick1 = conn1.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        conn1.close()

    # Restart #2 + exact replay.
    conn2, router2 = create_router(
        replay_db,
        router_mod,
        cost_model_mod,
        baseline,
    )
    try:
        digest_before_replay = router_mod.canonical_entry_router_digest(conn2)
        snap2_before = state_snapshot(conn2, router2, locked_tracks)

        route_replay = router2.route_candidate(candidate)
        digest_after_route_replay = router_mod.canonical_entry_router_digest(conn2)
        snap_after_route_replay = state_snapshot(conn2, router2, locked_tracks)

        fill_replay = router2.on_market_observation(
            candidate=candidate,
            observation=observation,
            price_impact_bps=impact_bps,
            observed_priority_fee_lamports=None,
            venue_fee_bps=None,
        )
        digest_after_fill_replay = router_mod.canonical_entry_router_digest(conn2)
        snap2_after = state_snapshot(conn2, router2, locked_tracks)
        quick2 = conn2.execute("PRAGMA quick_check").fetchone()[0]

        replay_route_state = enum_value(route_replay.route.state)
        replay_fill_route_state = enum_value(fill_replay.route.state)
        route_replay_order_ids = tuple(
            sorted(str(o.paper_order_id) for o in route_replay.orders)
        )
        fill_replay_order_ids = tuple(
            sorted(str(o.paper_order_id) for o in fill_replay.orders)
        )
    finally:
        conn2.close()

    # Restart #3
    conn3, router3 = create_router(
        replay_db,
        router_mod,
        cost_model_mod,
        baseline,
    )
    try:
        digest_restart3 = router_mod.canonical_entry_router_digest(conn3)
        snap3 = state_snapshot(conn3, router3, locked_tracks)
        quick3 = conn3.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        conn3.close()

    original_hash_after = sha256_file(original_db)

    checks = {
        "exact_copy_started": copy_hash_initial == original_hash_before,
        "restart1_filled_state_persisted": snapshot_semantically_valid(snap1),
        "restart2_filled_state_persisted": snapshot_semantically_valid(snap2_before),
        "restart1_restart2_digest_equal": (
            digest_restart1 == digest_before_replay
        ),
        "route_replay_stays_filled": replay_route_state == "FILLED",
        "route_replay_order_ids_exact": (
            route_replay_order_ids == snap2_before["order_ids"]
        ),
        "route_replay_digest_unchanged": (
            digest_before_replay == digest_after_route_replay
        ),
        "route_replay_state_unchanged": (
            snap2_before == snap_after_route_replay
        ),
        "fill_replay_stays_filled": replay_fill_route_state == "FILLED",
        "fill_replay_order_ids_exact": (
            fill_replay_order_ids == snap2_before["order_ids"]
        ),
        "fill_replay_digest_unchanged": (
            digest_after_route_replay == digest_after_fill_replay
        ),
        "state_unchanged_after_exact_replay": (
            snap2_before == snap2_after
        ),
        "restart3_filled_state_persisted": snapshot_semantically_valid(snap3),
        "restart3_digest_unchanged": (
            digest_restart3 == digest_before_replay
        ),
        "restart3_state_matches_restart1": (
            snap3 == snap1
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
    print(f"Restart #1 position API order         : {snap1['position_tracks_raw']}")
    print(f"Restart #2 position API order         : {snap2_before['position_tracks_raw']}")
    print(f"Restart #3 position API order         : {snap3['position_tracks_raw']}")
    print(f"Semantic position tracks              : {snap3['position_tracks']}")
    print(f"Semantic order tracks                 : {snap3['order_tracks']}")
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
        print(f"{label:<54}: {'PASS' if ok else 'FAIL'}")

    all_ok = all(checks.values())

    print()
    print(f"Original SHA256 after                : {original_hash_after}")
    print("Original B1D2 paper DB touched       : NO")
    print("New live market action               : NO")
    print("Wallet/signing/live orders           : NO")
    print("Parameter tuning/reselection         : NO")
    print()
    print(f"RESULT: {'PASS' if all_ok else 'CHECK'}")

    if all_ok:
        print(
            "NEXT: Phase 4 live exit-track orchestration foundation using the "
            "persisted three OPEN positions and locked FINAL-A / FINAL-B / SENS-C exits. "
            "Do not tune or reselect exits."
        )
        return 0

    print("Do not advance to live exit orchestration.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
