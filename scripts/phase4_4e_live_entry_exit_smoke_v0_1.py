from __future__ import annotations

import argparse
import collections
import importlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import types
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PHASE2_ROOT = PROJECT_ROOT / "src" / "phase2"
PHASE4_ROOT = PROJECT_ROOT / "src" / "phase4"

COLLECTOR = PROJECT_ROOT / "scripts" / "live_pump_collector_v0_3_4.py"
PRODUCTION_DB = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"
LOCKED_SELECTION = (
    PROJECT_ROOT
    / "data"
    / "research"
    / "phase3"
    / "EXP-0005"
    / "EXP-0005_D_selection_v0_1.json"
)
OUT_DIR = PROJECT_ROOT / "data" / "paper" / "live_smoke"

NS2 = "tradingbot_local_phase2_p4_4e"
NS4 = "tradingbot_local_phase4_p4_4e"

EXPECTED_ROLES = ("CONTROL", "ROBUST_1", "ROBUST_2", "ROBUST_3")

LOCKED_BASELINE_ID = "P4-COST-BASELINE-0001"
LOCKED_BASELINE_FINGERPRINT = (
    "9652aeffc792eb48d62348360c6ae804f660e43329195d1f169219b80ccab67c"
)
LOCKED_IMPACT_MODEL_ID = "P4-RUNTIME-PRICE-IMPACT-0001"
LOCKED_IMPACT_FINGERPRINT = (
    "30d8880aa58d56f70f3baf235f4973a80c94b59724a5ee286b67f60be924d9e1"
)

LOCKED_EXIT_SPEC_FINGERPRINT = (
    "0bde5378f4ff2b63c6a96ad64140840ef8b2770c3be772a7aa71f8bdeb070129"
)
LOCKED_EXIT_IMPACT_MODEL_ID = "P4-RUNTIME-EXIT-PRICE-IMPACT-0001"
LOCKED_EXIT_IMPACT_FINGERPRINT = (
    "e1f1fd1c786cc679cf54f45c16b5d9a0c6aff37ed4132ac63c8abc5506aea59d"
)
LOCKED_TRACKS = ("FINAL-A", "FINAL-B", "SENS-C")

EXPECTED_LOCKED_ROWS = {
    "CONTROL": {
        "parameter_set_id": "FP1-EXP0005-D-005-CONTROL-R500-T2-U1-PD1000-9000-RB200-BU1-FL0-RC200-RW10000",
        "min_return_bps": 500,
        "min_trades_since_t0": 2,
        "min_unique_buyers_since_t0": 1,
        "min_depth_bps": 1000,
        "max_depth_bps": 9000,
        "min_rebound_bps": 200,
        "min_buys": 1,
        "min_net_flow_reserve_ppm": 0,
        "min_extension_from_response_bps": 200,
        "max_extension_from_response_bps": 10000,
    },
    "ROBUST_1": {
        "parameter_set_id": "FP1-EXP0005-D-026-ROBUST_1-R1500-T5-U9-PD2500-6500-RB2300-BU2-FL6000-RC200-RW2500",
        "min_return_bps": 1500,
        "min_trades_since_t0": 5,
        "min_unique_buyers_since_t0": 9,
        "min_depth_bps": 2500,
        "max_depth_bps": 6500,
        "min_rebound_bps": 2300,
        "min_buys": 2,
        "min_net_flow_reserve_ppm": 6000,
        "min_extension_from_response_bps": 200,
        "max_extension_from_response_bps": 2500,
    },
    "ROBUST_2": {
        "parameter_set_id": "FP1-EXP0005-D-050-ROBUST_2-R2000-T3-U5-PD2500-6500-RB2300-BU2-FL6000-RC200-RW2500",
        "min_return_bps": 2000,
        "min_trades_since_t0": 3,
        "min_unique_buyers_since_t0": 5,
        "min_depth_bps": 2500,
        "max_depth_bps": 6500,
        "min_rebound_bps": 2300,
        "min_buys": 2,
        "min_net_flow_reserve_ppm": 6000,
        "min_extension_from_response_bps": 200,
        "max_extension_from_response_bps": 2500,
    },
    "ROBUST_3": {
        "parameter_set_id": "FP1-EXP0005-D-074-ROBUST_3-R5000-T3-U2-PD2500-6500-RB2300-BU2-FL6000-RC200-RW2500",
        "min_return_bps": 5000,
        "min_trades_since_t0": 3,
        "min_unique_buyers_since_t0": 2,
        "min_depth_bps": 2500,
        "max_depth_bps": 6500,
        "min_rebound_bps": 2300,
        "min_buys": 2,
        "min_net_flow_reserve_ppm": 6000,
        "min_extension_from_response_bps": 200,
        "max_extension_from_response_bps": 2500,
    },
}

EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)


def bind_namespace(name: str, path: Path) -> None:
    if name in sys.modules:
        return
    pkg = types.ModuleType(name)
    pkg.__path__ = [str(path)]
    pkg.__package__ = name
    sys.modules[name] = pkg


bind_namespace(NS2, PHASE2_ROOT)
bind_namespace(NS4, PHASE4_ROOT)


def local_import(ns: str, root: Path, module_name: str):
    mod = importlib.import_module(f"{ns}.{module_name}")
    loaded = Path(getattr(mod, "__file__", "")).resolve()
    expected = (root / f"{module_name}.py").resolve()
    if loaded != expected:
        raise RuntimeError(
            f"Local binding mismatch for {module_name}: "
            f"loaded={loaded} expected={expected}"
        )
    return mod


def primitive(value: Any) -> Any:
    if is_dataclass(value):
        return primitive(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, dict):
        return {str(k): primitive(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [primitive(v) for v in value]
    return value


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def us_to_dt(value: int) -> datetime:
    return EPOCH_UTC + timedelta(microseconds=int(value))


def now_us() -> int:
    delta = utc_now() - EPOCH_UTC
    return (
        delta.days * 86_400_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )


def open_production_ro() -> sqlite3.Connection:
    uri = PRODUCTION_DB.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=2.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA busy_timeout=1000")
    return conn


def max_rowid(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT rowid FROM pump_events ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    return 0 if row is None else int(row["rowid"])


def fetch_after(
    conn: sqlite3.Connection,
    cursor: int,
    limit: int,
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT rowid AS p1_rowid, *
        FROM pump_events
        WHERE rowid > ?
        ORDER BY rowid
        LIMIT ?
        """,
        (cursor, limit),
    ).fetchall()


def stop_process(
    proc: subprocess.Popen,
    grace_seconds: int = 15,
) -> tuple[str, int | None]:
    if proc.poll() is not None:
        return "EXITED_NATURALLY", proc.returncode

    if os.name == "nt":
        try:
            proc.send_signal(signal.CTRL_BREAK_EVENT)
            deadline = time.monotonic() + grace_seconds
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    return "CTRL_BREAK_GRACEFUL", proc.returncode
                time.sleep(0.25)
        except Exception:
            pass

    try:
        proc.terminate()
        deadline = time.monotonic() + min(grace_seconds, 10)
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                return "TERMINATE", proc.returncode
            time.sleep(0.25)
    except Exception:
        pass

    try:
        proc.kill()
        proc.wait(timeout=10)
        return "KILL", proc.returncode
    except Exception:
        return "STOP_FAILED", proc.poll()


def load_locked_parameter_sets(FirstPullbackParameterSet, Strategy):
    payload = json.loads(LOCKED_SELECTION.read_text(encoding="utf-8"))
    if payload.get("experiment_id") != "EXP-0005":
        raise ValueError("Locked entry selection must be EXP-0005")
    if payload.get("status") != "LOCKED_FOR_EXIT_RESEARCH":
        raise ValueError("EXP-0005 selection is not LOCKED_FOR_EXIT_RESEARCH")

    rows = payload.get("carry_forward")
    if not isinstance(rows, list) or len(rows) != 4:
        raise ValueError("Expected exactly four locked carry-forward roles")

    roles = tuple(str(row.get("role")) for row in rows)
    if roles != EXPECTED_ROLES:
        raise ValueError(f"Unexpected locked roles/order: {roles}")

    params_by_role = {}
    for row in rows:
        role = str(row["role"])
        expected = EXPECTED_LOCKED_ROWS[role]
        for key, expected_value in expected.items():
            actual = row.get(key)
            if actual != expected_value:
                raise ValueError(
                    f"Locked role drift {role}.{key}: "
                    f"actual={actual!r} expected={expected_value!r}"
                )

        p = FirstPullbackParameterSet(
            parameter_set_id=str(row["parameter_set_id"]),
            strategy_version="v1.1",
            max_entry_age_ms=300_000,
            impulse_parameters={
                "min_return_bps": int(row["min_return_bps"]),
                "min_trades_since_t0": int(row["min_trades_since_t0"]),
                "min_unique_buyers_since_t0": int(
                    row["min_unique_buyers_since_t0"]
                ),
            },
            pullback_parameters={
                "min_depth_bps": int(row["min_depth_bps"]),
                "max_depth_bps": int(row["max_depth_bps"]),
            },
            buyer_response_parameters={
                "window_id": "3s",
                "min_rebound_bps": int(row["min_rebound_bps"]),
                "min_buys": int(row["min_buys"]),
                "min_net_flow_reserve_ppm": int(
                    row["min_net_flow_reserve_ppm"]
                ),
            },
            reclaim_parameters={
                "min_extension_from_response_bps": int(
                    row["min_extension_from_response_bps"]
                )
            },
            runaway_entry_parameters={
                "max_extension_from_response_bps": int(
                    row["max_extension_from_response_bps"]
                )
            },
        )
        Strategy.validate_parameters(p)
        params_by_role[role] = p

    return payload, params_by_role


def resolve_locked_baseline(cost_baseline_mod, PaperCostBaseline):
    candidates: list[tuple[str, Any]] = []

    for name, value in vars(cost_baseline_mod).items():
        if isinstance(value, PaperCostBaseline):
            candidates.append((f"object:{name}", value))

    for name, value in vars(cost_baseline_mod).items():
        if not callable(value) or "baseline" not in name.lower():
            continue
        try:
            import inspect
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

    matching = [
        (source, baseline)
        for source, baseline in candidates
        if getattr(baseline, "assumption_set_id", None) == LOCKED_BASELINE_ID
        and getattr(baseline, "fingerprint", None) == LOCKED_BASELINE_FINGERPRINT
    ]
    if not matching:
        raise RuntimeError("Could not resolve exact locked P4 cost baseline")
    return matching[0]


def point_for_event(quote_mod, event):
    point = quote_mod.QuoteAwarePriceEngineV01.point(event)
    if point is None:
        return None
    if point.price_proxy is None:
        return None
    if point.numerator_reserve_raw is None or point.token_reserve_raw is None:
        return None
    if int(point.numerator_reserve_raw) <= 0 or int(point.token_reserve_raw) <= 0:
        return None
    return point


def source_is_gap_recovery(event) -> bool:
    source = getattr(event.base, "source", None)
    value = getattr(source, "value", source)
    return str(value).upper() == "GAP_RECOVERY"


def enum_value(value: Any) -> str:
    return str(getattr(value, "value", value))



def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 4.4E controlled live causal entry->parallel-exit paper smoke."
        )
    )
    parser.add_argument(
        "--max-entry-seconds",
        type=int,
        default=900,
        help="Maximum time to acquire one accepted live SOL_NATIVE paper entry. Default: 900s.",
    )
    parser.add_argument(
        "--max-exit-seconds",
        type=int,
        default=60,
        help="Maximum time after accepted entry to close all three locked exit tracks. Default: 60s.",
    )
    parser.add_argument("--poll-ms", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    if args.max_entry_seconds <= 0 or args.max_exit_seconds <= 0:
        raise SystemExit("max-entry-seconds and max-exit-seconds must be > 0")
    if not COLLECTOR.exists():
        raise SystemExit(f"Missing validated collector: {COLLECTOR}")
    if not LOCKED_SELECTION.exists():
        raise SystemExit(f"Missing locked selection: {LOCKED_SELECTION}")

    # Exact Phase-2 bindings already live-validated in B1D2.
    models_mod = local_import(NS2, PHASE2_ROOT, "models_v0_1")
    quote_mod = local_import(NS2, PHASE2_ROOT, "quote_aware_price_v0_1")
    flow_mod = local_import(NS2, PHASE2_ROOT, "denomination_aware_flow_v0_1")
    strategy_mod = local_import(NS2, PHASE2_ROOT, "strategy_first_pullback_v0_2")
    clock_mod = local_import(NS2, PHASE2_ROOT, "strategy_clock_v0_2")

    # Exact Phase-4 stack.
    cost_model_mod = local_import(NS4, PHASE4_ROOT, "paper_cost_model_v0_2")
    cost_baseline_mod = local_import(NS4, PHASE4_ROOT, "paper_cost_baseline_v0_1")
    router_mod = local_import(NS4, PHASE4_ROOT, "paper_entry_router_v0_1")
    entry_impact_mod = local_import(NS4, PHASE4_ROOT, "runtime_price_impact_v0_1")
    exit_orch_mod = local_import(NS4, PHASE4_ROOT, "paper_exit_orchestrator_v0_1")
    exit_bridge_mod = local_import(NS4, PHASE4_ROOT, "paper_exit_lifecycle_bridge_v0_2")
    exit_impact_mod = local_import(NS4, PHASE4_ROOT, "runtime_exit_price_impact_v0_1")
    exit_exec_mod = local_import(NS4, PHASE4_ROOT, "paper_exit_fill_executor_v0_1")

    FirstPullbackParameterSet = models_mod.FirstPullbackParameterSet
    Strategy = strategy_mod.FirstPullbackStrategyV02
    Clock = clock_mod.FirstPullbackStrategyClockV02
    QuoteAdapter = quote_mod.Phase1QuoteAwareAdapterV01
    DenomEngine = flow_mod.DenominationAwareFeatureEngineV01
    EntryImpactProvider = entry_impact_mod.RuntimePriceImpactV01

    selection_payload, params_by_role = load_locked_parameter_sets(
        FirstPullbackParameterSet,
        Strategy,
    )
    for role, params in params_by_role.items():
        if params.strategy_version != "v1.1":
            raise RuntimeError(
                f"Unexpected locked strategy version {role}: {params.strategy_version}"
            )
        Strategy.validate_parameters(params)

    baseline_source, baseline = resolve_locked_baseline(
        cost_baseline_mod,
        cost_model_mod.PaperCostBaseline,
    )
    cost_model = cost_model_mod.PaperCostModelV02(baseline)

    # Fail-fast immutable contract checks BEFORE collector startup.
    if entry_impact_mod.MODEL_ID != LOCKED_IMPACT_MODEL_ID:
        raise RuntimeError(f"Entry impact model ID drift: {entry_impact_mod.MODEL_ID}")
    if entry_impact_mod.MODEL_FINGERPRINT != LOCKED_IMPACT_FINGERPRINT:
        raise RuntimeError("Entry impact model fingerprint drift")
    if exit_orch_mod.LOCKED_EXIT_SPEC_FINGERPRINT != LOCKED_EXIT_SPEC_FINGERPRINT:
        raise RuntimeError("Locked exit spec fingerprint drift")
    if tuple(exit_orch_mod.LOCKED_TRACK_ORDER) != LOCKED_TRACKS:
        raise RuntimeError(
            f"Locked exit track order drift: {tuple(exit_orch_mod.LOCKED_TRACK_ORDER)}"
        )
    if exit_impact_mod.MODEL_ID != LOCKED_EXIT_IMPACT_MODEL_ID:
        raise RuntimeError(f"Exit impact model ID drift: {exit_impact_mod.MODEL_ID}")
    if exit_impact_mod.MODEL_FINGERPRINT != LOCKED_EXIT_IMPACT_FINGERPRINT:
        raise RuntimeError("Exit impact model fingerprint drift")
    if baseline.reference_position_lamports != 100_000_000:
        raise RuntimeError("Reference position drift: expected 0.10 SOL")
    if baseline.entry_latency_ms != 500:
        raise RuntimeError("Entry latency drift: expected 500ms")
    if baseline.exit_latency_ms != 500:
        raise RuntimeError("Exit latency drift: expected 500ms")
    if baseline.entry_slippage_cap_bps != 1500:
        raise RuntimeError("Entry slippage cap drift: expected 1500bps")
    if baseline.exit_slippage_cap_bps != 2000:
        raise RuntimeError("Exit slippage cap drift: expected 2000bps")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
    paper_db = OUT_DIR / f"phase4_4e_live_entry_exit_{stamp}.sqlite3"
    audit_path = OUT_DIR / f"phase4_4e_live_entry_exit_{stamp}.json"
    collector_log = OUT_DIR / f"phase4_4e_collector_{stamp}.log"

    paper_conn = router_mod.connect_entry_router_db(paper_db)
    router = router_mod.PaperEntryRouterV01(paper_conn, cost_model)
    locked_entry_tracks = tuple(router_mod.LOCKED_PHASE4_TRACKS)
    if locked_entry_tracks != LOCKED_TRACKS:
        raise RuntimeError(f"Entry-router track drift: {locked_entry_tracks}")

    prod_conn = open_production_ro()
    try:
        start_cursor = max_rowid(prod_conn)
    finally:
        prod_conn.close()

    print("=" * 124)
    print("PHASE 4.4E - CONTROLLED LIVE CAUSAL ENTRY -> PARALLEL EXIT-FILL SMOKE v0.1")
    print("=" * 124)
    print(f"Project root                         : {PROJECT_ROOT}")
    print(f"Collector                            : {COLLECTOR}")
    print(f"Production DB                        : {PRODUCTION_DB}")
    print("Production DB sidecar access         : QUERY-ONLY / READ-ONLY")
    print(f"Locked entry selection               : {LOCKED_SELECTION}")
    print(f"Locked entry roles                   : {EXPECTED_ROLES}")
    print("Entry strategy                       : FirstPullback v1.1 / V02")
    print("Entry clock                          : FirstPullbackStrategyClockV02")
    print(f"Reference position                   : {baseline.reference_position_lamports} lamports (0.10 SOL)")
    print(f"Cost baseline                        : {baseline.assumption_set_id}")
    print(f"Cost baseline fingerprint            : {baseline.fingerprint}")
    print(f"Cost baseline source                 : {baseline_source}")
    print(f"Entry latency / cap                  : {baseline.entry_latency_ms}ms / {baseline.entry_slippage_cap_bps}bps")
    print(f"Exit latency / cap                   : {baseline.exit_latency_ms}ms / {baseline.exit_slippage_cap_bps}bps")
    print(f"Entry impact model                   : {entry_impact_mod.MODEL_ID}")
    print(f"Entry impact fingerprint             : {entry_impact_mod.MODEL_FINGERPRINT}")
    print(f"Locked exit spec fingerprint         : {exit_orch_mod.LOCKED_EXIT_SPEC_FINGERPRINT}")
    print(f"Exit impact model                    : {exit_impact_mod.MODEL_ID}")
    print(f"Exit impact fingerprint              : {exit_impact_mod.MODEL_FINGERPRINT}")
    print(f"Locked exit tracks                   : {LOCKED_TRACKS}")
    print("FINAL-A                              : E3_TP10_T15")
    print("FINAL-B                              : E4_ACT10_GB03_T15")
    print("SENS-C                               : SENS_TP20_T5 (sensitivity only)")
    print(f"Paper DB                             : {paper_db}")
    print("PnL / expectancy / PF / drawdown     : NO (next bounded step)")
    print("Wallet / signing / live orders       : NO")
    print("Parameter tuning / reselection       : NO")
    print(f"Start cursor rowid                   : {start_cursor}")
    print(f"Max entry acquisition                : {args.max_entry_seconds}s")
    print(f"Max post-entry exit window           : {args.max_exit_seconds}s")
    print("Stop condition                       : all FINAL-A / FINAL-B / SENS-C positions CLOSED")
    print()

    creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    collector_fh = collector_log.open(
        "w",
        encoding="utf-8",
        errors="replace",
        buffering=1,
    )
    proc = subprocess.Popen(
        [
            sys.executable,
            "-u",
            str(COLLECTOR),
            "--max-pump-events",
            "0",
            "--drain-seconds",
            "20",
        ],
        cwd=str(PROJECT_ROOT),
        stdout=collector_fh,
        stderr=subprocess.STDOUT,
        env=env,
        creationflags=creationflags,
    )

    print(f"[INFO] collector PID: {proc.pid}")
    print("[INFO] automatic stop enabled; no Ctrl+C needed.")

    denom_engine = DenomEngine()
    runtimes: dict[tuple[str, str], dict[str, Any]] = {}
    eligible_mints: set[str] = set()
    cursor = start_cursor

    rows_seen = 0
    launch_rows = 0
    normalized_events = 0
    normalized_skips: collections.Counter[str] = collections.Counter()
    feature_states = 0
    strategy_evaluations = 0
    candidate_counts: collections.Counter[str] = collections.Counter()
    candidate_identity_counts: collections.Counter[str] = collections.Counter()
    entry_rejections: list[dict[str, Any]] = []
    strategy_errors: list[str] = []

    entry_active: dict[str, Any] | None = None
    entry_execution = None
    entry_role = None
    entry_candidate = None
    entry_envelope = None
    entry_observation = None
    entry_impact = None

    exit_bridge = None
    exit_executor = None
    signal_key: str | None = None
    exit_phase_started_mono: float | None = None
    exit_market_evaluations = 0
    exit_clock_evaluations = 0
    exit_attempts_reported: set[str] = set()
    exit_rejections: list[dict[str, Any]] = []
    exit_closures: dict[str, dict[str, Any]] = {}

    started_at = utc_now()
    started_mono = time.monotonic()
    next_health = started_mono
    stop_mode = "NOT_STOPPED"
    collector_returncode = None

    prod_conn = open_production_ro()

    def record_strategy_eval(role: str, evaluation: Any) -> None:
        nonlocal strategy_evaluations
        strategy_evaluations += 1

    def fire_entry_expiries() -> None:
        current_us = now_us()
        for (_mint, _role), rt in list(runtimes.items()):
            run = rt["run"]
            deadline = rt.get("deadline")
            if deadline is None or run.finished:
                continue
            Clock.fire_if_due(
                deadline,
                run,
                rt["params"],
                now_us=current_us,
            )

    def lifecycle_positions_for_signal() -> dict[str, Any]:
        if exit_bridge is None or signal_key is None:
            return {}
        refs = exit_bridge.bind_signal(signal_key)
        out = {}
        for ref in refs:
            p = router.lifecycle.get_position(ref.paper_position_id)
            if p is None:
                raise RuntimeError(f"Missing lifecycle position {ref.paper_position_id}")
            out[p.track_id] = p
        return out

    def maybe_bind_exit_routes() -> None:
        if exit_executor is None:
            return
        for p in lifecycle_positions_for_signal().values():
            if enum_value(p.state) in ("EXIT_PENDING", "CLOSED"):
                exit_executor.bind_position(p.paper_position_id)

    def record_exit_result(track_id: str, result: Any) -> None:
        attempt = result.attempt
        if attempt is not None and attempt.attempt_id not in exit_attempts_reported:
            exit_attempts_reported.add(attempt.attempt_id)
            print(
                f"[EXIT-ATTEMPT] track={track_id} "
                f"decision={enum_value(attempt.decision)} "
                f"obs={attempt.market_observed_at.isoformat()} "
                f"impact={attempt.price_impact_bps}bps "
                f"adverse={attempt.adverse_slippage_bps}bps "
                f"cap={attempt.slippage_cap_bps}bps"
            )
            if enum_value(attempt.decision) == "REJECTED_SLIPPAGE":
                exit_rejections.append(
                    {
                        "track_id": track_id,
                        "attempt": primitive(attempt),
                        "fill_evaluation": primitive(result.fill_evaluation),
                        "price_impact": primitive(result.price_impact),
                    }
                )

        if enum_value(result.position.state) == "CLOSED" and track_id not in exit_closures:
            route = result.route
            exit_closures[track_id] = {
                "route": primitive(route),
                "position": primitive(result.position),
                "attempt": primitive(result.attempt),
                "fill_evaluation": primitive(result.fill_evaluation),
                "price_impact": primitive(result.price_impact),
                "explicit_costs": primitive(result.explicit_costs),
            }
            print(
                f"[EXIT-CLOSED] track={track_id} "
                f"reason={route.exit_reason} "
                f"state={enum_value(route.state)} "
                f"gross={route.gross_exit_proceeds_lamports} "
                f"cost={route.total_exit_explicit_cost_lamports}"
            )

    def all_tracks_closed() -> bool:
        pos = lifecycle_positions_for_signal()
        return (
            set(pos) == set(LOCKED_TRACKS)
            and all(enum_value(pos[t].state) == "CLOSED" for t in LOCKED_TRACKS)
        )

    try:
        while True:
            now_mono = time.monotonic()

            if proc.poll() is not None:
                stop_mode = "EXITED_NATURALLY"
                collector_returncode = proc.returncode
                print(f"[CHECK] collector exited early returncode={collector_returncode}")
                break

            if entry_execution is None:
                if now_mono - started_mono >= args.max_entry_seconds:
                    break
                fire_entry_expiries()
            else:
                assert exit_phase_started_mono is not None
                if all_tracks_closed():
                    break
                if now_mono - exit_phase_started_mono >= args.max_exit_seconds:
                    break

                # Timer delivery for the locked fallback clocks.  The persisted
                # intent requested_at remains the exact 5s+1us / 15s+1us fire time.
                clock_result = exit_bridge.on_clock(signal_key, utc_now())
                exit_clock_evaluations += len(clock_result.evaluations)
                maybe_bind_exit_routes()

            try:
                rows = fetch_after(prod_conn, cursor, args.batch_size)
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                    time.sleep(args.poll_ms / 1000)
                    continue
                raise

            if rows:
                rows_seen += len(rows)
                cursor = max(int(row["p1_rowid"]) for row in rows)

                causal_rows: list[sqlite3.Row] = []
                for row in rows:
                    event_type = str(row["event_type"]).upper()
                    mint = row["mint"]
                    if event_type == "LAUNCH" and mint:
                        if str(mint) not in eligible_mints:
                            launch_rows += 1
                        eligible_mints.add(str(mint))
                    if mint and str(mint) in eligible_mints:
                        causal_rows.append(row)

                if causal_rows:
                    events, skipped = QuoteAdapter.normalize_rows(causal_rows)
                    normalized_events += len(events)
                    if isinstance(skipped, dict):
                        normalized_skips.update(
                            {str(k): int(v) for k, v in skipped.items()}
                        )

                    for qevent in events:
                        base = qevent.base
                        mint = str(base.mint)
                        if mint not in eligible_mints:
                            continue

                        point = point_for_event(quote_mod, qevent)
                        observed_at = us_to_dt(int(base.observed_at_us))

                        # ------------------------------------------------------
                        # EXIT phase: every same-signal market point is delivered
                        # in observed ingest order to the exit orchestrator and
                        # then to any EXIT_PENDING executors.
                        # ------------------------------------------------------
                        if entry_execution is not None:
                            if mint != entry_envelope.mint:
                                continue

                            if point is not None:
                                exit_obs = exit_orch_mod.ExitMarketObservation(
                                    mint=mint,
                                    observed_at=observed_at,
                                    ingest_seq=int(base.ingest_seq),
                                    price_identity=str(point.identity.label),
                                    price_numerator_raw=int(point.numerator_reserve_raw),
                                    price_denominator_raw=int(point.token_reserve_raw),
                                    is_gap_recovery=source_is_gap_recovery(qevent),
                                    source_event_key=str(base.event_key),
                                )
                                bridged = exit_bridge.on_market_observation(
                                    signal_key,
                                    exit_obs,
                                )
                                exit_market_evaluations += len(bridged.evaluations)
                                maybe_bind_exit_routes()

                                positions = lifecycle_positions_for_signal()
                                for track_id in LOCKED_TRACKS:
                                    p = positions[track_id]
                                    if enum_value(p.state) != "EXIT_PENDING":
                                        continue

                                    exec_obs = exit_exec_mod.ExitExecutionMarketObservation(
                                        mint=mint,
                                        observed_at=observed_at,
                                        ingest_seq=int(base.ingest_seq),
                                        price_identity=str(point.identity.label),
                                        price_numerator_raw=int(point.numerator_reserve_raw),
                                        price_denominator_raw=int(point.token_reserve_raw),
                                        current_virtual_token_reserve_raw=int(point.token_reserve_raw),
                                        is_gap_recovery=source_is_gap_recovery(qevent),
                                        source_event_key=str(base.event_key),
                                    )
                                    result = exit_executor.on_market_observation(
                                        paper_position_id=p.paper_position_id,
                                        observation=exec_obs,
                                        observed_priority_fee_lamports=None,
                                        venue_fee_bps=None,
                                    )
                                    record_exit_result(track_id, result)

                            if all_tracks_closed():
                                break
                            continue

                        # ------------------------------------------------------
                        # ENTRY phase.  This is the already-live-validated B1D2
                        # causal entry path, carried forward unchanged in scope.
                        # ------------------------------------------------------
                        if entry_active is not None:
                            envelope = entry_active["envelope"]
                            route = entry_active["route"]

                            if (
                                mint == envelope.mint
                                and point is not None
                                and not source_is_gap_recovery(qevent)
                                and str(point.identity.label) == envelope.price_identity
                                and observed_at >= route.execution_ready_at
                                and int(base.ingest_seq) > int(envelope.signal_ingest_seq)
                            ):
                                impact = EntryImpactProvider.for_entry(
                                    price_identity=envelope.price_identity,
                                    requested_size_lamports=int(route.requested_size_lamports),
                                    virtual_sol_reserve_lamports=int(point.numerator_reserve_raw),
                                )
                                if not impact.available:
                                    raise RuntimeError(
                                        "SOL_NATIVE entry impact unexpectedly unavailable: "
                                        f"{impact.reason_code}"
                                    )

                                observation = router_mod.EntryMarketObservation(
                                    mint=mint,
                                    observed_at=observed_at,
                                    ingest_seq=int(base.ingest_seq),
                                    price_identity=str(point.identity.label),
                                    price_numerator_raw=int(point.numerator_reserve_raw),
                                    price_denominator_raw=int(point.token_reserve_raw),
                                    is_gap_recovery=False,
                                    source_event_key=str(base.event_key),
                                )

                                execution = router.on_market_observation(
                                    candidate=envelope,
                                    observation=observation,
                                    price_impact_bps=int(impact.router_price_impact_bps),
                                    observed_priority_fee_lamports=None,
                                    venue_fee_bps=None,
                                )
                                route_state = enum_value(execution.route.state)

                                print(
                                    f"[ENTRY-EXECUTION] role={entry_active['role']} "
                                    f"candidate={envelope.candidate_id} "
                                    f"impact={impact.router_price_impact_bps}bps "
                                    f"route_state={route_state}"
                                )

                                if route_state == "REJECTED":
                                    entry_rejections.append(
                                        {
                                            "role": entry_active["role"],
                                            "candidate": primitive(entry_active["candidate"]),
                                            "envelope": primitive(envelope),
                                            "observation": primitive(observation),
                                            "impact": primitive(impact),
                                            "route": primitive(execution.route),
                                            "orders": [primitive(o) for o in execution.orders],
                                            "fill_evaluation": primitive(execution.fill_evaluation),
                                        }
                                    )
                                    entry_active = None
                                    continue

                                if route_state == "FILLED":
                                    entry_execution = execution
                                    entry_role = entry_active["role"]
                                    entry_candidate = entry_active["candidate"]
                                    entry_envelope = envelope
                                    entry_observation = observation
                                    entry_impact = impact
                                    signal_key = envelope.candidate_id

                                    exit_bridge = exit_bridge_mod.PaperExitLifecycleBridgeV02(
                                        paper_conn,
                                        lifecycle=router.lifecycle,
                                    )
                                    exit_executor = exit_exec_mod.PaperExitFillExecutorV01(
                                        paper_conn,
                                        cost_model,
                                        lifecycle=router.lifecycle,
                                        exit_orchestrator=exit_bridge.exit_orchestrator,
                                    )
                                    refs = exit_bridge.bind_signal(signal_key)
                                    if tuple(ref.track_id for ref in refs) != LOCKED_TRACKS:
                                        raise RuntimeError("Exit bridge did not bind semantic locked track order")

                                    exit_phase_started_mono = time.monotonic()
                                    entry_active = None
                                    print(
                                        f"[ENTRY-FILLED] candidate={signal_key} "
                                        f"role={entry_role} "
                                        f"obs={entry_observation.observed_at.isoformat()} "
                                        f"open_positions=3"
                                    )
                                    continue

                                raise RuntimeError(
                                    f"Entry observation left route non-terminal: {route_state}"
                                )

                        if entry_active is not None:
                            continue

                        state = denom_engine.process(qevent)
                        feature_states += 1

                        for role in EXPECTED_ROLES:
                            params = params_by_role[role]
                            key = (mint, role)
                            rt = runtimes.get(key)
                            if rt is None:
                                run = Strategy.start_run(state, params)
                                rt = {"params": params, "run": run, "deadline": None}
                                runtimes[key] = rt

                            run = rt["run"]
                            if rt["deadline"] is None:
                                rt["deadline"] = Clock.schedule_entry_expiry(
                                    state,
                                    run,
                                    params,
                                )

                            deadline = rt["deadline"]
                            if (
                                deadline is not None
                                and state.as_of_observed_at_us >= deadline.expire_at_us
                                and not run.finished
                            ):
                                Clock.fire_if_due(
                                    deadline,
                                    run,
                                    params,
                                    now_us=state.as_of_observed_at_us,
                                )
                                continue

                            if run.finished:
                                continue

                            try:
                                evaluation = Strategy.evaluate(state, run, params)
                            except Exception as exc:
                                strategy_errors.append(
                                    f"{role}:{mint}:{type(exc).__name__}:{exc}"
                                )
                                if len(strategy_errors) <= 5:
                                    print(f"[ERROR] strategy {strategy_errors[-1]}")
                                continue

                            record_strategy_eval(role, evaluation)
                            candidate = evaluation.candidate_signal
                            if candidate is None:
                                continue

                            candidate_counts[role] += 1
                            trigger_point = point_for_event(quote_mod, qevent)
                            if trigger_point is None or source_is_gap_recovery(qevent):
                                continue

                            identity = str(trigger_point.identity.label)
                            candidate_identity_counts[identity] += 1
                            if identity != "SOL_NATIVE":
                                continue

                            envelope = router_mod.CandidateSignalEnvelope(
                                candidate_id=str(candidate.signal_id),
                                mint=mint,
                                strategy_version=str(candidate.strategy_version),
                                parameter_set_id=str(candidate.parameter_set_id),
                                signal_observed_at=us_to_dt(int(candidate.generated_at_us)),
                                signal_ingest_seq=int(base.ingest_seq),
                                price_identity=identity,
                                price_numerator_raw=int(trigger_point.numerator_reserve_raw),
                                price_denominator_raw=int(trigger_point.token_reserve_raw),
                            )
                            routed = router.route_candidate(envelope)
                            if enum_value(routed.route.state) != "ENTRY_PENDING":
                                raise RuntimeError("Fresh SOL route did not enter ENTRY_PENDING")

                            expected_ready = envelope.signal_observed_at + timedelta(
                                milliseconds=baseline.entry_latency_ms
                            )
                            if routed.route.execution_ready_at != expected_ready:
                                raise RuntimeError("Entry execution_ready_at is not exact locked 500ms")

                            entry_active = {
                                "role": role,
                                "candidate": candidate,
                                "envelope": envelope,
                                "route": routed.route,
                            }
                            print(
                                f"[CANDIDATE] SOL_NATIVE role={role} "
                                f"candidate={envelope.candidate_id} "
                                f"signal={envelope.signal_observed_at.isoformat()} "
                                f"ready={routed.route.execution_ready_at.isoformat()}"
                            )
                            break

                    if entry_execution is not None and all_tracks_closed():
                        break

            if entry_execution is not None and all_tracks_closed():
                break

            if time.monotonic() >= next_health:
                phase = "ENTRY" if entry_execution is None else "EXIT"
                if entry_execution is None:
                    pending = (
                        "NONE"
                        if entry_active is None
                        else f"{entry_active['role']}@{entry_active['route'].execution_ready_at.isoformat()}"
                    )
                    print(
                        f"[HEALTH] phase={phase} elapsed={int(time.monotonic()-started_mono)}s "
                        f"rowid={cursor} rows={rows_seen} launches={launch_rows} "
                        f"mints={len(eligible_mints)} events={normalized_events} "
                        f"states={feature_states} evals={strategy_evaluations} "
                        f"candidates={sum(candidate_counts.values())} pending={pending}"
                    )
                else:
                    pos = lifecycle_positions_for_signal()
                    state_text = ",".join(
                        f"{t}:{enum_value(pos[t].state)}" for t in LOCKED_TRACKS
                    )
                    print(
                        f"[HEALTH] phase={phase} exit_elapsed={int(time.monotonic()-exit_phase_started_mono)}s "
                        f"rowid={cursor} rows={rows_seen} positions={state_text} "
                        f"exit_rejections={len(exit_rejections)} closed={len(exit_closures)}/3"
                    )
                next_health = time.monotonic() + 5.0

            time.sleep(args.poll_ms / 1000)

    finally:
        try:
            prod_conn.close()
        except Exception:
            pass
        if proc.poll() is None:
            stop_mode, collector_returncode = stop_process(proc, 15)
        collector_fh.close()

    elapsed = time.monotonic() - started_mono

    paper_quick = paper_conn.execute("PRAGMA quick_check").fetchone()[0]
    positions = lifecycle_positions_for_signal() if entry_execution is not None else {}
    entry_ok = (
        entry_execution is not None
        and enum_value(entry_execution.route.state) == "FILLED"
        and len(entry_execution.orders) == 3
        and all(enum_value(o.state) == "FILLED" for o in entry_execution.orders)
    )
    all_closed = (
        set(positions) == set(LOCKED_TRACKS)
        and all(enum_value(positions[t].state) == "CLOSED" for t in LOCKED_TRACKS)
    )

    exit_routes = {}
    exit_route_checks = {}
    if exit_executor is not None and positions:
        for track_id in LOCKED_TRACKS:
            p = positions[track_id]
            route = exit_executor.get_route_for_position(p.paper_position_id)
            exit_routes[track_id] = primitive(route)
            allowed_reason = {
                "FINAL-A": {"TAKE_PROFIT", "FALLBACK"},
                "FINAL-B": {"TRAIL", "FALLBACK"},
                "SENS-C": {"TAKE_PROFIT", "FALLBACK"},
            }[track_id]
            exit_route_checks[track_id] = bool(
                route is not None
                and enum_value(route.state) == "FILLED"
                and route.exit_reason in allowed_reason
                and route.gross_exit_proceeds_lamports is not None
                and route.gross_exit_proceeds_lamports > 0
                and route.total_exit_explicit_cost_lamports is not None
                and route.total_exit_explicit_cost_lamports >= 0
                and route.selected_market_observed_at is not None
                and route.selected_market_ingest_seq is not None
                and route.price_impact_bps is not None
            )

    pass_result = bool(
        entry_ok
        and all_closed
        and exit_route_checks
        and all(exit_route_checks.values())
        and str(paper_quick).lower() == "ok"
        and not strategy_errors
    )

    audit_payload = {
        "schema_version": "phase4_4e_live_entry_exit_smoke_v0.1",
        "started_at_utc": started_at.isoformat(),
        "finished_at_utc": utc_now().isoformat(),
        "elapsed_seconds": round(elapsed, 3),
        "collector": str(COLLECTOR),
        "collector_log": str(collector_log),
        "collector_stop_mode": stop_mode,
        "collector_returncode": collector_returncode,
        "production_db": str(PRODUCTION_DB),
        "production_db_sidecar_mode": "QUERY_ONLY_READ_ONLY",
        "paper_db": str(paper_db),
        "locked_selection": str(LOCKED_SELECTION),
        "locked_roles": list(EXPECTED_ROLES),
        "locked_tracks": list(LOCKED_TRACKS),
        "cost_baseline_id": baseline.assumption_set_id,
        "cost_baseline_fingerprint": baseline.fingerprint,
        "entry_impact_model_id": entry_impact_mod.MODEL_ID,
        "entry_impact_fingerprint": entry_impact_mod.MODEL_FINGERPRINT,
        "exit_spec_fingerprint": exit_orch_mod.LOCKED_EXIT_SPEC_FINGERPRINT,
        "exit_impact_model_id": exit_impact_mod.MODEL_ID,
        "exit_impact_fingerprint": exit_impact_mod.MODEL_FINGERPRINT,
        "start_cursor_rowid": start_cursor,
        "end_cursor_rowid": cursor,
        "rows_seen": rows_seen,
        "new_launch_rows": launch_rows,
        "eligible_new_mints": len(eligible_mints),
        "normalized_events": normalized_events,
        "normalized_skips": dict(normalized_skips),
        "feature_states": feature_states,
        "strategy_evaluations": strategy_evaluations,
        "candidate_counts": dict(candidate_counts),
        "candidate_identity_counts": dict(candidate_identity_counts),
        "strategy_errors": strategy_errors,
        "entry_rejections": entry_rejections,
        "entry_role": entry_role,
        "entry_candidate": primitive(entry_candidate),
        "entry_envelope": primitive(entry_envelope),
        "entry_observation": primitive(entry_observation),
        "entry_impact": primitive(entry_impact),
        "entry_route": primitive(entry_execution.route) if entry_execution else None,
        "entry_orders": [primitive(o) for o in entry_execution.orders] if entry_execution else [],
        "entry_fill_evaluation": primitive(entry_execution.fill_evaluation) if entry_execution else None,
        "entry_explicit_costs": primitive(entry_execution.explicit_costs) if entry_execution else None,
        "exit_market_evaluations": exit_market_evaluations,
        "exit_clock_evaluations": exit_clock_evaluations,
        "exit_rejections": exit_rejections,
        "exit_closures": exit_closures,
        "exit_routes": exit_routes,
        "final_positions": {k: primitive(v) for k, v in positions.items()},
        "paper_quick_check": paper_quick,
        "pnl_accounting_performed": False,
        "wallet_signing_live_orders": False,
        "parameter_tuning_reselection": False,
        "result": "PASS" if pass_result else "CHECK",
    }
    audit_path.write_text(
        json.dumps(
            audit_payload,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )

    print()
    print("-" * 124)
    print("VALIDATION")
    print("-" * 124)
    print("Exact locked entry roles                  : PASS")
    print("Exact locked exit spec/fingerprint        : PASS")
    print("Exact entry/exit impact fingerprints      : PASS")
    print("Locked 500ms entry/exit latency           : PASS")
    print(f"New live LAUNCH observed                  : {'PASS' if launch_rows > 0 else 'CHECK'}")
    print(f"Accepted causal SOL_NATIVE entry          : {'PASS' if entry_ok else 'CHECK'}")
    print(f"All three locked tracks CLOSED            : {'PASS' if all_closed else 'CHECK'}")
    for track_id in LOCKED_TRACKS:
        print(
            f"{track_id} causal exit route FILLED"
            f"{' ' * max(1, 37-len(track_id))}: "
            f"{'PASS' if exit_route_checks.get(track_id, False) else 'CHECK'}"
        )
    print(f"Paper SQLite quick_check                  : {'PASS' if str(paper_quick).lower() == 'ok' else 'FAIL'}")
    print(f"Strategy runtime errors                   : {'PASS' if not strategy_errors else 'FAIL'}")
    print("Production DB writes by sidecar           : NO")
    print("Wallet/signing/live orders                : NO")
    print("PnL/expectancy/PF/drawdown                : NO")
    print("Parameter tuning/reselection              : NO")
    print()
    print(f"Elapsed                                   : {elapsed:.1f}s")
    print(f"Entry role                                : {entry_role}")
    print(f"Entry slippage rejections                 : {len(entry_rejections)}")
    print(f"Exit slippage rejections                  : {len(exit_rejections)}")
    if positions:
        for track_id in LOCKED_TRACKS:
            p = positions[track_id]
            route = exit_executor.get_route_for_position(p.paper_position_id)
            attempts = () if route is None else exit_executor.list_attempts(route.route_id)
            print(
                f"{track_id:<8} position={enum_value(p.state):<6} "
                f"reason={(route.exit_reason if route else None)} "
                f"attempts={len(attempts)} "
                f"impact={(route.price_impact_bps if route else None)}bps "
                f"gross={(route.gross_exit_proceeds_lamports if route else None)} "
                f"cost={(route.total_exit_explicit_cost_lamports if route else None)}"
            )
    print(f"Paper DB                                  : {paper_db}")
    print(f"Audit                                     : {audit_path}")
    print(f"Collector log                             : {collector_log}")
    print()
    print(f"RESULT: {'PASS' if pass_result else 'CHECK'}")

    if pass_result:
        print(
            "NEXT: build deterministic trade accounting / net PnL / expectancy / "
            "profit factor / drawdown and skipped/rejection audit over these persisted fills. "
            "Do not tune entries or exits."
        )
        paper_conn.close()
        return 0

    if entry_execution is None:
        print(
            "No accepted live SOL_NATIVE entry completed inside the bounded acquisition window. "
            "Re-run unchanged; do not alter locked parameters."
        )
    elif not all_closed:
        print(
            "Entry filled, but not all three exit tracks obtained accepted causal fills "
            "inside the bounded exit window. Do not fabricate exits; return this output."
        )
    else:
        print("Do not advance. Return this output for diagnosis.")

    paper_conn.close()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
