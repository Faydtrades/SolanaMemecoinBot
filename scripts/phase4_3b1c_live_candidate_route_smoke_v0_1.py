from __future__ import annotations

import argparse
import collections
import importlib
import inspect
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

NS2 = "tradingbot_local_phase2_b1c_live_route"
NS4 = "tradingbot_local_phase4_b1c_live_route"

LOCKED_BASELINE_ID = "P4-COST-BASELINE-0001"
LOCKED_BASELINE_FINGERPRINT = (
    "9652aeffc792eb48d62348360c6ae804f660e43329195d1f169219b80ccab67c"
)

EXPECTED_ROLES = ("CONTROL", "ROBUST_1", "ROBUST_2", "ROBUST_3")

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
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA busy_timeout = 1000")
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
    # Exact Phase-1 adapter row identity contract.
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
    if not LOCKED_SELECTION.exists():
        raise FileNotFoundError(f"Missing locked selection: {LOCKED_SELECTION}")

    payload = json.loads(LOCKED_SELECTION.read_text(encoding="utf-8"))
    if payload.get("experiment_id") != "EXP-0005":
        raise ValueError("Locked entry selection must be EXP-0005")
    if payload.get("status") != "LOCKED_FOR_EXIT_RESEARCH":
        raise ValueError("EXP-0005 selection is not LOCKED_FOR_EXIT_RESEARCH")

    rows = payload.get("carry_forward")
    if not isinstance(rows, list) or len(rows) != 4:
        raise ValueError("Expected exactly four locked carry-forward entry roles")

    roles = tuple(str(row.get("role")) for row in rows)
    if roles != EXPECTED_ROLES:
        raise ValueError(f"Unexpected locked role order: {roles}")

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

    # Prefer a module-level immutable baseline object.
    for name, value in vars(cost_baseline_mod).items():
        if isinstance(value, PaperCostBaseline):
            candidates.append((f"object:{name}", value))

    # If the module exposes a no-argument factory, allow it only if its
    # returned fingerprint exactly matches the already validated baseline.
    for name, value in vars(cost_baseline_mod).items():
        if not inspect.isfunction(value):
            continue
        if "baseline" not in name.lower():
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

    matching = [
        (source, baseline)
        for source, baseline in candidates
        if getattr(baseline, "assumption_set_id", None) == LOCKED_BASELINE_ID
        and getattr(baseline, "fingerprint", None) == LOCKED_BASELINE_FINGERPRINT
    ]

    if not matching:
        discovered = [
            {
                "source": source,
                "assumption_set_id": getattr(baseline, "assumption_set_id", None),
                "fingerprint": getattr(baseline, "fingerprint", None),
            }
            for source, baseline in candidates
        ]
        raise RuntimeError(
            "Could not resolve exact locked P4 cost baseline. "
            f"Discovered={discovered}"
        )

    # Multiple aliases to the exact same immutable baseline are harmless.
    source, baseline = matching[0]
    return source, baseline


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


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Phase 4.3B1C controlled live CandidateSignal -> "
            "simulated paper-entry routing smoke."
        )
    )
    parser.add_argument(
        "--max-seconds",
        type=int,
        default=600,
        help="Hard live-smoke limit. Default: 600s (10 minutes).",
    )
    parser.add_argument("--poll-ms", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    if args.max_seconds <= 0:
        raise SystemExit("--max-seconds must be > 0")
    if not COLLECTOR.exists():
        raise SystemExit(f"Missing validated collector: {COLLECTOR}")

    # Exact local bindings.
    models_mod = local_import(NS2, PHASE2_ROOT, "models_v0_1")
    quote_mod = local_import(NS2, PHASE2_ROOT, "quote_aware_price_v0_1")
    flow_mod = local_import(NS2, PHASE2_ROOT, "denomination_aware_flow_v0_1")
    strategy_mod = local_import(NS2, PHASE2_ROOT, "strategy_first_pullback_v0_2")
    clock_mod = local_import(NS2, PHASE2_ROOT, "strategy_clock_v0_1")

    cost_model_mod = local_import(NS4, PHASE4_ROOT, "paper_cost_model_v0_2")
    cost_baseline_mod = local_import(NS4, PHASE4_ROOT, "paper_cost_baseline_v0_1")
    router_mod = local_import(NS4, PHASE4_ROOT, "paper_entry_router_v0_1")

    FirstPullbackParameterSet = models_mod.FirstPullbackParameterSet
    Strategy = strategy_mod.FirstPullbackStrategyV02
    Clock = clock_mod.FirstPullbackStrategyClockV01
    QuoteAdapter = quote_mod.Phase1QuoteAwareAdapterV01
    DenomEngine = flow_mod.DenominationAwareFeatureEngineV01

    selection_payload, params_by_role = load_locked_parameter_sets(
        FirstPullbackParameterSet,
        Strategy,
    )

    baseline_source, baseline = resolve_locked_baseline(
        cost_baseline_mod,
        cost_model_mod.PaperCostBaseline,
    )
    cost_model = cost_model_mod.PaperCostModelV02(baseline)

    if baseline.reference_position_lamports != 100_000_000:
        raise RuntimeError(
            "Locked reference position drift: "
            f"{baseline.reference_position_lamports}"
        )
    if baseline.entry_latency_ms != 500:
        raise RuntimeError(
            f"Locked entry latency drift: {baseline.entry_latency_ms}"
        )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
    paper_db = OUT_DIR / f"phase4_3b1c_live_candidate_route_{stamp}.sqlite3"
    audit_path = OUT_DIR / f"phase4_3b1c_live_candidate_route_{stamp}.json"
    collector_log = OUT_DIR / f"phase4_3b1c_collector_{stamp}.log"

    paper_conn = router_mod.connect_entry_router_db(paper_db)
    router = router_mod.PaperEntryRouterV01(
        paper_conn,
        cost_model,
    )

    locked_tracks_obj = getattr(router_mod, "LOCKED_PHASE4_TRACKS", None)
    if locked_tracks_obj is None:
        raise RuntimeError("paper_entry_router_v0_1 has no LOCKED_PHASE4_TRACKS")
    locked_tracks = tuple(locked_tracks_obj)
    if len(locked_tracks) != 3:
        raise RuntimeError(
            f"Expected three locked Phase-4 paper tracks, got {locked_tracks}"
        )

    prod_conn = open_production_ro()
    try:
        start_cursor = max_rowid(prod_conn)
    finally:
        prod_conn.close()

    print("=" * 118)
    print("PHASE 4.3B1C - LIVE CANDIDATESIGNAL -> SIMULATED ENTRY ROUTING SMOKE v0.1")
    print("=" * 118)
    print(f"Project root                       : {PROJECT_ROOT}")
    print(f"Collector                          : {COLLECTOR}")
    print(f"Production DB                      : {PRODUCTION_DB}")
    print("Production DB sidecar access       : QUERY-ONLY / READ-ONLY")
    print(f"Locked entry selection             : {LOCKED_SELECTION}")
    print(f"Locked roles                       : {EXPECTED_ROLES}")
    print("Strategy                           : FirstPullback v1.1 / V02 implementation")
    print("Entry window                       : 0..300000ms inclusive; expire +1us")
    print(f"Cost baseline                      : {baseline.assumption_set_id}")
    print(f"Cost baseline fingerprint          : {baseline.fingerprint}")
    print(f"Cost baseline source               : {baseline_source}")
    print(f"Reference position                 : {baseline.reference_position_lamports} lamports (0.10 SOL)")
    print(f"Entry latency                      : {baseline.entry_latency_ms}ms")
    print(f"Locked paper tracks                : {locked_tracks}")
    print(f"Paper DB                           : {paper_db}")
    print("Paper orders                       : YES — isolated paper DB only")
    print("Paper entry fills                  : NO — not fabricated in this bounded step")
    print("Wallet/signing/live orders         : NO")
    print("Parameter tuning/reselection       : NO")
    print(f"Start cursor rowid                 : {start_cursor}")
    print(f"Hard runtime limit                 : {args.max_seconds}s")
    print("Stop condition                     : first routable live CandidateSignal")
    print()

    # Start validated collector automatically.
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

    # (mint, role) -> runtime state
    runtimes: dict[tuple[str, str], dict[str, Any]] = {}

    eligible_mints: set[str] = set()
    cursor = start_cursor

    rows_seen = 0
    launch_rows = 0
    quote_events = 0
    quote_skips: collections.Counter[str] = collections.Counter()
    states = 0
    evaluations = 0
    transitions = 0
    candidate_counts: collections.Counter[str] = collections.Counter()
    expiry_counts: collections.Counter[str] = collections.Counter()
    reason_counts: collections.Counter[str] = collections.Counter()
    strategy_errors: list[str] = []
    candidate_unroutable: list[dict[str, Any]] = []
    transition_records: list[dict[str, Any]] = []

    routed = None
    routed_role = None
    routed_candidate = None
    routed_envelope = None
    replay_digest_equal = None
    replay_order_ids_equal = None

    stop_mode = "NOT_STOPPED"
    collector_returncode = None

    started_mono = time.monotonic()
    started_at = utc_now()
    next_health = started_mono

    prod_conn = open_production_ro()

    def record_eval(role: str, evaluation: Any) -> None:
        nonlocal evaluations, transitions
        evaluations += 1
        reason_counts[str(evaluation.reason_code)] += 1

        if evaluation.transition_occurred:
            transitions += 1
            transition_records.append(
                {
                    "role": role,
                    "mint": evaluation.mint,
                    "evaluated_at_us": evaluation.evaluated_at_us,
                    "previous_state": primitive(evaluation.previous_state),
                    "current_state": primitive(evaluation.current_state),
                    "reason_code": evaluation.reason_code,
                    "filters_passed": list(evaluation.filters_passed),
                    "filters_failed": list(evaluation.filters_failed),
                }
            )

    def fire_silent_expiries() -> None:
        current_us = now_us()
        for (mint, role), rt in list(runtimes.items()):
            run = rt["run"]
            deadline = rt.get("deadline")
            params = rt["params"]

            if deadline is None or run.finished:
                continue

            evaluation = Clock.fire_if_due(
                deadline,
                run,
                params,
                now_us=current_us,
            )
            if evaluation is not None:
                record_eval(role, evaluation)
                expiry_counts[role] += 1

    try:
        while True:
            elapsed = time.monotonic() - started_mono

            if proc.poll() is not None:
                stop_mode = "EXITED_NATURALLY"
                collector_returncode = proc.returncode
                print(
                    f"[CHECK] collector exited early returncode={collector_returncode}"
                )
                break

            if elapsed >= args.max_seconds:
                break

            fire_silent_expiries()

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

                # Causality: a mint enters this smoke only at/after its own
                # newly-observed LAUNCH row.
                causal_rows: list[sqlite3.Row] = []
                for row in rows:
                    event_type = str(row["event_type"]).upper()
                    mint = row["mint"]

                    if event_type == "LAUNCH" and mint:
                        if mint not in eligible_mints:
                            launch_rows += 1
                        eligible_mints.add(str(mint))

                    if mint and str(mint) in eligible_mints:
                        causal_rows.append(row)

                if causal_rows:
                    events, skipped = QuoteAdapter.normalize_rows(causal_rows)
                    quote_events += len(events)

                    if isinstance(skipped, dict):
                        quote_skips.update(
                            {str(k): int(v) for k, v in skipped.items()}
                        )

                    for qevent in events:
                        base = qevent.base
                        mint = str(base.mint)

                        if mint not in eligible_mints:
                            continue

                        state = denom_engine.process(qevent)
                        states += 1

                        for role in EXPECTED_ROLES:
                            params = params_by_role[role]
                            key = (mint, role)

                            rt = runtimes.get(key)
                            if rt is None:
                                run = Strategy.start_run(state, params)
                                rt = {
                                    "params": params,
                                    "run": run,
                                    "deadline": None,
                                }
                                runtimes[key] = rt

                            run = rt["run"]

                            if rt["deadline"] is None:
                                rt["deadline"] = Clock.schedule_entry_expiry(
                                    state,
                                    run,
                                    params,
                                )

                            deadline = rt["deadline"]

                            # Deadline wins before any market observation strictly
                            # outside the entry window.
                            if (
                                deadline is not None
                                and state.as_of_observed_at_us >= deadline.expire_at_us
                                and not run.finished
                            ):
                                clock_eval = Clock.fire_if_due(
                                    deadline,
                                    run,
                                    params,
                                    now_us=state.as_of_observed_at_us,
                                )
                                if clock_eval is not None:
                                    record_eval(role, clock_eval)
                                    expiry_counts[role] += 1
                                    continue

                            if run.finished:
                                continue

                            try:
                                evaluation = Strategy.evaluate(
                                    state,
                                    run,
                                    params,
                                )
                            except Exception as exc:
                                strategy_errors.append(
                                    f"{role}:{mint}:{type(exc).__name__}:{exc}"
                                )
                                if len(strategy_errors) <= 5:
                                    print(
                                        f"[ERROR] strategy {strategy_errors[-1]}"
                                    )
                                continue

                            record_eval(role, evaluation)

                            candidate = evaluation.candidate_signal
                            if candidate is None:
                                continue

                            candidate_counts[role] += 1

                            # Route only using raw price identity from the exact
                            # triggering market event. Never invent a price.
                            point = point_for_event(quote_mod, qevent)

                            if point is None:
                                candidate_unroutable.append(
                                    {
                                        "role": role,
                                        "candidate_id": candidate.signal_id,
                                        "mint": mint,
                                        "reason": "TRIGGER_EVENT_HAS_NO_RAW_RATIONAL_PRICE",
                                    }
                                )
                                continue

                            if source_is_gap_recovery(qevent):
                                candidate_unroutable.append(
                                    {
                                        "role": role,
                                        "candidate_id": candidate.signal_id,
                                        "mint": mint,
                                        "reason": "GAP_RECOVERY_TRIGGER_NOT_ROUTED_LIVE",
                                    }
                                )
                                continue

                            signal_observed_at = us_to_dt(
                                int(candidate.generated_at_us)
                            )

                            envelope = router_mod.CandidateSignalEnvelope(
                                candidate_id=str(candidate.signal_id),
                                mint=mint,
                                strategy_version=str(candidate.strategy_version),
                                parameter_set_id=str(candidate.parameter_set_id),
                                signal_observed_at=signal_observed_at,
                                signal_ingest_seq=int(base.ingest_seq),
                                price_identity=str(point.identity.label),
                                price_numerator_raw=int(
                                    point.numerator_reserve_raw
                                ),
                                price_denominator_raw=int(
                                    point.token_reserve_raw
                                ),
                            )

                            first_result = router.route_candidate(envelope)

                            # Live exact-replay idempotence on the same candidate.
                            digest_before_replay = (
                                router_mod.canonical_entry_router_digest(
                                    paper_conn
                                )
                            )
                            replay_result = router.route_candidate(envelope)
                            digest_after_replay = (
                                router_mod.canonical_entry_router_digest(
                                    paper_conn
                                )
                            )

                            first_order_ids = tuple(
                                order.paper_order_id
                                for order in first_result.orders
                            )
                            replay_order_ids = tuple(
                                order.paper_order_id
                                for order in replay_result.orders
                            )

                            replay_digest_equal = (
                                digest_before_replay
                                == digest_after_replay
                            )
                            replay_order_ids_equal = (
                                first_order_ids == replay_order_ids
                            )

                            routed = first_result
                            routed_role = role
                            routed_candidate = candidate
                            routed_envelope = envelope
                            break

                        if routed is not None:
                            break

            if routed is not None:
                break

            if time.monotonic() >= next_health:
                print(
                    f"[HEALTH] elapsed={int(elapsed)}s "
                    f"rowid={cursor} rows={rows_seen} launches={launch_rows} "
                    f"mints={len(eligible_mints)} quote_events={quote_events} "
                    f"states={states} evals={evaluations} "
                    f"candidates={sum(candidate_counts.values())} "
                    f"routed={'YES' if routed else 'NO'}"
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

    route_checks: dict[str, bool] = {}

    if routed is not None:
        route = routed.route
        orders = routed.orders

        route_state = getattr(route.state, "value", str(route.state))
        order_states = tuple(
            getattr(order.state, "value", str(order.state))
            for order in orders
        )
        order_tracks = tuple(order.track_id for order in orders)
        order_ids = tuple(order.paper_order_id for order in orders)

        expected_ready = (
            routed_envelope.signal_observed_at
            + timedelta(milliseconds=baseline.entry_latency_ms)
        )

        route_checks = {
            "route_created": route is not None,
            "route_entry_pending": route_state == "ENTRY_PENDING",
            "three_locked_orders": len(orders) == 3,
            "tracks_match_router_lock": order_tracks == locked_tracks,
            "all_orders_entry_pending": all(
                state == "ENTRY_PENDING" for state in order_states
            ),
            "shared_reference_size": all(
                int(order.requested_size_lamports)
                == int(baseline.reference_position_lamports)
                for order in orders
            ),
            "execution_ready_exact_500ms": (
                route.execution_ready_at == expected_ready
            ),
            "route_parameter_matches_candidate": (
                route.parameter_set_id
                == routed_envelope.parameter_set_id
            ),
            "route_mint_matches_candidate": (
                route.mint == routed_envelope.mint
            ),
            "replay_digest_unchanged": bool(replay_digest_equal),
            "replay_order_ids_unchanged": bool(replay_order_ids_equal),
        }
    else:
        route_state = None
        order_states = ()
        order_tracks = ()
        order_ids = ()

    quick_check = paper_conn.execute("PRAGMA quick_check").fetchone()[0]
    paper_db_ok = quick_check == "ok"

    all_route_checks = bool(route_checks) and all(route_checks.values())

    pass_result = (
        routed is not None
        and all_route_checks
        and paper_db_ok
        and len(strategy_errors) == 0
    )

    audit_payload = {
        "schema_version": "phase4_3b1c_live_candidate_route_smoke_v0.1",
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
        "locked_selection_experiment_id": selection_payload.get(
            "experiment_id"
        ),
        "locked_roles": list(EXPECTED_ROLES),
        "parameter_set_ids": {
            role: params_by_role[role].parameter_set_id
            for role in EXPECTED_ROLES
        },
        "cost_baseline": primitive(baseline),
        "cost_baseline_fingerprint": baseline.fingerprint,
        "locked_tracks": list(locked_tracks),
        "start_cursor_rowid": start_cursor,
        "end_cursor_rowid": cursor,
        "rows_seen": rows_seen,
        "new_launch_rows": launch_rows,
        "eligible_new_mints": len(eligible_mints),
        "quote_events": quote_events,
        "quote_skips": dict(quote_skips),
        "denomination_states": states,
        "strategy_evaluations": evaluations,
        "strategy_transitions": transitions,
        "candidate_counts_by_role": dict(candidate_counts),
        "expiry_counts_by_role": dict(expiry_counts),
        "reason_counts": dict(reason_counts),
        "strategy_errors": strategy_errors,
        "candidate_unroutable": candidate_unroutable,
        "transition_records": transition_records,
        "routed_role": routed_role,
        "candidate_signal": (
            primitive(routed_candidate)
            if routed_candidate is not None
            else None
        ),
        "candidate_envelope": (
            primitive(routed_envelope)
            if routed_envelope is not None
            else None
        ),
        "route": (
            primitive(routed.route)
            if routed is not None
            else None
        ),
        "orders": (
            [primitive(order) for order in routed.orders]
            if routed is not None
            else []
        ),
        "route_checks": route_checks,
        "paper_db_quick_check": quick_check,
        "wallet_signing_live_orders": False,
        "paper_entry_fill_attempted": False,
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
    print("-" * 118)
    print("VALIDATION")
    print("-" * 118)
    print("Exact EXP-0005 locked entry roles       : PASS")
    print("FirstPullback v1.1 parameters           : PASS")
    print("P4-COST-BASELINE-0001 fingerprint       : PASS")
    print(f"New live LAUNCH observed                : {'PASS' if launch_rows > 0 else 'CHECK'}")
    print(f"Quote-aware live events                 : {'PASS' if quote_events > 0 else 'CHECK'}")
    print(f"Denomination-aware states               : {'PASS' if states > 0 else 'CHECK'}")
    print(f"Strategy runtime errors                 : {'PASS' if not strategy_errors else 'FAIL'}")
    print(f"Live CandidateSignal observed           : {'PASS' if candidate_counts else 'CHECK'}")
    print(f"Routable CandidateSignal                : {'PASS' if routed is not None else 'CHECK'}")

    if route_checks:
        for label, ok in route_checks.items():
            print(f"{label:<40}: {'PASS' if ok else 'FAIL'}")

    print(f"Paper SQLite quick_check                : {'PASS' if paper_db_ok else 'FAIL'}")
    print("Production DB writes by sidecar         : NO")
    print("Wallet/signing/live orders              : NO")
    print("Paper entry fill attempted              : NO")
    print("Parameter search/reselection            : NO")
    print()
    print(f"Elapsed                                 : {elapsed:.1f}s")
    print(f"New launch rows                         : {launch_rows}")
    print(f"Eligible new mints                      : {len(eligible_mints)}")
    print(f"Quote-aware events                      : {quote_events}")
    print(f"Denomination states                     : {states}")
    print(f"Strategy evaluations                    : {evaluations}")
    print(f"Candidate counts by role                : {dict(candidate_counts)}")
    print(f"Routed role                             : {routed_role}")
    print(f"Route state                             : {route_state}")
    print(f"Order tracks                            : {order_tracks}")
    print(f"Order states                            : {order_states}")
    print(f"Paper DB                                : {paper_db}")
    print(f"Audit                                   : {audit_path}")
    print(f"Collector log                           : {collector_log}")
    print()
    print(f"RESULT: {'PASS' if pass_result else 'CHECK'}")

    if pass_result:
        print(
            "NEXT: bounded causal entry-fill integration using post-signal "
            "market observations and runtime price-impact input; no invented fill."
        )
        paper_conn.close()
        return 0

    if routed is None and not strategy_errors:
        print(
            "No routable CandidateSignal appeared inside this bounded live window. "
            "This is a CHECK, not a strategy failure. Re-run the same locked smoke "
            "rather than changing parameters."
        )
    else:
        print(
            "Do not advance to causal entry fill. Return this output for diagnosis."
        )

    paper_conn.close()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
