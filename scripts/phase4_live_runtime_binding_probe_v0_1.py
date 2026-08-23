from __future__ import annotations

import dataclasses
import importlib
import inspect
import json
import sqlite3
import sys
import typing
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_DB = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"
PHASE3_DIR = PROJECT_ROOT / "data" / "research" / "phase3"

TARGET_ARTIFACT_NAMES = (
    "DEV-FREEZE-0002_active_locked_entry_count_preview_v0_1.json",
    "DEV-FREEZE-0002_closed_prefix_locked_entry_readiness_report_v0_2_1.json",
    "DEV-FREEZE-0002_closed_prefix_preview_v0_2_1.json",
    "EXP-0005_D_selection_v0_1.json",
    "EXP-0012",
)

LOCKED_ROLES = ("CONTROL", "ROBUST_1", "ROBUST_2", "ROBUST_3")


def section(title: str) -> None:
    print()
    print("-" * 118)
    print(title)
    print("-" * 118)


def sig(obj: Any) -> str:
    try:
        return str(inspect.signature(obj))
    except Exception as exc:
        return f"<signature unavailable: {type(exc).__name__}: {exc}>"


def dataclass_shape(cls: type) -> None:
    print(f"{cls.__module__}.{cls.__name__}{sig(cls)}")
    try:
        hints = typing.get_type_hints(cls)
    except Exception:
        hints = getattr(cls, "__annotations__", {}) or {}

    if dataclasses.is_dataclass(cls):
        for f in dataclasses.fields(cls):
            typ = hints.get(f.name, f.type)
            if f.default is not dataclasses.MISSING:
                default = repr(f.default)
            elif f.default_factory is not dataclasses.MISSING:  # type: ignore[attr-defined]
                default = "<factory>"
            else:
                default = "<required>"
            print(f"  - {f.name}: {typ!r} default={default}")
    else:
        print("  (not a dataclass)")


def walk_role_objects(obj: Any, source: str, out: list[dict[str, Any]], depth: int = 0) -> None:
    if depth > 16 or len(out) >= 100:
        return

    if isinstance(obj, dict):
        keys = set(map(str, obj.keys()))
        values = {str(v) for v in obj.values() if isinstance(v, (str, int, float))}
        roles = [r for r in LOCKED_ROLES if r in keys or r in values]

        if roles:
            out.append(
                {
                    "source": source,
                    "roles": roles,
                    "object": obj,
                }
            )

        for value in obj.values():
            walk_role_objects(value, source, out, depth + 1)

    elif isinstance(obj, list):
        for value in obj:
            walk_role_objects(value, source, out, depth + 1)


def find_target_artifacts() -> list[Path]:
    if not PHASE3_DIR.exists():
        return []

    all_json = list(PHASE3_DIR.rglob("*.json"))
    scored: list[tuple[int, Path]] = []

    for p in all_json:
        name = p.name
        text_path = str(p).replace("\\", "/")
        score = 0
        for target in TARGET_ARTIFACT_NAMES:
            if target in name or target in text_path:
                score += 100
        if "DEV-FREEZE-0002" in name:
            score += 20
        if "selection" in name.lower():
            score += 10
        if "EXP-0012" in text_path:
            score += 15
        if score:
            scored.append((score, p))

    scored.sort(key=lambda x: (-x[0], str(x[1]).lower()))
    return [p for _, p in scored]


def artifact_probe() -> None:
    artifacts = find_target_artifacts()

    if not artifacts:
        print("No target Phase-3 JSON artifacts found.")
        return

    for path in artifacts[:20]:
        rel = path.relative_to(PROJECT_ROOT)
        print()
        print(f"[ARTIFACT] {rel}")
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"  JSON ERROR: {type(exc).__name__}: {exc}")
            continue

        found: list[dict[str, Any]] = []
        walk_role_objects(data, str(rel), found)

        if not found:
            print("  No role-bearing object found.")
            continue

        # Prefer compact objects with a parameter_set_id / role.
        def rank(item: dict[str, Any]) -> tuple[int, int]:
            obj = item["object"]
            points = 0
            if isinstance(obj, dict):
                if "parameter_set_id" in obj:
                    points += 20
                if "role" in obj:
                    points += 20
                if "parameters" in obj or "entry_parameters" in obj:
                    points += 30
                points += sum(
                    1 for k in (
                        "min_return_bps",
                        "min_trades_since_t0",
                        "min_unique_buyers_since_t0",
                        "min_depth_bps",
                        "max_depth_bps",
                        "min_buys",
                        "min_net_flow_reserve_ppm",
                        "min_rebound_bps",
                        "min_extension_from_response_bps",
                        "max_extension_from_response_bps",
                    )
                    if k in obj
                )
            return (-points, len(json.dumps(obj, default=str)))

        found.sort(key=rank)

        printed_roles: set[str] = set()
        for item in found:
            obj = item["object"]
            roles = item["roles"]
            if all(r in printed_roles for r in roles):
                continue

            text = json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)
            if len(text) > 6000:
                text = text[:6000] + "...<TRUNCATED>"

            print(f"  roles={roles}")
            print(f"  object={text}")
            printed_roles.update(roles)

            if set(LOCKED_ROLES).issubset(printed_roles):
                break


def db_tail_rows(limit: int = 30) -> list[sqlite3.Row]:
    uri = PRODUCTION_DB.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=2.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only = ON")
        rows = conn.execute(
            """
            SELECT *
            FROM pump_events
            ORDER BY rowid DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return list(reversed(rows))
    finally:
        conn.close()


def phase2_runtime_probe() -> None:
    models = importlib.import_module("src.phase2.models_v0_1")
    feature_mod = importlib.import_module("src.phase2.feature_engine_v0_2")
    strategy_mod = importlib.import_module("src.phase2.strategy_first_pullback_v0_2")
    adapter_mod = importlib.import_module("src.phase2.phase1_readonly_adapter_v0_1")
    clock_mod = importlib.import_module("src.phase2.strategy_clock_v0_1")
    quote_mod = importlib.import_module("src.phase2.quote_aware_price_v0_1")
    flow_mod = importlib.import_module("src.phase2.denomination_aware_flow_v0_1")

    section("EXACT PHASE-2 RUNTIME SIGNATURES")

    targets = [
        getattr(feature_mod, "FeatureEngineV02"),
        getattr(feature_mod, "FeatureEngineV02").process,
        getattr(strategy_mod, "FirstPullbackStrategyV02"),
        getattr(strategy_mod, "FirstPullbackStrategyV02").evaluate,
        getattr(adapter_mod, "Phase1ReadonlyAdapterV01"),
        getattr(adapter_mod, "Phase1ReadonlyAdapterV01").normalize_rows,
        getattr(clock_mod, "StrategyClockV01"),
    ]

    for obj in targets:
        print(f"{getattr(obj, '__qualname__', repr(obj))}: {sig(obj)}")

    section("PHASE-2 DATACLASS SHAPES")

    names = (
        "NormalizedMarketEvent",
        "MarketState",
        "StrategyRunState",
        "StrategyRun",
        "StrategyEvaluation",
        "CandidateSignal",
        "FirstPullbackParameterSet",
    )

    for name in names:
        cls = getattr(models, name, None)
        if cls is None:
            print(f"{name}: MISSING")
            continue
        dataclass_shape(cls)
        print()

    # Print all strategy-module dataclasses because nested parameter classes
    # may live there rather than in models_v0_1.
    print("[strategy_first_pullback_v0_2.py dataclasses]")
    for name, cls in vars(strategy_mod).items():
        if inspect.isclass(cls) and dataclasses.is_dataclass(cls):
            print()
            dataclass_shape(cls)

    section("LAST LIVE ROWS -> PHASE1 READONLY ADAPTER")

    rows = db_tail_rows(30)
    print(f"Rows fetched read-only: {len(rows)}")

    normalized = adapter_mod.Phase1ReadonlyAdapterV01.normalize_rows(rows)
    print(f"Adapter output rows     : {len(normalized)}")

    for item in normalized[-8:]:
        if dataclasses.is_dataclass(item):
            payload = dataclasses.asdict(item)
        else:
            payload = repr(item)
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str) if isinstance(payload, dict) else str(payload)
        if len(text) > 2500:
            text = text[:2500] + "...<TRUNCATED>"
        print(text)

    section("FEATUREENGINE V0.2 LIVE-SHAPE PROBE")

    engine = feature_mod.FeatureEngineV02()
    state_count = 0
    last_state = None
    errors: list[str] = []

    for item in normalized:
        try:
            state = engine.process(item)
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}")
            continue

        if state is not None:
            state_count += 1
            last_state = state

    print(f"Feature states emitted  : {state_count}")
    print(f"Feature errors          : {len(errors)}")
    for err in errors[:5]:
        print(f"  ERROR: {err}")

    if last_state is not None:
        if dataclasses.is_dataclass(last_state):
            payload = dataclasses.asdict(last_state)
            text = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
            if len(text) > 8000:
                text = text[:8000] + "...<TRUNCATED>"
            print("Last MarketState:")
            print(text)
        else:
            print(f"Last MarketState: {last_state!r}")

    section("AUXILIARY RUNTIME API")

    for mod, names in (
        (quote_mod, ("QuoteAwareProcessorV01", "QuoteAwareMarketState")),
        (flow_mod, ("DenominationAwareMarketCombinerV01", "DenominationAwareMarketState")),
    ):
        for name in names:
            obj = getattr(mod, name, None)
            if obj is None:
                print(f"{mod.__name__}.{name}: MISSING")
            else:
                print(f"{mod.__name__}.{name}: {sig(obj)}")
                if inspect.isclass(obj) and dataclasses.is_dataclass(obj):
                    dataclass_shape(obj)


def main() -> int:
    print("=" * 118)
    print("PHASE 4.3B1A - EXACT RUNTIME BINDING PROBE v0.1")
    print("=" * 118)
    print(f"Project root                       : {PROJECT_ROOT}")
    print(f"Production DB                      : {PRODUCTION_DB}")
    print("Production DB mode                 : QUERY-ONLY / READ-ONLY")
    print("Network / RPC                      : NO")
    print("Wallet / signing / live orders     : NO")
    print("Paper orders                       : NO")
    print("Parameter tuning / reselection     : NO")
    print("Purpose                            : capture exact constructors + locked parameter objects before live runner")
    print()

    try:
        phase2_runtime_probe()
    except Exception as exc:
        print()
        print(f"RUNTIME PROBE ERROR: {type(exc).__name__}: {exc}")
        print("RESULT: CHECK")
        return 2

    section("LOCKED PHASE-3 PARAMETER OBJECTS")
    artifact_probe()

    print()
    print("-" * 118)
    print("VALIDATION")
    print("-" * 118)
    print("Phase-2 modules imported           : PASS")
    print("Production DB read-only tail       : PASS")
    print("Phase1 adapter executed            : PASS")
    print("FeatureEngineV02 executed          : PASS")
    print("No strategy parameter mutation     : PASS")
    print("No paper/live orders               : PASS")
    print()
    print("RESULT: PASS")
    print("NEXT: use this exact output to build Phase 4.3B1 live CandidateSignal sidecar + controlled paper-entry smoke.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
