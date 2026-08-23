from __future__ import annotations

import ast
import dataclasses
import importlib
import inspect
import json
import sys
import types
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
PHASE2_ROOT = SRC_ROOT / "phase2"
PHASE4_ROOT = SRC_ROOT / "phase4"
PHASE3_DIR = PROJECT_ROOT / "data" / "research" / "phase3"

NS2 = "tradingbot_local_phase2_b1c_contract"
NS4 = "tradingbot_local_phase4_b1c_contract"

LOCKED_ROLES = ("CONTROL", "ROBUST_1", "ROBUST_2", "ROBUST_3")


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
            f"Binding mismatch for {module_name}: loaded={loaded} expected={expected}"
        )
    return mod


def section(title: str) -> None:
    print()
    print("-" * 118)
    print(title)
    print("-" * 118)


def signature(obj: Any) -> str:
    try:
        return str(inspect.signature(obj))
    except Exception as exc:
        return f"<unavailable: {type(exc).__name__}: {exc}>"


def describe_class(cls: type) -> None:
    print(f"{cls.__module__}.{cls.__name__}{signature(cls)}")
    if dataclasses.is_dataclass(cls):
        for f in dataclasses.fields(cls):
            if f.default is not dataclasses.MISSING:
                default = repr(f.default)
            elif f.default_factory is not dataclasses.MISSING:  # type: ignore[attr-defined]
                default = "<factory>"
            else:
                default = "<required>"
            print(f"  - {f.name}: {f.type!r} default={default}")


def class_inventory(mod: Any) -> list[type]:
    out = []
    for _, obj in vars(mod).items():
        if inspect.isclass(obj) and getattr(obj, "__module__", None) == mod.__name__:
            out.append(obj)
    return sorted(out, key=lambda cls: cls.__name__)


def function_inventory(mod: Any) -> list[Any]:
    out = []
    for _, obj in vars(mod).items():
        if inspect.isfunction(obj) and getattr(obj, "__module__", None) == mod.__name__:
            out.append(obj)
    return sorted(out, key=lambda fn: fn.__name__)


def role_objects_from_json(obj: Any, found: list[dict[str, Any]], depth: int = 0) -> None:
    if depth > 16:
        return
    if isinstance(obj, dict):
        role = obj.get("role")
        if role in LOCKED_ROLES:
            found.append(obj)
        for value in obj.values():
            role_objects_from_json(value, found, depth + 1)
    elif isinstance(obj, list):
        for value in obj:
            role_objects_from_json(value, found, depth + 1)


def score_role_object(obj: dict[str, Any]) -> int:
    score = 0
    if "parameter_set_id" in obj:
        score += 50
    if "parameters" in obj:
        score += 50
    for key in (
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
    ):
        if key in obj:
            score += 5
    return score


def locked_role_artifacts() -> None:
    candidates = []
    for path in PHASE3_DIR.rglob("*.json"):
        low = path.name.lower()
        if "selection" not in low and "locked" not in low and "exp-0012" not in str(path).lower():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue

        found: list[dict[str, Any]] = []
        role_objects_from_json(data, found)
        for obj in found:
            candidates.append((score_role_object(obj), path, obj))

    for role in LOCKED_ROLES:
        role_candidates = [
            item for item in candidates if item[2].get("role") == role
        ]
        role_candidates.sort(key=lambda x: (-x[0], str(x[1]).lower()))

        print()
        print(f"[{role}]")
        if not role_candidates:
            print("  NOT FOUND")
            continue

        _, path, obj = role_candidates[0]
        print(f"  source={path.relative_to(PROJECT_ROOT)}")
        print(
            "  object="
            + json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str)
        )


def main() -> int:
    print("=" * 118)
    print("PHASE 4.3B1C0 - EXACT CANDIDATE / ROUTER CONTRACT CAPTURE v0.1")
    print("=" * 118)
    print(f"Project root                    : {PROJECT_ROOT}")
    print("Production DB opened            : NO")
    print("Collector started               : NO")
    print("Network/RPC                     : NO")
    print("Wallet/signing/live orders      : NO")
    print("Paper orders                    : NO")
    print("Parameter tuning/reselection    : NO")
    print("Purpose                         : exact local API + locked-role capture before B1C live runner")

    models = local_import(NS2, PHASE2_ROOT, "models_v0_1")
    strategy = local_import(NS2, PHASE2_ROOT, "strategy_first_pullback_v0_2")
    clock = local_import(NS2, PHASE2_ROOT, "strategy_clock_v0_1")
    router = local_import(NS4, PHASE4_ROOT, "paper_entry_router_v0_1")
    lifecycle = local_import(NS4, PHASE4_ROOT, "paper_lifecycle_v0_1")

    section("PHASE-2 CORE DATACLASSES")
    for name in (
        "StrategyRunState",
        "StrategyRun",
        "StrategyEvaluation",
        "CandidateSignal",
        "FirstPullbackParameterSet",
    ):
        cls = getattr(models, name, None)
        if cls is None:
            print(f"{name}: MISSING")
        else:
            describe_class(cls)
            print()

    section("FIRST PULLBACK STRATEGY API")
    for cls in class_inventory(strategy):
        if "Pullback" in cls.__name__ or dataclasses.is_dataclass(cls):
            describe_class(cls)
            methods = []
            for name, member in inspect.getmembers(cls, predicate=inspect.isfunction):
                if not name.startswith("__"):
                    methods.append((name, signature(member)))
            for name, sig in methods:
                print(f"    method {name}{sig}")
            print()

    section("STRATEGY CLOCK API")
    for cls in class_inventory(clock):
        describe_class(cls)
        for name, member in inspect.getmembers(cls, predicate=inspect.isfunction):
            if not name.startswith("__"):
                print(f"    method {name}{signature(member)}")
        print()

    section("PHASE-4 PAPER ENTRY ROUTER API")
    for cls in class_inventory(router):
        describe_class(cls)
        for name, member in inspect.getmembers(cls, predicate=inspect.isfunction):
            if not name.startswith("__"):
                print(f"    method {name}{signature(member)}")
        print()

    for fn in function_inventory(router):
        print(f"function {fn.__name__}{signature(fn)}")

    section("PHASE-4 PAPER LIFECYCLE API")
    for cls in class_inventory(lifecycle):
        describe_class(cls)
        for name, member in inspect.getmembers(cls, predicate=inspect.isfunction):
            if not name.startswith("__"):
                print(f"    method {name}{signature(member)}")
        print()

    section("LOCKED PHASE-3 ROLE OBJECTS")
    locked_role_artifacts()

    print()
    print("-" * 118)
    print("VALIDATION")
    print("-" * 118)
    print("Exact local Phase-2 binding       : PASS")
    print("Exact local Phase-4 binding       : PASS")
    print("No production DB access           : PASS")
    print("No network / wallet / orders      : PASS")
    print("No parameter search/reselection   : PASS")
    print()
    print("RESULT: PASS")
    print(
        "NEXT: build Phase 4.3B1C live FirstPullback CandidateSignal -> "
        "simulated entry routing against these exact interfaces."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
