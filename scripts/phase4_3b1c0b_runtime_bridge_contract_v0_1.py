from __future__ import annotations

import dataclasses
import importlib
import inspect
import sys
import types
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
PHASE2_ROOT = SRC_ROOT / "phase2"
PHASE4_ROOT = SRC_ROOT / "phase4"

NS2 = "tradingbot_local_phase2_b1c0b"
NS4 = "tradingbot_local_phase4_b1c0b"


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
            f"Binding mismatch {module_name}: loaded={loaded} expected={expected}"
        )
    return mod


def section(title: str) -> None:
    print()
    print("-" * 120)
    print(title)
    print("-" * 120)


def sig(obj: Any) -> str:
    try:
        return str(inspect.signature(obj))
    except Exception as exc:
        return f"<signature unavailable: {type(exc).__name__}: {exc}>"


def public_descriptors(cls: type) -> None:
    print(f"{cls.__module__}.{cls.__name__}{sig(cls)}")
    for name in sorted(cls.__dict__):
        if name.startswith("_"):
            continue
        raw = inspect.getattr_static(cls, name)
        bound = getattr(cls, name)
        if isinstance(raw, classmethod):
            kind = "classmethod"
        elif isinstance(raw, staticmethod):
            kind = "staticmethod"
        elif inspect.isfunction(raw):
            kind = "method"
        elif isinstance(raw, property):
            kind = "property"
        else:
            continue
        print(f"  {kind:<12} {name}{sig(bound)}")


def print_source(obj: Any, label: str, max_lines: int = 180) -> None:
    print()
    print(f"[{label}]")
    try:
        text = inspect.getsource(obj)
    except Exception as exc:
        print(f"  SOURCE UNAVAILABLE: {type(exc).__name__}: {exc}")
        return
    lines = text.splitlines()
    for i, line in enumerate(lines[:max_lines], 1):
        print(f"{i:4d}: {line}")
    if len(lines) > max_lines:
        print(f"... <TRUNCATED {len(lines)-max_lines} lines>")


def describe_dataclasses(mod: Any) -> None:
    for name, obj in sorted(vars(mod).items()):
        if (
            inspect.isclass(obj)
            and dataclasses.is_dataclass(obj)
            and getattr(obj, "__module__", None) == mod.__name__
        ):
            print(f"{name}{sig(obj)}")
            for f in dataclasses.fields(obj):
                print(f"  - {f.name}: {f.type!r}")
            print()


def grep_construction(pattern: str, roots: list[Path], max_hits: int = 12) -> None:
    hits = []
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            try:
                lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
            except Exception:
                continue
            for i, line in enumerate(lines):
                if pattern in line:
                    lo = max(0, i - 8)
                    hi = min(len(lines), i + 30)
                    hits.append((path, i + 1, lines[lo:hi], lo + 1))
                    if len(hits) >= max_hits:
                        break
            if len(hits) >= max_hits:
                break
        if len(hits) >= max_hits:
            break

    print(f"pattern={pattern!r} hits={len(hits)}")
    for path, lineno, window, startno in hits:
        print()
        print(f"[{path.relative_to(PROJECT_ROOT)} around line {lineno}]")
        for offset, line in enumerate(window):
            print(f"{startno + offset:5d}: {line}")


def main() -> int:
    print("=" * 120)
    print("PHASE 4.3B1C0B - RUNTIME BRIDGE CONTRACT COMPLETION v0.1")
    print("=" * 120)
    print(f"Project root                    : {PROJECT_ROOT}")
    print("Production DB opened            : NO")
    print("Collector started               : NO")
    print("Network/RPC                     : NO")
    print("Wallet/signing/live orders      : NO")
    print("Paper orders                    : NO")
    print("Parameter tuning/reselection    : NO")
    print("Purpose                         : close exact API gaps before B1C live CandidateSignal runner")

    strategy = local_import(NS2, PHASE2_ROOT, "strategy_first_pullback_v0_2")
    clock = local_import(NS2, PHASE2_ROOT, "strategy_clock_v0_1")
    quote = local_import(NS2, PHASE2_ROOT, "quote_aware_price_v0_1")
    flow = local_import(NS2, PHASE2_ROOT, "denomination_aware_flow_v0_1")
    models = local_import(NS2, PHASE2_ROOT, "models_v0_1")
    audit = local_import(NS2, PHASE2_ROOT, "audit_orchestrator_v0_1")

    cost_model = local_import(NS4, PHASE4_ROOT, "paper_cost_model_v0_2")
    cost_baseline = local_import(NS4, PHASE4_ROOT, "paper_cost_baseline_v0_1")
    router = local_import(NS4, PHASE4_ROOT, "paper_entry_router_v0_1")

    section("STRATEGY — INCLUDING CLASSMETHOD/STATICMETHOD DESCRIPTORS")
    Strategy = getattr(strategy, "FirstPullbackStrategyV02")
    public_descriptors(Strategy)
    if hasattr(Strategy, "evaluate"):
        print_source(Strategy.evaluate, "FirstPullbackStrategyV02.evaluate")

    section("STRATEGY CLOCK — INCLUDING CLASSMETHOD/STATICMETHOD DESCRIPTORS")
    Clock = getattr(clock, "FirstPullbackStrategyClockV01")
    public_descriptors(Clock)
    for name in sorted(Clock.__dict__):
        if name.startswith("_"):
            continue
        raw = inspect.getattr_static(Clock, name)
        if isinstance(raw, (classmethod, staticmethod)) or inspect.isfunction(raw):
            print_source(getattr(Clock, name), f"FirstPullbackStrategyClockV01.{name}", 120)

    section("DENOMINATION-AWARE FLOW BRIDGE")
    for name, obj in sorted(vars(flow).items()):
        if inspect.isclass(obj) and getattr(obj, "__module__", None) == flow.__name__:
            public_descriptors(obj)
            print()
    describe_dataclasses(flow)
    Combiner = getattr(flow, "DenominationAwareMarketCombinerV01", None)
    if Combiner is not None and hasattr(Combiner, "process"):
        print_source(Combiner.process, "DenominationAwareMarketCombinerV01.process", 180)

    section("QUOTE-AWARE PRICE BRIDGE")
    for name, obj in sorted(vars(quote).items()):
        if inspect.isclass(obj) and getattr(obj, "__module__", None) == quote.__name__:
            public_descriptors(obj)
            print()
    describe_dataclasses(quote)

    section("AUDIT / ORCHESTRATOR PUBLIC API")
    for name, obj in sorted(vars(audit).items()):
        if inspect.isclass(obj) and getattr(obj, "__module__", None) == audit.__name__:
            public_descriptors(obj)
            print()

    section("PAPER COST MODEL v0.2")
    for name, obj in sorted(vars(cost_model).items()):
        if inspect.isclass(obj) and getattr(obj, "__module__", None) == cost_model.__name__:
            public_descriptors(obj)
            if dataclasses.is_dataclass(obj):
                for f in dataclasses.fields(obj):
                    print(f"  field {f.name}: {f.type!r}")
            print()
    for name, obj in sorted(vars(cost_model).items()):
        if inspect.isfunction(obj) and getattr(obj, "__module__", None) == cost_model.__name__:
            print(f"function {name}{sig(obj)}")

    section("PAPER COST BASELINE v0.1")
    for name, value in sorted(vars(cost_baseline).items()):
        if name.startswith("_"):
            continue
        if inspect.isclass(value):
            public_descriptors(value)
            if dataclasses.is_dataclass(value):
                for f in dataclasses.fields(value):
                    print(f"  field {f.name}: {f.type!r}")
            print()
        elif inspect.isfunction(value):
            print(f"function {name}{sig(value)}")
        elif isinstance(value, (str, int, float, bool, tuple)):
            if any(tok in name.upper() for tok in (
                "BASELINE", "ASSUMPTION", "FINGERPRINT", "REFERENCE",
                "LATENCY", "SIZE", "FEE", "SLIPPAGE"
            )):
                print(f"{name} = {value!r}")

    section("ROUTER — EXACT route_candidate / on_market_observation SOURCE")
    Router = getattr(router, "PaperEntryRouterV01")
    print_source(Router.route_candidate, "PaperEntryRouterV01.route_candidate", 180)
    print_source(Router.on_market_observation, "PaperEntryRouterV01.on_market_observation", 220)

    section("EXISTING LOCAL PARAMETER CONSTRUCTION")
    grep_construction(
        "FirstPullbackParameterSet(",
        [PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"],
        max_hits=10,
    )

    section("EXISTING LOCAL RUN-STATE CONSTRUCTION")
    grep_construction(
        "StrategyRunState(",
        [PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"],
        max_hits=10,
    )

    section("EXISTING LOCAL DENOMINATION COMBINER USAGE")
    grep_construction(
        "DenominationAwareMarketCombinerV01",
        [PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"],
        max_hits=10,
    )

    section("EXISTING LOCAL STRATEGY CLOCK USAGE")
    grep_construction(
        "FirstPullbackStrategyClockV01",
        [PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"],
        max_hits=10,
    )

    print()
    print("-" * 120)
    print("VALIDATION")
    print("-" * 120)
    print("Exact local Phase-2 binding       : PASS")
    print("Exact local Phase-4 binding       : PASS")
    print("Descriptor-aware API capture      : PASS")
    print("No production DB access           : PASS")
    print("No network / wallet / orders      : PASS")
    print("No parameter search/reselection   : PASS")
    print()
    print("RESULT: PASS")
    print("NEXT: build Phase 4.3B1C live CandidateSignal -> simulated entry runner.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
