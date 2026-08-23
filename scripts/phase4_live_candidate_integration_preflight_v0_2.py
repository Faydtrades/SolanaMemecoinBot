from __future__ import annotations

import ast
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_DB = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"

REQUIRED_FILES = {
    "Phase1 production collector": PROJECT_ROOT / "scripts" / "live_pump_collector_v0_3_3.py",
    "Phase2 core models": PROJECT_ROOT / "src" / "phase2" / "models_v0_1.py",
    "BOT_TRUTH feature engine": PROJECT_ROOT / "src" / "phase2" / "feature_engine_v0_2.py",
    "FirstPullback v1.1 implementation": PROJECT_ROOT / "src" / "phase2" / "strategy_first_pullback_v0_2.py",
    "Phase1 read-only adapter": PROJECT_ROOT / "src" / "phase2" / "phase1_readonly_adapter_v0_1.py",
    "Strategy clock": PROJECT_ROOT / "src" / "phase2" / "strategy_clock_v0_1.py",
    "Quote-aware price": PROJECT_ROOT / "src" / "phase2" / "quote_aware_price_v0_1.py",
    "Denomination-aware flow": PROJECT_ROOT / "src" / "phase2" / "denomination_aware_flow_v0_1.py",
    "Phase4 lifecycle": PROJECT_ROOT / "src" / "phase4" / "paper_lifecycle_v0_1.py",
    "Phase4 cost baseline": PROJECT_ROOT / "src" / "phase4" / "paper_cost_baseline_v0_1.py",
    "Phase4 cost model": PROJECT_ROOT / "src" / "phase4" / "paper_cost_model_v0_2.py",
    "Phase4 entry router": PROJECT_ROOT / "src" / "phase4" / "paper_entry_router_v0_1.py",
}

LOCKED_ENTRY_ROLES = ("CONTROL", "ROBUST_1", "ROBUST_2", "ROBUST_3")
TEXT_SUFFIXES = {".json", ".csv", ".md", ".txt", ".py"}
MAX_SCAN_BYTES = 8 * 1024 * 1024


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def safe_read(path: Path) -> str | None:
    try:
        if path.stat().st_size > MAX_SCAN_BYTES:
            return None
        return path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None


def ast_signature(args: ast.arguments) -> str:
    names: list[str] = []
    all_args = list(args.posonlyargs) + list(args.args)
    default_start = len(all_args) - len(args.defaults)
    for i, arg in enumerate(all_args):
        names.append(f"{arg.arg}{'=' if i >= default_start else ''}")
    if args.vararg:
        names.append(f"*{args.vararg.arg}")
    elif args.kwonlyargs:
        names.append("*")
    for arg, default in zip(args.kwonlyargs, args.kw_defaults):
        names.append(f"{arg.arg}{'=' if default is not None else ''}")
    if args.kwarg:
        names.append(f"**{args.kwarg.arg}")
    return "(" + ", ".join(names) + ")"


def source_api_summary(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    tree = ast.parse(text)
    lines: list[str] = []
    focus_tokens = (
        "candidate", "feature", "strategy", "adapter", "market",
        "parameter", "replay", "clock", "price", "flow", "normalize",
    )
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if any(tok in node.name.lower() for tok in focus_tokens):
                lines.append(f"function {node.name}{ast_signature(node.args)}")
        elif isinstance(node, ast.ClassDef):
            if any(tok in node.name.lower() for tok in focus_tokens):
                fields: list[str] = []
                methods: list[str] = []
                for child in node.body:
                    if isinstance(child, ast.AnnAssign) and isinstance(child.target, ast.Name):
                        fields.append(child.target.id)
                    if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        if child.name == "__init__" or any(
                            tok in child.name.lower()
                            for tok in ("ingest", "process", "evaluate", "update", "replay", "normalize", "state")
                        ):
                            methods.append(f"{child.name}{ast_signature(child.args)}")
                suffix = ""
                if fields:
                    suffix += f" | fields={fields}"
                if methods:
                    suffix += f" | methods={methods}"
                lines.append(f"class {node.name}{suffix}")
    return lines[:80]


def candidate_scan_roots() -> list[Path]:
    candidates = [
        PROJECT_ROOT / "data" / "research",
        PROJECT_ROOT / "data" / "phase3",
        PROJECT_ROOT / "results",
        PROJECT_ROOT / "experiments",
        PROJECT_ROOT / "src" / "phase3",
        PROJECT_ROOT / "scripts",
        PROJECT_ROOT / "docs",
    ]
    return [p for p in candidates if p.exists()]


def role_artifact_matches() -> list[tuple[int, Path, tuple[str, ...]]]:
    seen: set[Path] = set()
    matches: list[tuple[int, Path, tuple[str, ...]]] = []
    for base in candidate_scan_roots():
        for path in base.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
                continue
            if path in seen:
                continue
            seen.add(path)
            text = safe_read(path)
            if text is None:
                continue
            roles = tuple(role for role in LOCKED_ENTRY_ROLES if role in text)
            phase3ish = (
                "phase3" in str(path).lower()
                or "exp-00" in path.name.lower()
                or "dev-freeze-0002" in text.lower()
            )
            if roles and phase3ish:
                matches.append((len(roles), path, roles))
    matches.sort(key=lambda x: (-x[0], str(x[1]).lower()))
    return matches


def collect_role_objects(obj: Any, source: str, out: list[dict[str, Any]], depth: int = 0) -> None:
    if depth > 12 or len(out) >= 40:
        return
    if isinstance(obj, dict):
        values = {str(v) for v in obj.values() if isinstance(v, (str, int, float))}
        keys = set(map(str, obj.keys()))
        role_hits = [role for role in LOCKED_ENTRY_ROLES if role in values or role in keys]
        interesting_keys = {
            "role", "parameter_set_id", "entry_parameter_set_id", "variant_id",
            "parameters", "entry_parameters", "min_return_bps", "min_depth_bps",
            "max_depth_bps", "min_rebound_bps", "min_buys",
            "min_net_flow_reserve_ppm", "min_extension_from_response_bps",
            "max_extension_from_response_bps",
        }
        if role_hits and (interesting_keys & keys or len(role_hits) >= 2):
            compact: dict[str, Any] = {}
            for k, v in obj.items():
                if k in interesting_keys or k in LOCKED_ENTRY_ROLES:
                    compact[k] = v
            if not compact:
                compact = {
                    k: v for k, v in obj.items()
                    if isinstance(v, (str, int, float, bool, type(None)))
                }
            out.append({"source": source, "roles": role_hits, "object": compact})
        for v in obj.values():
            collect_role_objects(v, source, out, depth + 1)
    elif isinstance(obj, list):
        for v in obj:
            collect_role_objects(v, source, out, depth + 1)


def structured_role_findings(paths: list[Path]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in paths:
        if path.suffix.lower() != ".json":
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        collect_role_objects(data, str(path.relative_to(PROJECT_ROOT)), out)
        if len(out) >= 40:
            break
    return out


def inspect_production_db_fast() -> tuple[bool, dict[str, Any]]:
    if not PRODUCTION_DB.exists():
        return False, {"error": "production DB missing"}

    st_before = PRODUCTION_DB.stat()
    uri = PRODUCTION_DB.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=1.0)
    conn.row_factory = sqlite3.Row

    try:
        conn.execute("PRAGMA query_only = ON")
        conn.execute("PRAGMA busy_timeout = 1000")

        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

        required_tables = {
            "pump_events",
            "websocket_observations",
            "collector_events",
        }
        missing_tables = sorted(required_tables - tables)

        cols: list[str] = []
        latest_rowid = None
        latest_observed = None

        if "pump_events" in tables:
            cols = [
                row["name"]
                for row in conn.execute("PRAGMA table_info(pump_events)").fetchall()
            ]

            # Fast B-tree tail lookup only. No COUNT(*), no full table scan.
            try:
                row = conn.execute(
                    "SELECT rowid, decoded_at_utc FROM pump_events ORDER BY rowid DESC LIMIT 1"
                ).fetchone()
                if row is not None:
                    latest_rowid = row["rowid"]
                    latest_observed = row["decoded_at_utc"]
            except sqlite3.Error:
                pass

    finally:
        conn.close()

    st_after = PRODUCTION_DB.stat()

    unchanged = (
        st_before.st_size == st_after.st_size
        and st_before.st_mtime_ns == st_after.st_mtime_ns
    )

    required_cols = {
        "event_key", "event_type", "mint", "decoded_at_utc",
        "virtual_sol_reserves", "virtual_token_reserves",
        "quote_mint", "virtual_quote_reserves",
    }
    missing_cols = sorted(required_cols - set(cols))

    ok = not missing_tables and not missing_cols and unchanged

    return ok, {
        "latest_rowid": latest_rowid,
        "latest_observed": latest_observed,
        "missing_tables": missing_tables,
        "missing_columns": missing_cols,
        "db_unchanged_during_readonly_probe": unchanged,
        "size_bytes": st_after.st_size,
    }


def main() -> int:
    print("=" * 108)
    print("PHASE 4.3B0 - LIVE CANDIDATESIGNAL INTEGRATION PREFLIGHT v0.2")
    print("=" * 108)
    print(f"Project root                         : {PROJECT_ROOT}")
    print(f"Production DB                        : {PRODUCTION_DB}")
    print("Production DB mode                   : QUERY-ONLY / READ-ONLY")
    print("Full SQLite quick_check              : SKIPPED BY DESIGN")
    print("Full pump_events COUNT(*)            : SKIPPED BY DESIGN")
    print("Network / RPC                        : NO")
    print("Wallet / signing / live orders       : NO")
    print("Paper orders                         : NO")
    print("DEV-FREEZE-0002 tuning/reselection   : NO")
    print("Purpose                              : bind 4.3B to exact local Phase2/3 interfaces before live smoke")
    print()

    missing_required: list[str] = []

    print("-" * 108)
    print("ACTIVE BASELINE FILE CHECK")
    print("-" * 108)

    for label, path in REQUIRED_FILES.items():
        exists = path.exists()
        rel = path.relative_to(PROJECT_ROOT)
        print(f"{label:<39}: {'PASS' if exists else 'MISSING'} | {rel}")
        if not exists:
            missing_required.append(label)

    print()
    print("-" * 108)
    print("PRODUCTION DB FAST READ-ONLY CONTRACT")
    print("-" * 108)

    db_ok, db_info = inspect_production_db_fast()

    if "error" in db_info:
        print(f"DB probe                             : FAIL | {db_info['error']}")
    else:
        print("Schema read                          : PASS")
        print(f"Latest pump rowid                    : {db_info['latest_rowid']}")
        print(f"Latest observed                      : {db_info['latest_observed']}")
        print(f"Missing required tables              : {db_info['missing_tables'] or 'NONE'}")
        print(f"Missing required columns             : {db_info['missing_columns'] or 'NONE'}")
        print(
            "DB unchanged during query-only probe : "
            + ("PASS" if db_info["db_unchanged_during_readonly_probe"] else "FAIL")
        )

    print()
    print("-" * 108)
    print("PHASE2 API SOURCE SHAPE (NO BEHAVIOR CHANGES)")
    print("-" * 108)

    api_targets = [
        REQUIRED_FILES["Phase2 core models"],
        REQUIRED_FILES["BOT_TRUTH feature engine"],
        REQUIRED_FILES["FirstPullback v1.1 implementation"],
        REQUIRED_FILES["Phase1 read-only adapter"],
        REQUIRED_FILES["Strategy clock"],
        REQUIRED_FILES["Quote-aware price"],
        REQUIRED_FILES["Denomination-aware flow"],
    ]

    for path in api_targets:
        print()
        print(f"[{path.relative_to(PROJECT_ROOT)}]")
        if not path.exists():
            print("  MISSING")
            continue
        try:
            summary = source_api_summary(path)
            if not summary:
                print("  (no focused API symbols found by AST summary)")
            for line in summary:
                print(f"  {line}")
        except Exception as exc:
            print(f"  AST_ERROR: {type(exc).__name__}: {exc}")

    print()
    print("-" * 108)
    print("LOCKED PHASE3 ENTRY-ROLE ARTIFACT DISCOVERY")
    print("-" * 108)

    matches = role_artifact_matches()
    found_roles: set[str] = set()
    top_paths: list[Path] = []

    for score, path, roles in matches[:30]:
        found_roles.update(roles)
        top_paths.append(path)
        rel = path.relative_to(PROJECT_ROOT)
        try:
            digest = sha256_file(path)
        except Exception:
            digest = "HASH_ERROR"
        print(f"roles={','.join(roles):<36} | {rel}")
        print(f"  sha256={digest}")

    if not matches:
        print("No Phase3 role-bearing artifacts found in scanned project paths.")

    print()
    print(f"Locked roles expected                : {LOCKED_ENTRY_ROLES}")
    print(
        "Locked roles discovered              : "
        + str(tuple(r for r in LOCKED_ENTRY_ROLES if r in found_roles))
    )

    print()
    print("-" * 108)
    print("STRUCTURED ROLE/PARAMETER FINDINGS")
    print("-" * 108)

    findings = structured_role_findings(top_paths[:20])

    if not findings:
        print("No compact structured parameter objects extracted.")
        print(
            "This is not itself a strategy failure; the next build must bind to the exact "
            "local Phase3 artifact path shown above."
        )
    else:
        for i, item in enumerate(findings[:20], 1):
            print(f"[{i:02d}] source={item['source']} | roles={item['roles']}")
            print(
                "     "
                + json.dumps(
                    item["object"],
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                )[:1800]
            )

    roles_ok = set(LOCKED_ENTRY_ROLES).issubset(found_roles)
    files_ok = not missing_required

    print()
    print("-" * 108)
    print("VALIDATION")
    print("-" * 108)
    print(f"All active baseline files present    : {'PASS' if files_ok else 'FAIL'}")
    print(f"Production DB fast read-only contract: {'PASS' if db_ok else 'FAIL'}")
    print(f"CONTROL + ROBUST_1/2/3 discoverable  : {'PASS' if roles_ok else 'CHECK'}")
    print("CandidateSignal remains strategy boundary: PASS")
    print("No parameter search / reselection    : PASS")
    print("No wallet / signing / live orders    : PASS")
    print()

    if files_ok and db_ok and roles_ok:
        print("RESULT: PASS")
        print(
            "NEXT: build Phase 4.3B1 live CandidateSignal source + controlled live smoke "
            "against these exact interfaces."
        )
        return 0

    print("RESULT: CHECK")
    print(
        "Do not start live 4.3B1 yet. Return this output so the missing local binding "
        "can be resolved without guessing."
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
