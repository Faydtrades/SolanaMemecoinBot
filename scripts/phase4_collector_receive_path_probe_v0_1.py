from __future__ import annotations

import ast
import hashlib
import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
COLLECTOR = PROJECT_ROOT / "scripts" / "live_pump_collector_v0_3_3.py"

WORKING_REFERENCE = {
    "ws_rpc": "wss://api.mainnet.solana.com",
    "pump_program_id": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
    "subscription_method": "logsSubscribe",
    "commitment": "processed",
    "ping_interval": None,
}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def line_window(lines: list[str], center: int, radius: int = 8) -> str:
    lo = max(1, center - radius)
    hi = min(len(lines), center + radius)
    out = []
    for n in range(lo, hi + 1):
        mark = ">>" if n == center else "  "
        out.append(f"{mark} {n:5d}: {lines[n-1].rstrip()}")
    return "\n".join(out)


def find_lines(lines: list[str], patterns: list[str]) -> list[tuple[int, str]]:
    hits = []
    for i, line in enumerate(lines, 1):
        low = line.lower()
        if any(p.lower() in low for p in patterns):
            hits.append((i, line.rstrip()))
    return hits


def top_level_assignments(tree: ast.AST) -> dict[str, str]:
    result = {}
    if not isinstance(tree, ast.Module):
        return result
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = []
            value = getattr(node, "value", None)
            if isinstance(node, ast.Assign):
                targets = [t for t in node.targets if isinstance(t, ast.Name)]
            elif isinstance(node.target, ast.Name):
                targets = [node.target]
            for target in targets:
                try:
                    result[target.id] = ast.unparse(value)
                except Exception:
                    result[target.id] = "<unparse unavailable>"
    return result


def function_inventory(tree: ast.AST) -> list[tuple[str, int, int]]:
    out = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            end = getattr(node, "end_lineno", node.lineno)
            out.append((node.name, node.lineno, end))
    return sorted(out, key=lambda x: x[1])


def relevant_functions(tree: ast.AST, text: str) -> list[tuple[str, int, int]]:
    lines = text.splitlines()
    inventory = function_inventory(tree)
    tokens = (
        "websocket", "subscribe", "listener", "collect", "receive", "recv",
        "stale", "watchdog", "reconnect", "live", "pump", "notification"
    )
    chosen = []
    for name, start, end in inventory:
        body = "\n".join(lines[start-1:end]).lower()
        if any(tok in name.lower() or tok in body for tok in tokens):
            chosen.append((name, start, end))
    return chosen[:30]


def main() -> int:
    print("=" * 112)
    print("PHASE 4.3B1B - COLLECTOR RECEIVE-PATH PROBE v0.1")
    print("=" * 112)
    print(f"Project root                 : {PROJECT_ROOT}")
    print(f"Collector                    : {COLLECTOR}")
    print("Collector modified           : NO")
    print("Collector executed           : NO")
    print("Production DB opened         : NO")
    print("Network/RPC                  : NO")
    print("Wallet/orders                : NO")
    print("Purpose                      : compare exact collector WS path with known-good direct subscription")
    print()

    if not COLLECTOR.exists():
        print("RESULT: CHECK")
        print(f"Collector file not found: {COLLECTOR}")
        return 2

    text = COLLECTOR.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()

    try:
        tree = ast.parse(text)
    except SyntaxError as exc:
        print(f"AST parse error: {exc}")
        print("RESULT: CHECK")
        return 2

    print(f"Collector SHA256             : {sha256_file(COLLECTOR)}")
    print(f"Collector lines              : {len(lines)}")
    print()

    print("-" * 112)
    print("KNOWN-GOOD REFERENCE FROM DIRECT PASS")
    print("-" * 112)
    for k, v in WORKING_REFERENCE.items():
        print(f"{k:<30}: {v!r}")

    print()
    print("-" * 112)
    print("TOP-LEVEL COLLECTOR CONSTANTS / ASSIGNMENTS OF INTEREST")
    print("-" * 112)

    assigns = top_level_assignments(tree)
    interest = (
        "RPC", "WS", "WEBSOCKET", "PUMP", "PROGRAM", "STALE", "PING",
        "TIMEOUT", "RECONNECT", "COMMITMENT", "SUBSCRIBE"
    )
    printed = 0
    for name, value in sorted(assigns.items()):
        if any(tok in name.upper() for tok in interest):
            print(f"{name:<42}= {value}")
            printed += 1
    if printed == 0:
        print("(No matching top-level assignments found.)")

    print()
    print("-" * 112)
    print("SOURCE HITS — WEBSOCKET / SUBSCRIPTION / RECEIVE")
    print("-" * 112)

    patterns = [
        "websockets.connect",
        "logsSubscribe",
        "logsNotification",
        ".recv(",
        "async for",
        "ping_interval",
        "open_timeout",
        "close_timeout",
        "stale",
        "wait_for(",
        "timeout",
        "mentions",
        "commitment",
        "PUMP_PROGRAM",
        "wss://",
    ]

    hits = find_lines(lines, patterns)
    if not hits:
        print("(No relevant source hits found.)")
    else:
        for n, line in hits[:120]:
            print(f"{n:5d}: {line}")

    print()
    print("-" * 112)
    print("CONTEXT WINDOWS FOR CRITICAL HITS")
    print("-" * 112)

    critical_patterns = [
        "websockets.connect",
        "logsSubscribe",
        ".recv(",
        "async for",
        "wait_for(",
    ]
    critical = find_lines(lines, critical_patterns)

    seen_centers = set()
    for n, line in critical[:30]:
        if n in seen_centers:
            continue
        seen_centers.add(n)
        print()
        print(f"[around line {n}]")
        print(line_window(lines, n, radius=7))

    print()
    print("-" * 112)
    print("RELEVANT FUNCTION INVENTORY")
    print("-" * 112)

    funcs = relevant_functions(tree, text)
    for name, start, end in funcs:
        print(f"{name:<48} lines {start}-{end}")

    print()
    print("-" * 112)
    print("AUTOMATIC CONTRACT CHECKS")
    print("-" * 112)

    checks = {
        "contains mainnet WS endpoint": "wss://api.mainnet.solana.com" in text,
        "contains locked Pump program id": WORKING_REFERENCE["pump_program_id"] in text,
        "contains logsSubscribe": "logsSubscribe" in text,
        "contains processed commitment": (
            '"processed"' in text or "'processed'" in text
        ),
        "contains mentions filter": "mentions" in text,
        "heartbeat disabled somewhere": bool(
            re.search(r"ping_interval\s*=\s*None", text)
        ),
        "has receive primitive": (
            ".recv(" in text or "async for" in text
        ),
    }

    for label, ok in checks.items():
        print(f"{label:<44}: {'PASS' if ok else 'CHECK'}")

    hard_ok = all(
        checks[k]
        for k in (
            "contains mainnet WS endpoint",
            "contains locked Pump program id",
            "contains logsSubscribe",
            "contains processed commitment",
            "contains mentions filter",
            "has receive primitive",
        )
    )

    print()
    print("RESULT:", "PASS" if hard_ok else "CHECK")
    print()
    print(
        "IMPORTANT: PASS here does NOT mean the collector live path is healthy. "
        "It means the static contract looks structurally compatible. "
        "Return this output so the exact receive-loop difference can be identified "
        "before any collector code is changed."
    )
    return 0 if hard_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
