from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import shutil
import sqlite3
import sys
import types
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PHASE4_ROOT = PROJECT_ROOT / "src" / "phase4"
SMOKE_DIR = PROJECT_ROOT / "data" / "paper" / "live_smoke"
SELFTEST_DIR = PROJECT_ROOT / "data" / "selftest"

NS4 = "tradingbot_local_phase4_b1d2_lifecycle_probe_v02"


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


def latest_pair() -> tuple[Path, Path | None]:
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
    return db, audit if audit.exists() else None


def enum_value(v):
    return getattr(v, "value", v)


def main() -> int:
    print("=" * 118)
    print("PHASE 4.3B1D2 - LIFECYCLE API REPLAY PROBE v0.2")
    print("=" * 118)

    original_db, audit_path = latest_pair()

    lifecycle_mod = local_import("paper_lifecycle_v0_1")
    router_mod = local_import("paper_entry_router_v0_1")

    Store = lifecycle_mod.PaperLifecycleStore
    locked_tracks = tuple(router_mod.LOCKED_PHASE4_TRACKS)

    SELFTEST_DIR.mkdir(parents=True, exist_ok=True)
    replay_db = SELFTEST_DIR / (
        original_db.stem + "_lifecycle_api_replay_v0_2.sqlite3"
    )
    if replay_db.exists():
        replay_db.unlink()

    original_hash_before = sha256_file(original_db)
    shutil.copy2(original_db, replay_db)
    copied_hash_before = sha256_file(replay_db)

    if copied_hash_before != original_hash_before:
        raise RuntimeError("Isolated replay copy hash does not match original")

    print(f"Project root                    : {PROJECT_ROOT}")
    print(f"Original paper DB               : {original_db}")
    print(f"Isolated replay DB              : {replay_db}")
    print(f"Original audit                  : {audit_path if audit_path else 'NOT FOUND'}")
    print("Original DB opened              : NO")
    print("Original DB writes              : NO")
    print("Replay DB mode                  : ISOLATED COPY / READ-WRITE")
    print("Collector / network             : NO")
    print("Wallet / live orders            : NO")
    print(f"Lifecycle class                 : {Store.__module__}.{Store.__name__}")
    print(f"Lifecycle constructor           : {inspect.signature(Store)}")
    print(f"LOCKED_PHASE4_TRACKS            : {locked_tracks}")
    print(f"Original SHA256 before          : {original_hash_before}")
    print()

    original_failed = []
    if audit_path is not None:
        payload = json.loads(audit_path.read_text(encoding="utf-8"))
        original_failed = sorted(
            k
            for k, v in (payload.get("fill_checks") or {}).items()
            if v is not True
        )

    conn = sqlite3.connect(replay_db, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")

    try:
        store = Store(conn)

        first = store.list_open_positions()
        second = store.list_open_positions()

        first_tracks = tuple(p.track_id for p in first)
        second_tracks = tuple(p.track_id for p in second)

        raw_rows = conn.execute(
            """
            SELECT rowid, paper_position_id, paper_order_id, track_id, state
            FROM paper_positions
            WHERE state='OPEN'
            """
        ).fetchall()

        raw_tracks = tuple(str(r["track_id"]) for r in raw_rows)
        quick = conn.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        conn.close()

    original_hash_after = sha256_file(original_db)
    copied_hash_after = sha256_file(replay_db)

    print("-" * 118)
    print("RUNTIME API RESULT")
    print("-" * 118)
    print(f"Original failed checks          : {original_failed}")
    print(f"list_open_positions #1          : {first_tracks}")
    print(f"list_open_positions #2          : {second_tracks}")
    print(f"Raw DB row order                : {raw_tracks}")
    print()

    print("Track value types:")
    for i, p in enumerate(first, 1):
        print(
            f"  {i}: value={p.track_id!r} "
            f"type={type(p.track_id).__module__}.{type(p.track_id).__name__} "
            f"enum_value={enum_value(p.track_id)!r}"
        )

    print()
    print("-" * 118)
    print("list_open_positions SOURCE")
    print("-" * 118)
    try:
        print(inspect.getsource(Store.list_open_positions))
    except Exception as exc:
        print(f"SOURCE UNAVAILABLE: {type(exc).__name__}: {exc}")

    normalized_first = tuple(str(enum_value(x)) for x in first_tracks)
    normalized_second = tuple(str(enum_value(x)) for x in second_tracks)
    normalized_locked = tuple(str(enum_value(x)) for x in locked_tracks)

    checks = {
        "original_only_position_track_check_failed": (
            original_failed == ["position_tracks_match_lock"]
        ),
        "exactly_three_runtime_positions": len(first) == 3,
        "runtime_membership_exact": (
            set(normalized_first) == set(normalized_locked)
            and len(set(normalized_first)) == 3
        ),
        "runtime_repeat_deterministic": (
            normalized_first == normalized_second
        ),
        "runtime_equals_locked_order": (
            normalized_first == normalized_locked
        ),
        "raw_db_membership_exact": (
            set(raw_tracks) == set(normalized_locked)
            and len(set(raw_tracks)) == 3
        ),
        "replay_db_quick_check": str(quick).lower() == "ok",
        "original_hash_unchanged": (
            original_hash_before == original_hash_after
        ),
        "copy_started_exact": (
            copied_hash_before == original_hash_before
        ),
    }

    print("-" * 118)
    print("VALIDATION")
    print("-" * 118)
    for label, ok in checks.items():
        print(f"{label:<54}: {'PASS' if ok else 'FAIL'}")

    print()
    print(f"Original SHA256 after           : {original_hash_after}")
    print(f"Replay copy SHA256 before       : {copied_hash_before}")
    print(f"Replay copy SHA256 after        : {copied_hash_after}")
    print("Original paper DB touched       : NO")
    print()

    core_ok = all(
        checks[k]
        for k in (
            "original_only_position_track_check_failed",
            "exactly_three_runtime_positions",
            "runtime_membership_exact",
            "runtime_repeat_deterministic",
            "raw_db_membership_exact",
            "replay_db_quick_check",
            "original_hash_unchanged",
            "copy_started_exact",
        )
    )

    if core_ok and not checks["runtime_equals_locked_order"]:
        print("DIAGNOSIS: LIFECYCLE_LIST_ORDER_ONLY_FALSE_NEGATIVE")
        print(
            "The actual lifecycle API returns the exact three locked tracks "
            "deterministically, but in an order different from LOCKED_PHASE4_TRACKS. "
            "The original B1D2 validator treated ordering as semantic."
        )
        print("RESULT: PASS_DIAGNOSIS")
        return 0

    if core_ok and checks["runtime_equals_locked_order"]:
        print("DIAGNOSIS: CURRENT_RUNTIME_API_MATCHES_LOCK")
        print(
            "The actual lifecycle API reproduces the exact locked order on an exact "
            "copy of the persisted B1D2 database. Therefore the original live-run "
            "failure is not persisted lifecycle corruption and is not reproduced "
            "by the current lifecycle API. One final static check of the original "
            "B1D2 validator expression/types is required before closure."
        )
        print("RESULT: CHECK")
        return 2

    print("DIAGNOSIS: REAL_RUNTIME_LIFECYCLE_MISMATCH")
    print("RESULT: CHECK")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
