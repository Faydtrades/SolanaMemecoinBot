from __future__ import annotations

import argparse
import asyncio
import importlib.util
from pathlib import Path
import sys
import time

PROJECT_ROOT = Path(__file__).resolve().parents[1]
COLLECTOR_PATH = PROJECT_ROOT / "scripts" / "live_pump_collector_v0_3_3.py"

REQUIRED_FUNCTIONS = (
    "connect_db",
    "ensure_schema",
    "recover_jobs",
    "confirmation_worker",
    "deep_enrichment_worker",
    "gap_reconciliation_worker",
    "queue_counts",
)

STATS_KEYS = (
    "notifications",
    "new_observations",
    "duplicate_observations",
    "failed_transactions",
    "successful_non_pump_noise",
    "pump_events_decoded",
    "pump_events_inserted",
    "duplicate_pump_events",
    "decode_errors",
    "reconnects",
    "gap_count",
    "gap_seconds",
    "recovered_confirmation_jobs",
    "recovered_deep_jobs",
    "legacy_v0_2_jobs_ignored",
    "status_batch_calls",
    "status_signatures_checked",
    "confirmed_signatures",
    "confirmation_pending",
    "confirmation_failed",
    "confirmation_rpc_errors",
    "rpc_429_count",
    "deep_rpc_calls",
    "deep_enriched",
    "deep_null",
    "deep_errors",
    "gap_jobs_created",
    "gap_jobs_completed",
    "gap_jobs_oversize",
    "gap_blocks_checked",
    "gap_events_recovered",
    "gap_reconciliation_errors",
)


def load_collector():
    if not COLLECTOR_PATH.exists():
        raise FileNotFoundError(COLLECTOR_PATH)

    spec = importlib.util.spec_from_file_location(
        "tradingbot_live_pump_collector_v0_3_3",
        COLLECTOR_PATH,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load production collector module")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    missing = [name for name in REQUIRED_FUNCTIONS if not hasattr(module, name)]
    if missing:
        raise RuntimeError(
            "Production collector API mismatch; refusing recovery. "
            f"Missing: {missing}"
        )

    if getattr(module, "STATUS_BATCH_SIZE", None) != 64:
        raise RuntimeError(
            "Unexpected production confirmation batch size; "
            "refusing to assume v0.3.3 semantics."
        )

    return module


def fresh_stats() -> dict[str, int | float]:
    stats: dict[str, int | float] = {key: 0 for key in STATS_KEYS}
    stats["gap_seconds"] = 0.0
    return stats


async def drain(timeout_seconds: int, progress_seconds: int) -> int:
    collector = load_collector()

    conn = collector.connect_db()
    try:
        collector.ensure_schema(conn)
        recovered_confirm, recovered_deep, legacy = collector.recover_jobs(conn)
    finally:
        conn.close()

    stats = fresh_stats()
    stats["recovered_confirmation_jobs"] = recovered_confirm
    stats["recovered_deep_jobs"] = recovered_deep
    stats["legacy_v0_2_jobs_ignored"] = legacy

    initial = collector.queue_counts()

    print("EXP-0007 QUEUE-DRAIN RECOVERY v0.1")
    print("=" * 80)
    print(f"Production collector       : {COLLECTOR_PATH.name}")
    print("Live WebSocket listener    : NOT STARTED")
    print("New market collection      : NO")
    print("Recovery workers           : confirmation / deep / gap")
    print(f"Confirmation batch size    : {collector.STATUS_BATCH_SIZE}")
    print(f"Timeout                    : {timeout_seconds} s")
    print("-" * 80)
    print(f"Initial confirmation jobs  : {initial[0]}")
    print(f"Initial deep jobs          : {initial[1]}")
    print(f"Initial gap jobs           : {initial[2]}")
    print(f"Recovered confirmation     : {recovered_confirm}")
    print(f"Recovered deep             : {recovered_deep}")
    print()

    workers_stop = asyncio.Event()

    tasks = (
        asyncio.create_task(collector.confirmation_worker(workers_stop, stats)),
        asyncio.create_task(collector.deep_enrichment_worker(workers_stop, stats)),
        asyncio.create_task(collector.gap_reconciliation_worker(workers_stop, stats)),
    )

    started = time.monotonic()
    next_progress = started

    try:
        while True:
            now = time.monotonic()
            confirmation, deep, gap = collector.queue_counts()

            if confirmation == 0 and deep == 0 and gap == 0:
                break

            if now - started >= timeout_seconds:
                break

            if now >= next_progress:
                elapsed = now - started
                print(
                    f"[DRAIN {elapsed/60:6.1f}m] "
                    f"confirmation={confirmation} | "
                    f"deep={deep} | gap={gap} | "
                    f"checked={stats['status_signatures_checked']} | "
                    f"confirmed={stats['confirmed_signatures']} | "
                    f"failed={stats['confirmation_failed']} | "
                    f"deep_done={stats['deep_enriched']} | "
                    f"rpc429={stats['rpc_429_count']}"
                )
                next_progress = now + progress_seconds

            await asyncio.sleep(1.0)

    finally:
        workers_stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)

    final = collector.queue_counts()
    elapsed = time.monotonic() - started

    print()
    print("-" * 80)
    print("SUMMARY")
    print("-" * 80)
    print(f"Elapsed                    : {elapsed/60:.1f} min")
    print(f"Status batch RPC calls     : {stats['status_batch_calls']}")
    print(f"Signatures status-checked  : {stats['status_signatures_checked']}")
    print(f"Confirmed/finalized sigs   : {stats['confirmed_signatures']}")
    print(f"Confirmed failed sigs      : {stats['confirmation_failed']}")
    print(f"Confirmation RPC errors    : {stats['confirmation_rpc_errors']}")
    print(f"Deep getTransaction calls  : {stats['deep_rpc_calls']}")
    print(f"Deep jobs completed        : {stats['deep_enriched']}")
    print(f"Deep errors/retries        : {stats['deep_errors']}")
    print(f"Gap jobs completed         : {stats['gap_jobs_completed']}")
    print(f"Gap events recovered       : {stats['gap_events_recovered']}")
    print(f"HTTP 429 count             : {stats['rpc_429_count']}")
    print()
    print(f"Confirmation jobs remaining: {final[0]}")
    print(f"Deep jobs remaining        : {final[1]}")
    print(f"Gap jobs remaining         : {final[2]}")
    print("Live WebSocket listener    : NEVER STARTED")
    print("New collector session      : NO")
    print("=" * 80)

    if final == (0, 0, 0):
        print("RESULT: PASS")
        return 0

    print("RESULT: CHECK - QUEUES STILL REMAIN")
    return 2


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Drain persistent v0.3.3 confirmation/deep/gap queues without "
            "starting the live WebSocket listener."
        )
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=7200,
        help="Maximum recovery duration. Default: 7200 seconds (2 hours).",
    )
    parser.add_argument(
        "--progress-seconds",
        type=int,
        default=30,
        help="Queue progress print interval. Default: 30 seconds.",
    )
    parser.add_argument(
        "--confirm-drain",
        action="store_true",
        help="Required acknowledgement that this updates persistent queue/status data.",
    )
    args = parser.parse_args()

    if not args.confirm_drain:
        raise RuntimeError(
            "Refusing queue recovery without --confirm-drain. "
            "This is not a read-only operation."
        )
    if args.timeout_seconds <= 0:
        raise ValueError("--timeout-seconds must be > 0")
    if args.progress_seconds <= 0:
        raise ValueError("--progress-seconds must be > 0")

    try:
        rc = asyncio.run(
            drain(
                timeout_seconds=args.timeout_seconds,
                progress_seconds=args.progress_seconds,
            )
        )
    except KeyboardInterrupt:
        print()
        print("[STOP] Recovery interrupted. Persistent queues remain restart-safe.")
        print("Do NOT freeze yet.")
        rc = 130

    raise SystemExit(rc)


if __name__ == "__main__":
    main()
