from __future__ import annotations

from collections import Counter
from pathlib import Path
import argparse
import statistics
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from phase2.data_coverage_root_cause_v0_1_1 import (
    nearest_observation_before_us,
    read_control_events,
    reconstruct_collector_coverage,
    us_to_iso,
)
from phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01


def pct(n: float, d: float) -> float:
    return 0.0 if d <= 0 else 100.0 * n / d


def percentile_nearest(values: list[float], p: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    idx = max(0, min(len(s)-1, round((p/100.0)*(len(s)-1))))
    return s[idx]


def fmt_seconds(v: float | None) -> str:
    if v is None:
        return "N/A"
    if v >= 3600:
        return f"{v/3600:.3f} h"
    if v >= 60:
        return f"{v/60:.2f} min"
    return f"{v:.3f} s"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--production-db",
        default=str(PROJECT_ROOT / "data/db/tradingbot.sqlite3"),
    )
    args = ap.parse_args()
    db = Path(args.production_db).resolve()
    if not db.exists():
        raise FileNotFoundError(db)

    conn = Phase1ReadOnlyAdapterV01.open_readonly(db)
    try:
        controls = read_control_events(conn)
        intervals, sessions, gaps, summary = reconstruct_collector_coverage(
            controls,
            nearest_before=lambda cutoff_us, after_us: nearest_observation_before_us(
                conn, cutoff_us, after_us=after_us
            ),
        )
    finally:
        conn.close()

    if not sessions:
        raise RuntimeError("no reconstructed collector sessions")

    latest_session = max(sessions, key=lambda s: int(s["session_id"]))
    sid = int(latest_session["session_id"])
    s_intervals = [i for i in intervals if i.session_id == sid]
    s_gaps = [g for g in gaps if int(g["session_id"]) == sid]
    s_controls = []
    session_start_us = int(latest_session["start_at_us"])
    session_stop_us = int(latest_session["stop_at_us"] or session_start_us)
    for ev in controls:
        if ev.happened_at_us >= session_start_us:
            s_controls.append(ev)

    wall_seconds = max(0.0, (session_stop_us - session_start_us) / 1_000_000)
    active_durations = [
        max(0.0, (i.end_us - i.start_us) / 1_000_000) for i in s_intervals
    ]
    gap_durations = [float(g["gap_seconds"]) for g in s_gaps]
    close_reasons = Counter(i.close_reason for i in s_intervals)
    control_types = Counter(ev.event_type for ev in s_controls)

    longest = max(s_intervals, key=lambda i: i.end_us - i.start_us) if s_intervals else None
    latest = max(s_intervals, key=lambda i: i.end_us) if s_intervals else None

    print("PHASE 3 LIVE COVERAGE DIAGNOSTIC v0.1")
    print("=" * 76)
    print(f"Latest reconstructed session : {sid}")
    print(f"Collector version             : {latest_session['version']}")
    print(f"Session start                 : {latest_session['start_at_utc']}")
    print(f"Session observed through      : {latest_session['stop_at_utc']}")
    print(f"Session close/state           : {latest_session['stop_kind']}")
    print(f"Session wall duration         : {fmt_seconds(wall_seconds)}")
    print(f"Active intervals (session)    : {len(s_intervals)}")
    print(f"Explicit gaps (session)       : {len(s_gaps)}")
    print(f"Active seconds (session)      : {fmt_seconds(sum(active_durations))}")
    print(f"Explicit gap time (session)   : {fmt_seconds(sum(gap_durations))}")
    print(f"Active/wall ratio             : {pct(sum(active_durations), wall_seconds):.2f}%")
    print(f"Uncertain interval closes     : {latest_session['uncertain_interval_closes']}")
    print("-" * 76)

    if longest is not None:
        longest_s = (longest.end_us - longest.start_us) / 1_000_000
        print(
            f"Longest clean interval        : {fmt_seconds(longest_s)} "
            f"({us_to_iso(longest.start_us)} -> {us_to_iso(longest.end_us)})"
        )
    if latest is not None:
        latest_s = (latest.end_us - latest.start_us) / 1_000_000
        print(
            f"Latest clean interval         : {fmt_seconds(latest_s)} "
            f"({us_to_iso(latest.start_us)} -> {us_to_iso(latest.end_us)})"
        )

    if active_durations:
        print(f"Median clean interval         : {fmt_seconds(statistics.median(active_durations))}")
        print(f"P90 clean interval            : {fmt_seconds(percentile_nearest(active_durations, 90))}")
    if gap_durations:
        print(f"Median explicit gap           : {fmt_seconds(statistics.median(gap_durations))}")
        print(f"P90 explicit gap              : {fmt_seconds(percentile_nearest(gap_durations, 90))}")
        print(f"Max explicit gap              : {fmt_seconds(max(gap_durations))}")

    print("-" * 76)
    print("Interval close reasons:")
    for k, v in sorted(close_reasons.items()):
        print(f"  {k:28s} {v}")

    print("Control events in latest session:")
    for k, v in sorted(control_types.items()):
        print(f"  {k:28s} {v}")

    if s_gaps:
        print("-" * 76)
        print("Last 5 explicit gaps:")
        for g in s_gaps[-5:]:
            print(
                f"  {g['gap_start_utc']} -> {g['gap_end_utc']}  "
                f"{fmt_seconds(float(g['gap_seconds']))}"
            )

    print("-" * 76)
    print(f"All reconstructed sessions    : {summary['sessions']}")
    print(f"All explicit gaps             : {summary['explicit_gaps']}")
    print("Production DB opened          : READ-ONLY")
    print("Production DB mutated         : NO")
    print("Freeze created                : NO")
    print("=" * 76)
    print("RESULT: PASS")


if __name__ == "__main__":
    main()
