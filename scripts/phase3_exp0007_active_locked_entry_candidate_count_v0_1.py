from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from phase2.data_coverage_root_cause_v0_1_1 import (
    iso_to_us,
    nearest_observation_before_us,
    read_control_events,
    reconstruct_collector_coverage,
    us_to_iso,
)
from phase2.phase1_readonly_adapter_v0_1 import (
    BOT_TRUTH_SOURCE_PREFIXES,
    Phase1ReadOnlyAdapterV01,
)
from phase2.quote_aware_price_v0_1 import Phase1QuoteAwareAdapterV01
from phase3.devfreeze0002_design_v0_2 import (
    CleanInterval,
    REQUIRED_POST_T0_MS,
    eligible_interval_for_t0,
)
from phase3.locked_entry_readiness_v0_1_1 import (
    EXPECTED_ROLES,
    load_locked_selection,
    parameter_set_from_locked_row,
)
from phase3.phase2_outcome_adapter_v0_1_2 import extract_candidates

SCHEMA_VERSION = "P3-ACTIVE-LOCKED-ENTRY-COUNT-0.1"
ENTRY_WINDOW_MS = 300_000


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json_atomic(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def bot_truth_source_sql(alias: str = "") -> tuple[str, tuple[str, ...]]:
    prefix = f"{alias}." if alias else ""
    clause = "(" + " OR ".join(
        f"{prefix}source_decoded_file LIKE ?" for _ in BOT_TRUTH_SOURCE_PREFIXES
    ) + ")"
    args = tuple(p + "%" for p in BOT_TRUTH_SOURCE_PREFIXES)
    return clause, args


def global_first_tradables_inside_session(
    conn,
    *,
    session_start_us: int,
    session_end_us: int,
) -> list[dict[str, object]]:
    where_source, source_args = bot_truth_source_sql()
    rows = conn.execute(
        f"""
        WITH ranked AS (
          SELECT
            rowid AS p1_rowid,
            mint,
            decoded_at_utc,
            source_decoded_file,
            ROW_NUMBER() OVER (
              PARTITION BY mint
              ORDER BY decoded_at_utc ASC, rowid ASC
            ) AS rn
          FROM pump_events
          WHERE mint IS NOT NULL
            AND event_type IN ('BUY','SELL')
            AND pump_timestamp IS NOT NULL
            AND decoded_at_utc IS NOT NULL
            AND slot IS NOT NULL
            AND {where_source}
        )
        SELECT mint, decoded_at_utc, source_decoded_file, p1_rowid
        FROM ranked
        WHERE rn = 1
          AND decoded_at_utc >= ?
          AND decoded_at_utc <= ?
          AND source_decoded_file NOT LIKE 'GAP_RECONCILIATION_%'
        ORDER BY decoded_at_utc, mint
        """,
        (*source_args, us_to_iso(session_start_us), us_to_iso(session_end_us)),
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_strategy_rows_for_eligible_tokens(
    conn,
    *,
    eligible: list[dict[str, object]],
    session_start_us: int,
    chunk_size: int = 300,
):
    """Fetch only rows needed by the exact 5m entry replay.

    Each eligible token already passed the stricter t0->t0+10m clean-window guard.
    We include any LAUNCH/BUY/SELL observed from session start through exact t0+5m.
    """
    if not eligible:
        return []

    where_source, source_args = bot_truth_source_sql("p")
    session_start_utc = us_to_iso(session_start_us)
    all_rows = []

    for offset in range(0, len(eligible), chunk_size):
        batch = eligible[offset:offset + chunk_size]
        values_sql = ",".join("(?,?)" for _ in batch)
        values_args: list[object] = []
        for item in batch:
            values_args.extend(
                [
                    str(item["mint"]),
                    us_to_iso(int(item["t0_us"]) + ENTRY_WINDOW_MS * 1000),
                ]
            )

        rows = conn.execute(
            f"""
            WITH eligible(mint, deadline_utc) AS (
                VALUES {values_sql}
            )
            SELECT p.rowid AS p1_rowid, p.*
            FROM pump_events AS p
            JOIN eligible AS e ON e.mint = p.mint
            WHERE p.event_type IN ('LAUNCH','BUY','SELL')
              AND p.pump_timestamp IS NOT NULL
              AND p.decoded_at_utc IS NOT NULL
              AND p.slot IS NOT NULL
              AND p.decoded_at_utc >= ?
              AND p.decoded_at_utc <= e.deadline_utc
              AND {where_source}
            ORDER BY p.decoded_at_utc ASC, p.rowid ASC
            """,
            (*values_args, session_start_utc, *source_args),
        ).fetchall()
        all_rows.extend(rows)

    all_rows.sort(
        key=lambda r: (
            Phase1ReadOnlyAdapterV01._iso_to_us(str(r["decoded_at_utc"])),
            int(r["p1_rowid"]),
        )
    )
    return all_rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--production-db",
        default=str(PROJECT_ROOT / "data/db/tradingbot.sqlite3"),
    )
    ap.add_argument(
        "--selection",
        default=str(
            PROJECT_ROOT
            / "data/research/phase3/EXP-0005/EXP-0005_D_selection_v0_1.json"
        ),
    )
    ap.add_argument(
        "--out",
        default=str(
            PROJECT_ROOT
            / "data/research/phase3/EXP-0007/"
              "DEV-FREEZE-0002_active_locked_entry_count_preview_v0_1.json"
        ),
    )
    args = ap.parse_args()

    db = Path(args.production_db).resolve()
    selection_path = Path(args.selection).resolve()
    out = Path(args.out).resolve()
    for p in (db, selection_path):
        if not p.exists():
            raise FileNotFoundError(p)

    selection_payload, selection_rows = load_locked_selection(selection_path)
    if tuple(str(r["role"]) for r in selection_rows) != EXPECTED_ROLES:
        raise RuntimeError("locked EXP-0005 role order changed")

    before = (db.stat().st_size, db.stat().st_mtime_ns)

    conn = Phase1ReadOnlyAdapterV01.open_readonly(db)
    try:
        # One stable SQLite read snapshot while the live writer continues in WAL mode.
        conn.execute("BEGIN")
        Phase1QuoteAwareAdapterV01.validate_schema(conn)

        snapshot_max_rowid = int(
            conn.execute("SELECT COALESCE(MAX(rowid),0) FROM pump_events").fetchone()[0]
        )

        controls = read_control_events(conn)
        intervals, sessions, gaps, summary = reconstruct_collector_coverage(
            controls,
            nearest_before=lambda cutoff_us, after_us: nearest_observation_before_us(
                conn, cutoff_us, after_us=after_us
            ),
        )
        if not sessions:
            raise RuntimeError("no reconstructed collector sessions")

        latest = max(sessions, key=lambda s: int(s["session_id"]))
        sid = int(latest["session_id"])
        session_start_us = int(latest["start_at_us"])
        session_end_us = int(latest["stop_at_us"] or session_start_us)
        session_intervals = [
            CleanInterval(
                session_id=i.session_id,
                start_us=int(i.start_us),
                end_us=int(i.end_us),
                uncertain_close=bool(i.uncertain_close),
            )
            for i in intervals
            if i.session_id == sid
        ]
        if not session_intervals:
            raise RuntimeError("latest session has no active intervals")

        t0_rows = global_first_tradables_inside_session(
            conn,
            session_start_us=session_start_us,
            session_end_us=session_end_us,
        )

        eligible: list[dict[str, object]] = []
        for row in t0_rows:
            t0_us = iso_to_us(str(row["decoded_at_utc"]))
            interval = eligible_interval_for_t0(
                t0_us=t0_us,
                required_post_t0_ms=REQUIRED_POST_T0_MS,
                intervals=session_intervals,
            )
            if interval is None:
                continue
            eligible.append(
                {
                    **row,
                    "t0_us": t0_us,
                    "clean_interval_start_us": interval.start_us,
                    "clean_interval_end_us": interval.end_us,
                }
            )

        strategy_rows = fetch_strategy_rows_for_eligible_tokens(
            conn,
            eligible=eligible,
            session_start_us=session_start_us,
        )
        events, skipped = Phase1QuoteAwareAdapterV01.normalize_rows(strategy_rows)
        if skipped:
            raise RuntimeError(f"normalization skips prevent exact count: {skipped}")

        # Sanity: every replayed mint must belong to the projected eligible cohort.
        eligible_mints = {str(r["mint"]) for r in eligible}
        event_mints = {e.base.mint for e in events}
        unexpected = event_mints - eligible_mints
        if unexpected:
            raise RuntimeError(f"replay contains {len(unexpected)} non-eligible mints")

        regimes = []
        candidate_mints_by_role: dict[str, set[str]] = {}
        for row in selection_rows:
            role = str(row["role"])
            params = parameter_set_from_locked_row(row)
            candidates, run_summary = extract_candidates(events, params)

            if run_summary.run_count != run_summary.accounted_run_count:
                raise RuntimeError(f"{role}: terminal accounting incomplete")
            if run_summary.candidate_count != run_summary.candidate_state_count:
                raise RuntimeError(f"{role}: candidate lifecycle mismatch")

            mints = {c.signal.mint for c in candidates}
            candidate_mints_by_role[role] = mints
            regimes.append(
                {
                    "role": role,
                    "parameter_set_id": params.parameter_set_id,
                    "candidate_count": len(candidates),
                    "candidate_mint_count": len(mints),
                    "run_count": run_summary.run_count,
                    "terminal": {
                        "invalidated": run_summary.invalidated_count,
                        "expired": run_summary.expired_count,
                        "rejected": run_summary.rejected_count,
                        "trade": run_summary.trade_count,
                        "candidate_state": run_summary.candidate_state_count,
                    },
                }
            )

        robust_roles = ("ROBUST_1", "ROBUST_2", "ROBUST_3")
        robust_union = set().union(*(candidate_mints_by_role[r] for r in robust_roles))
        all_three = set.intersection(*(candidate_mints_by_role[r] for r in robust_roles))
        pairwise = {}
        for i, a in enumerate(robust_roles):
            for b in robust_roles[i + 1:]:
                pairwise[f"{a}&{b}"] = len(
                    candidate_mints_by_role[a] & candidate_mints_by_role[b]
                )

        session_gap_rows = [g for g in gaps if int(g["session_id"]) == sid]
        active_seconds = sum(
            max(0, i.end_us - i.start_us) for i in session_intervals
        ) / 1_000_000
        wall_seconds = max(0, session_end_us - session_start_us) / 1_000_000

        payload = {
            "schema_version": SCHEMA_VERSION,
            "purpose": "ENTRY_CANDIDATE_COUNT_ONLY",
            "outcomes_inspected": False,
            "exit_paths_built": False,
            "pnl_inspected": False,
            "exit_threshold_tuning_performed": False,
            "production_db_read_only": True,
            "snapshot": {
                "session_id": sid,
                "collector_version": latest["version"],
                "session_start_utc": latest["start_at_utc"],
                "observed_through_utc": latest["stop_at_utc"],
                "session_state": latest["stop_kind"],
                "snapshot_max_pump_event_rowid": snapshot_max_rowid,
                "wall_hours": wall_seconds / 3600,
                "active_hours": active_seconds / 3600,
                "active_ratio": 0.0 if wall_seconds <= 0 else active_seconds / wall_seconds,
                "explicit_gap_count": len(session_gap_rows),
                "explicit_gap_seconds": sum(float(g["gap_seconds"]) for g in session_gap_rows),
            },
            "cohort": {
                "global_t0s_inside_session": len(t0_rows),
                "projected_eligible_tokens_t0_to_t0_plus_10m_clean": len(eligible),
                "strategy_rows_replayed_through_exact_t0_plus_5m": len(strategy_rows),
                "normalized_events_replayed": len(events),
            },
            "entry_binding": {
                "selection_path": str(selection_path),
                "selection_sha256": sha256_file(selection_path),
                "experiment_id": selection_payload["experiment_id"],
                "status": selection_payload["status"],
                "A_B_C_D_fixed": True,
            },
            "regimes": regimes,
            "robust_overlap_by_mint": {
                "unique_mints_in_any_robust_regime": len(robust_union),
                "mints_in_all_three_robust_regimes": len(all_three),
                "pairwise": pairwise,
            },
            "readiness_note": (
                "These are exact locked ENTRY candidate counts at this read snapshot. "
                "They are NOT yet exact E1/E2 clean-5m-path counts because no forward "
                "outcome/ExitPathReplay was inspected."
            ),
        }
        write_json_atomic(out, payload)

        conn.rollback()
    finally:
        conn.close()

    after = (db.stat().st_size, db.stat().st_mtime_ns)

    by_role = {r["role"]: r for r in payload["regimes"]}
    print("EXP-0007 ACTIVE LOCKED-ENTRY CANDIDATE COUNTER v0.1")
    print("=" * 76)
    print(f"Session id                   : {payload['snapshot']['session_id']}")
    print(f"Session wall duration        : {payload['snapshot']['wall_hours']:.2f} h")
    print(f"Session active duration      : {payload['snapshot']['active_hours']:.2f} h")
    print(f"Active/wall ratio            : {100*payload['snapshot']['active_ratio']:.2f}%")
    print(f"Projected eligible tokens    : {payload['cohort']['projected_eligible_tokens_t0_to_t0_plus_10m_clean']}")
    print("-" * 76)
    for role in EXPECTED_ROLES:
        print(f"{role:10s} candidates          : {by_role[role]['candidate_count']}")
    print("-" * 76)
    print(f"Unique mints any robust      : {payload['robust_overlap_by_mint']['unique_mints_in_any_robust_regime']}")
    print(f"Mints in all 3 robust        : {payload['robust_overlap_by_mint']['mints_in_all_three_robust_regimes']}")
    print("Forward outcomes inspected   : NO")
    print("Exit paths built             : NO")
    print("PnL inspected                : NO")
    print("Entry A/B/C/D changed        : NO")
    print("Production DB opened         : READ-ONLY")
    print("Production DB mutated        : NO")
    print(f"DB changed by live writer    : {'YES (expected while collector runs)' if before != after else 'NO/NOT OBSERVED'}")
    print(f"Preview JSON                 : {out}")
    print("=" * 76)
    print("RESULT: PASS")


if __name__ == "__main__":
    main()
