from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
import sqlite3
from typing import Callable, Iterable, Mapping, Sequence

from .models_v0_1 import EventType, IngestionSource, NormalizedMarketEvent
from .phase1_readonly_adapter_v0_1 import BOT_TRUTH_SOURCE_PREFIXES


SCHEMA_VERSION = "P2DCRCA-0.1.1"
ENTRY_WINDOW_MS = 300_000
BIN_MINUTES = 30

CONTROL_EVENT_TYPES = (
    "COLLECTOR_V0_3_START",
    "WS_CONNECTED",
    "PUMP_SUBSCRIPTION_ACTIVE",
    "WS_STALE",
    "WS_DISCONNECTED",
    "DATA_GAP_CLOSED",
    "LIVE_COLLECTION_STOPPED",
)


def _jsonable(value):
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return value


def canonical_json(value: object) -> str:
    return json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def stable_sha256(value: object) -> str:
    return sha256(canonical_json(value).encode("utf-8")).hexdigest()


def iso_to_us(value: str) -> int:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        raise ValueError(f"UTC timestamp lacks timezone: {value!r}")
    return int(round(dt.astimezone(timezone.utc).timestamp() * 1_000_000))


def us_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc).isoformat()


def optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value))
    except Exception:
        return None


def positive(value: object) -> bool:
    parsed = optional_int(value)
    return parsed is not None and parsed > 0


def safe_event_json(value: object) -> dict[str, object]:
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def reserve_path_diagnosis(row: Mapping[str, object]) -> str:
    """Classify pricing-reserve availability without silently changing price semantics."""
    v_sol = optional_int(row.get("virtual_sol_reserves"))
    v_tok = optional_int(row.get("virtual_token_reserves"))
    v_quote = optional_int(row.get("virtual_quote_reserves"))

    if v_tok is None:
        return "VIRTUAL_TOKEN_MISSING"
    if v_tok <= 0:
        return "VIRTUAL_TOKEN_NONPOSITIVE"
    if v_sol is not None and v_sol > 0:
        return "SOL_RESERVE_PRICEABLE"
    if v_quote is not None and v_quote > 0:
        return "QUOTE_RESERVE_FALLBACK_CANDIDATE"
    if v_sol is None and v_quote is None:
        return "SOL_AND_QUOTE_RESERVES_MISSING"
    if v_quote is None:
        return "SOL_NONPOSITIVE_QUOTE_MISSING"
    return "SOL_AND_QUOTE_NONPOSITIVE"


def diagnose_unpriceable_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    t0_observed_at_us: int,
    entry_window_ms: int = ENTRY_WINDOW_MS,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    deadline = t0_observed_at_us + entry_window_ms * 1_000
    event_rows: list[dict[str, object]] = []
    diagnosis_counts: Counter[str] = Counter()
    quote_mints: Counter[str] = Counter()
    ix_names: Counter[str] = Counter()
    tail_decode: Counter[str] = Counter()

    for raw_row in rows:
        # sqlite3.Row supports key indexing but not dict.get(). Normalize it
        # at the DB boundary so the diagnostic helpers behave identically for
        # sqlite3.Row and ordinary dictionaries.
        row = dict(raw_row)
        observed_raw = row.get("decoded_at_utc")
        if observed_raw is None:
            continue
        observed_us = iso_to_us(str(observed_raw))
        if observed_us < t0_observed_at_us or observed_us > deadline:
            continue
        if str(row.get("event_type")) not in ("BUY", "SELL"):
            continue

        diagnosis = reserve_path_diagnosis(row)
        diagnosis_counts[diagnosis] += 1
        event_json = safe_event_json(row.get("event_json"))
        quote_mint = row.get("quote_mint") or event_json.get("quote_mint")
        ix_name = event_json.get("ix_name")
        full_tail = event_json.get("full_current_tail_decoded")

        if quote_mint:
            quote_mints[str(quote_mint)] += 1
        if ix_name:
            ix_names[str(ix_name)] += 1
        tail_decode[str(full_tail)] += 1

        event_rows.append(
            {
                "mint": str(row.get("mint") or ""),
                "event_key": str(row.get("event_key") or ""),
                "signature": str(row.get("signature") or ""),
                "event_type": str(row.get("event_type") or ""),
                "observed_at_utc": str(observed_raw),
                "observed_at_us": observed_us,
                "source_decoded_file": str(row.get("source_decoded_file") or ""),
                "diagnosis": diagnosis,
                "sol_amount_lamports": optional_int(row.get("sol_amount_lamports")),
                "quote_amount_raw": optional_int(row.get("quote_amount_raw")),
                "virtual_sol_reserves": optional_int(row.get("virtual_sol_reserves")),
                "virtual_token_reserves": optional_int(row.get("virtual_token_reserves")),
                "virtual_quote_reserves": optional_int(row.get("virtual_quote_reserves")),
                "real_sol_reserves": optional_int(row.get("real_sol_reserves")),
                "real_token_reserves": optional_int(row.get("real_token_reserves")),
                "real_quote_reserves": optional_int(row.get("real_quote_reserves")),
                "quote_mint": None if quote_mint is None else str(quote_mint),
                "ix_name": None if ix_name is None else str(ix_name),
                "full_current_tail_decoded": full_tail,
            }
        )

    total = len(event_rows)
    quote_candidate = diagnosis_counts.get("QUOTE_RESERVE_FALLBACK_CANDIDATE", 0)
    unresolved = total - diagnosis_counts.get("SOL_RESERVE_PRICEABLE", 0) - quote_candidate

    if total == 0:
        token_class = "NO_IN_WINDOW_TRADE_ROWS"
    elif quote_candidate == total:
        token_class = "ALL_TRADES_QUOTE_RESERVE_FALLBACK_CANDIDATE"
    elif quote_candidate > 0 and unresolved > 0:
        token_class = "MIXED_QUOTE_FALLBACK_AND_UNRESOLVED"
    elif unresolved == total:
        token_class = "NO_POSITIVE_RESERVE_PATH"
    else:
        token_class = "MIXED_OTHER"

    summary = {
        "event_count_5m": total,
        "diagnosis_counts": dict(sorted(diagnosis_counts.items())),
        "quote_candidate_events": quote_candidate,
        "unresolved_events": unresolved,
        "token_root_class": token_class,
        "distinct_quote_mints": dict(sorted(quote_mints.items())),
        "ix_name_counts": dict(sorted(ix_names.items())),
        "full_tail_decode_counts": dict(sorted(tail_decode.items())),
    }
    return event_rows, summary


def fetch_raw_rows_for_mint(
    conn: sqlite3.Connection,
    mint: str,
) -> list[sqlite3.Row]:
    where_source = "(" + " OR ".join(
        "source_decoded_file LIKE ?" for _ in BOT_TRUTH_SOURCE_PREFIXES
    ) + ")"
    source_args = tuple(prefix + "%" for prefix in BOT_TRUTH_SOURCE_PREFIXES)
    return conn.execute(
        f"""
        SELECT rowid AS p1_rowid, *
        FROM pump_events
        WHERE mint=?
          AND event_type IN ('LAUNCH','BUY','SELL')
          AND decoded_at_utc IS NOT NULL
          AND {where_source}
        ORDER BY rowid
        """,
        (mint, *source_args),
    ).fetchall()


def diagnose_unpriceable_tokens(
    conn: sqlite3.Connection,
    grouped: dict[str, list[NormalizedMarketEvent]],
    cohort_mints: Sequence[str],
    *,
    entry_window_ms: int = ENTRY_WINDOW_MS,
) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, object]]:
    token_rows: list[dict[str, object]] = []
    event_rows_all: list[dict[str, object]] = []
    token_classes: Counter[str] = Counter()
    quote_mint_tokens: dict[str, set[str]] = defaultdict(set)
    quote_mint_events: Counter[str] = Counter()
    diagnosis_events: Counter[str] = Counter()

    for mint in cohort_mints:
        trades = [
            e for e in grouped.get(mint, [])
            if e.event_type in (EventType.BUY, EventType.SELL)
        ]
        if not trades:
            continue
        t0 = trades[0].observed_at_us
        deadline = t0 + entry_window_ms * 1_000
        in_window = [e for e in trades if t0 <= e.observed_at_us <= deadline]
        if not in_window:
            continue
        # Root-cause target: no event has a positive SOL + token reserve path.
        if any(
            e.virtual_sol_reserve_lamports is not None
            and e.virtual_sol_reserve_lamports > 0
            and e.virtual_token_reserve_raw is not None
            and e.virtual_token_reserve_raw > 0
            for e in in_window
        ):
            continue

        raw_rows = fetch_raw_rows_for_mint(conn, mint)
        event_rows, summary = diagnose_unpriceable_rows(
            raw_rows,
            t0_observed_at_us=t0,
            entry_window_ms=entry_window_ms,
        )
        event_rows_all.extend(event_rows)
        token_class = str(summary["token_root_class"])
        token_classes[token_class] += 1

        for code, n in dict(summary["diagnosis_counts"]).items():
            diagnosis_events[str(code)] += int(n)
        for quote_mint, n in dict(summary["distinct_quote_mints"]).items():
            quote_mint_events[str(quote_mint)] += int(n)
            quote_mint_tokens[str(quote_mint)].add(mint)

        token_rows.append(
            {
                "mint": mint,
                "t0_observed_at_us": t0,
                "t0_observed_at_utc": us_to_iso(t0),
                "trade_events_5m": len(in_window),
                "root_class": token_class,
                "quote_candidate_events": int(summary["quote_candidate_events"]),
                "unresolved_events": int(summary["unresolved_events"]),
                "diagnosis_counts_json": canonical_json(summary["diagnosis_counts"]),
                "quote_mints_json": canonical_json(summary["distinct_quote_mints"]),
                "ix_names_json": canonical_json(summary["ix_name_counts"]),
                "tail_decode_json": canonical_json(summary["full_tail_decode_counts"]),
            }
        )

    summary = {
        "unpriceable_tokens": len(token_rows),
        "unpriceable_trade_events": len(event_rows_all),
        "token_root_class_counts": dict(sorted(token_classes.items())),
        "event_diagnosis_counts": dict(sorted(diagnosis_events.items())),
        "quote_mint_event_counts": dict(sorted(quote_mint_events.items())),
        "quote_mint_token_counts": {
            mint: len(tokens) for mint, tokens in sorted(quote_mint_tokens.items())
        },
        "all_quote_fallback_candidate_tokens": token_classes.get(
            "ALL_TRADES_QUOTE_RESERVE_FALLBACK_CANDIDATE", 0
        ),
    }
    return token_rows, event_rows_all, summary


@dataclass(frozen=True, slots=True)
class ControlEvent:
    id: int
    event_type: str
    happened_at_us: int
    details: dict[str, object]


@dataclass(frozen=True, slots=True)
class ActiveInterval:
    session_id: int
    start_us: int
    end_us: int
    close_reason: str
    uncertain_close: bool


def safe_details(value: object) -> dict[str, object]:
    try:
        parsed = json.loads(str(value))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def read_control_events(conn: sqlite3.Connection) -> list[ControlEvent]:
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='collector_events'"
    ).fetchone()
    if exists is None:
        raise ValueError("collector_events table missing")
    placeholders = ",".join("?" for _ in CONTROL_EVENT_TYPES)
    rows = conn.execute(
        f"""
        SELECT id,event_type,happened_at_utc,details_json
        FROM collector_events
        WHERE event_type IN ({placeholders})
        ORDER BY id
        """,
        CONTROL_EVENT_TYPES,
    ).fetchall()
    return [
        ControlEvent(
            id=int(r["id"]),
            event_type=str(r["event_type"]),
            happened_at_us=iso_to_us(str(r["happened_at_utc"])),
            details=safe_details(r["details_json"]),
        )
        for r in rows
    ]


def nearest_observation_before_us(
    conn: sqlite3.Connection,
    cutoff_us: int,
    *,
    after_us: int | None = None,
) -> int | None:
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='websocket_observations'"
    ).fetchone()
    if exists is None:
        return None
    # A very large sentinel cutoff means "latest stored observation".  This keeps
    # abrupt-EOF recovery safe without attempting to convert an out-of-range
    # timestamp to a Python datetime.
    if cutoff_us >= 100_000_000_000_000_000:
        if after_us is None:
            row = conn.execute(
                """
                SELECT received_at_utc
                FROM websocket_observations
                ORDER BY received_at_utc DESC
                LIMIT 1
                """
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT received_at_utc
                FROM websocket_observations
                WHERE received_at_utc >= ?
                ORDER BY received_at_utc DESC
                LIMIT 1
                """,
                (us_to_iso(after_us),),
            ).fetchone()
    else:
        cutoff_iso = us_to_iso(cutoff_us)
        if after_us is None:
            row = conn.execute(
                """
                SELECT received_at_utc
                FROM websocket_observations
                WHERE received_at_utc <= ?
                ORDER BY received_at_utc DESC
                LIMIT 1
                """,
                (cutoff_iso,),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT received_at_utc
                FROM websocket_observations
                WHERE received_at_utc <= ?
                  AND received_at_utc >= ?
                ORDER BY received_at_utc DESC
                LIMIT 1
                """,
                (cutoff_iso, us_to_iso(after_us)),
            ).fetchone()
    return None if row is None else iso_to_us(str(row["received_at_utc"]))


def infer_collector_version(details: Mapping[str, object]) -> str:
    raw_path = str(details.get("raw_path") or "")
    if "v0_3_3" in raw_path:
        return "v0.3.3"
    if "v0_3_2" in raw_path:
        return "v0.3.2"
    if "v0_3" in raw_path:
        return "v0.3"
    return "v0.3.x"


def reconstruct_collector_coverage(
    events: Sequence[ControlEvent],
    *,
    nearest_before: Callable[[int, int | None], int | None] | None = None,
) -> tuple[list[ActiveInterval], list[dict[str, object]], list[dict[str, object]], dict[str, object]]:
    intervals: list[ActiveInterval] = []
    session_rows: list[dict[str, object]] = []
    gap_rows: list[dict[str, object]] = []

    session_id = 0
    session_start: int | None = None
    session_version = "UNKNOWN"
    active_start: int | None = None
    session_interval_start_idx = 0
    session_gap_start_idx = 0
    session_stop: int | None = None
    session_stop_kind: str | None = None

    def close_active(end_us: int, reason: str, uncertain: bool) -> None:
        nonlocal active_start
        if active_start is None:
            return
        end = max(active_start, end_us)
        intervals.append(
            ActiveInterval(
                session_id=session_id,
                start_us=active_start,
                end_us=end,
                close_reason=reason,
                uncertain_close=uncertain,
            )
        )
        active_start = None

    def finalize_session(stop_us: int | None, stop_kind: str | None) -> None:
        nonlocal session_start, session_stop, session_stop_kind, session_interval_start_idx, session_gap_start_idx
        if session_start is None:
            return
        session_intervals = intervals[session_interval_start_idx:]
        session_gaps = gap_rows[session_gap_start_idx:]
        active_us = sum(max(0, i.end_us - i.start_us) for i in session_intervals)
        explicit_gap_us = sum(int(g["gap_duration_us"]) for g in session_gaps)
        session_rows.append(
            {
                "session_id": session_id,
                "version": session_version,
                "start_at_us": session_start,
                "start_at_utc": us_to_iso(session_start),
                "stop_at_us": stop_us,
                "stop_at_utc": us_to_iso(stop_us),
                "stop_kind": stop_kind,
                "active_intervals": len(session_intervals),
                "active_seconds": active_us / 1_000_000,
                "explicit_gap_count": len(session_gaps),
                "explicit_gap_seconds": explicit_gap_us / 1_000_000,
                "uncertain_interval_closes": sum(i.uncertain_close for i in session_intervals),
            }
        )
        session_start = None
        session_stop = None
        session_stop_kind = None

    for ev in events:
        et = ev.event_type
        t = ev.happened_at_us

        if et == "COLLECTOR_V0_3_START":
            if session_start is not None:
                if active_start is not None:
                    fallback = nearest_before(t, active_start) if nearest_before else None
                    close_at = fallback if fallback is not None and fallback >= active_start else t
                    close_active(close_at, "NEXT_COLLECTOR_START", True)
                finalize_session(t, "NEXT_COLLECTOR_START")
            session_id += 1
            session_start = t
            session_version = infer_collector_version(ev.details)
            session_interval_start_idx = len(intervals)
            session_gap_start_idx = len(gap_rows)
            continue

        if session_start is None:
            continue

        if et == "PUMP_SUBSCRIPTION_ACTIVE":
            if active_start is not None:
                close_active(t, "SUBSCRIPTION_REPLACED", True)
            active_start = t
        elif et == "WS_DISCONNECTED":
            close_active(t, "WS_DISCONNECTED", False)
        elif et == "DATA_GAP_CLOSED":
            gs = ev.details.get("gap_started_at_utc")
            ge = ev.details.get("gap_ended_at_utc")
            if gs and ge:
                start_us = iso_to_us(str(gs))
                end_us = iso_to_us(str(ge))
                duration_us = max(0, end_us - start_us)
            else:
                seconds = float(ev.details.get("gap_seconds") or 0.0)
                end_us = t
                start_us = int(t - seconds * 1_000_000)
                duration_us = max(0, end_us - start_us)
            gap_rows.append(
                {
                    "session_id": session_id,
                    "gap_start_us": start_us,
                    "gap_start_utc": us_to_iso(start_us),
                    "gap_end_us": end_us,
                    "gap_end_utc": us_to_iso(end_us),
                    "gap_duration_us": duration_us,
                    "gap_seconds": duration_us / 1_000_000,
                }
            )
        elif et == "LIVE_COLLECTION_STOPPED":
            if active_start is not None:
                close_active(t, "LIVE_COLLECTION_STOPPED", False)
            finalize_session(t, "LIVE_COLLECTION_STOPPED")

    if session_start is not None:
        if active_start is not None:
            fallback = nearest_before(10**18, active_start) if nearest_before else None
            close_at = fallback if fallback is not None and fallback >= active_start else active_start
            close_active(close_at, "EOF_LAST_OBSERVATION", True)
        stop = intervals[-1].end_us if intervals and intervals[-1].session_id == session_id else session_start
        finalize_session(stop, "EOF_UNCERTAIN")

    summary = {
        "control_events": len(events),
        "sessions": len(session_rows),
        "active_intervals": len(intervals),
        "explicit_gaps": len(gap_rows),
        "active_seconds_total": sum(max(0, i.end_us - i.start_us) for i in intervals) / 1_000_000,
        "explicit_gap_seconds_total": sum(int(g["gap_duration_us"]) for g in gap_rows) / 1_000_000,
        "uncertain_interval_closes": sum(i.uncertain_close for i in intervals),
        "session_versions": dict(sorted(Counter(str(s["version"]) for s in session_rows).items())),
    }
    return intervals, session_rows, gap_rows, summary


def overlap_us(a0: int, a1: int, b0: int, b1: int) -> int:
    return max(0, min(a1, b1) - max(a0, b0))


def floor_bin_us(t_us: int, minutes: int = BIN_MINUTES) -> int:
    width = minutes * 60 * 1_000_000
    return (t_us // width) * width


def build_coverage_bins(
    intervals: Sequence[ActiveInterval],
    gap_rows: Sequence[Mapping[str, object]],
    t0_values_us: Sequence[int],
    *,
    bin_minutes: int = BIN_MINUTES,
) -> list[dict[str, object]]:
    if not t0_values_us:
        return []
    width = bin_minutes * 60 * 1_000_000
    start = floor_bin_us(min(t0_values_us), bin_minutes)
    end = floor_bin_us(max(t0_values_us), bin_minutes) + width
    token_counts: Counter[int] = Counter(floor_bin_us(t, bin_minutes) for t in t0_values_us)

    out: list[dict[str, object]] = []
    cursor = start
    while cursor < end:
        active = sum(overlap_us(i.start_us, i.end_us, cursor, cursor + width) for i in intervals)
        explicit_gap = sum(
            overlap_us(
                int(g["gap_start_us"]),
                int(g["gap_end_us"]),
                cursor,
                cursor + width,
            )
            for g in gap_rows
        )
        n = token_counts.get(cursor, 0)
        active_seconds = active / 1_000_000
        active_minutes = active_seconds / 60
        coverage_pct = min(100.0, 100.0 * active / width)
        if coverage_pct >= 95:
            coverage_class = "FULL"
        elif coverage_pct >= 50:
            coverage_class = "PARTIAL"
        elif coverage_pct > 0:
            coverage_class = "LOW"
        else:
            coverage_class = "OFFLINE"
        out.append(
            {
                "bin_start_us": cursor,
                "bin_start_utc": us_to_iso(cursor),
                "bin_end_us": cursor + width,
                "bin_end_utc": us_to_iso(cursor + width),
                "tokens": n,
                "active_seconds": round(active_seconds, 6),
                "active_minutes": round(active_minutes, 6),
                "active_coverage_pct": round(coverage_pct, 3),
                "coverage_class": coverage_class,
                "explicit_gap_seconds": round(explicit_gap / 1_000_000, 6),
                "tokens_per_active_minute": None if active_minutes <= 0 else n / active_minutes,
            }
        )
        cursor += width
    return out


def build_root_cause_payload(
    *,
    unpriceable_summary: Mapping[str, object],
    coverage_summary: Mapping[str, object],
    coverage_bins: Sequence[Mapping[str, object]],
    session_rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    core = {
        "schema_version": SCHEMA_VERSION,
        "purpose": "UNPRICEABLE TOKEN ROOT CAUSE + COLLECTOR COVERAGE AUDIT",
        "performance_pnl_included": False,
        "parameter_optimization_performed": False,
        "unpriceable_summary": dict(unpriceable_summary),
        "collector_coverage_summary": dict(coverage_summary),
        "coverage_bins": [dict(r) for r in coverage_bins],
        "collector_sessions": [dict(r) for r in session_rows],
    }
    core["deterministic_payload_sha256"] = stable_sha256(core)
    return core
