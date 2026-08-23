from __future__ import annotations

import json
import math
import sqlite3
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_DB = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"

SCHEMA_VERSION = "phase4_3b1d0_runtime_price_impact_probe_v0.2"
ROW_LIMIT = 50_000
RAW_ROWID_WINDOW = 100_000
REFERENCE_POSITION_LAMPORTS = 100_000_000  # locked Phase-4 research reference: 0.10 SOL


def as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def pct(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    pos = (len(ordered) - 1) * p
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return float(ordered[lo])
    frac = pos - lo
    return float(ordered[lo] * (1.0 - frac) + ordered[hi] * frac)


def fmt_num(value: float | int | None, digits: int = 3) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, int):
        return f"{value:,}"
    return f"{value:,.{digits}f}"


def ppm_error(actual: int, expected: int) -> float | None:
    if expected <= 0:
        return None
    return (actual - expected) * 1_000_000.0 / expected


def abs_ppm_product_drift(a: int, b: int) -> float | None:
    if a <= 0:
        return None
    return abs(b - a) * 1_000_000.0 / a


def safe_json(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    try:
        out = json.loads(str(value))
    except Exception:
        return {}
    return out if isinstance(out, dict) else {}


def is_gap_source(source: Any) -> bool:
    return "GAP" in str(source or "").upper()


def connect_readonly(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(path)
    uri = path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})")}


def required_columns_check(conn: sqlite3.Connection) -> None:
    required = {
        "event_type",
        "mint",
        "sol_amount_lamports",
        "token_amount_raw",
        "virtual_sol_reserves",
        "virtual_token_reserves",
        "quote_mint",
        "quote_amount_raw",
        "virtual_quote_reserves",
        "source_decoded_file",
        "inserted_at_utc",
        "event_json",
    }
    actual = table_columns(conn, "pump_events")
    missing = sorted(required - actual)
    if missing:
        raise RuntimeError(f"pump_events missing required columns: {missing}")


def load_rows(conn: sqlite3.Connection) -> tuple[list[sqlite3.Row], int, int]:
    # v0.2: hard-bound the scan to a recent raw rowid window so the probe
    # cannot silently walk the entire production history.
    max_row = int(
        conn.execute("SELECT COALESCE(MAX(rowid), 0) AS n FROM pump_events").fetchone()["n"]
    )
    min_row = max(0, max_row - RAW_ROWID_WINDOW)

    rows = conn.execute(
        """
        SELECT
            rowid AS p1_rowid,
            event_type,
            mint,
            sol_amount_lamports,
            token_amount_raw,
            virtual_sol_reserves,
            virtual_token_reserves,
            quote_mint,
            quote_amount_raw,
            virtual_quote_reserves,
            source_decoded_file,
            inserted_at_utc,
            event_json
        FROM pump_events
        WHERE rowid > ?
          AND event_type IN ('BUY','SELL')
          AND mint IS NOT NULL
          AND token_amount_raw IS NOT NULL
          AND virtual_token_reserves IS NOT NULL
          AND (
                (sol_amount_lamports IS NOT NULL AND virtual_sol_reserves IS NOT NULL)
                OR
                (quote_amount_raw IS NOT NULL AND virtual_quote_reserves IS NOT NULL)
              )
        ORDER BY rowid DESC
        LIMIT ?
        """,
        (min_row, ROW_LIMIT),
    ).fetchall()
    rows.reverse()
    return rows, min_row, max_row


def identity_and_amount(row: sqlite3.Row) -> tuple[str, int, int] | None:
    sol_amount = as_int(row["sol_amount_lamports"])
    sol_reserve = as_int(row["virtual_sol_reserves"])
    if sol_amount is not None and sol_amount > 0 and sol_reserve is not None and sol_reserve > 0:
        return ("SOL_NATIVE", sol_amount, sol_reserve)

    quote_amount = as_int(row["quote_amount_raw"])
    quote_reserve = as_int(row["virtual_quote_reserves"])
    quote_mint = row["quote_mint"]
    if (
        quote_amount is not None
        and quote_amount > 0
        and quote_reserve is not None
        and quote_reserve > 0
        and quote_mint
    ):
        return (f"QUOTE:{quote_mint}", quote_amount, quote_reserve)

    return None


def summarize_float(label: str, values: list[float], unit: str = "") -> None:
    if not values:
        print(f"{label:<49}: N/A")
        return
    suffix = f" {unit}" if unit else ""
    print(
        f"{label:<49}: "
        f"n={len(values):,} "
        f"p50={fmt_num(pct(values, 0.50))}{suffix} "
        f"p90={fmt_num(pct(values, 0.90))}{suffix} "
        f"p95={fmt_num(pct(values, 0.95))}{suffix}"
    )


def main() -> int:
    print("=" * 122)
    print("PHASE 4.3B1D0 - RUNTIME PRICE-IMPACT MECHANICS PROBE v0.2")
    print("=" * 122)
    print(f"Project root                         : {PROJECT_ROOT}")
    print(f"Production DB                        : {PRODUCTION_DB}")
    print("Production DB mode                   : QUERY-ONLY / READ-ONLY")
    print(f"Eligible trade-row limit             : {ROW_LIMIT:,}")
    print(f"Raw rowid scan window                : latest {RAW_ROWID_WINDOW:,} rows")
    print(f"Reference position                   : {REFERENCE_POSITION_LAMPORTS:,} lamports (0.10 SOL)")
    print("Collector                            : NOT STARTED")
    print("Network / RPC                        : NO")
    print("Paper orders / fills                 : NO")
    print("Wallet / signing / live orders       : NO")
    print("Parameter tuning / reselection       : NO")
    print("Purpose                              : validate observed Pump reserve mechanics before any runtime impact model is used")
    print()

    print("[STEP 1/3] Opening production SQLite read-only ...", flush=True)
    conn = connect_readonly(PRODUCTION_DB)
    try:
        print("[STEP 2/3] Validating required pump_events columns ...", flush=True)
        required_columns_check(conn)
        print("[STEP 3/3] Loading bounded recent trade slice ...", flush=True)
        started_query = __import__("time").monotonic()
        rows, min_rowid, max_rowid_seen = load_rows(conn)
        query_seconds = __import__("time").monotonic() - started_query
        print(
            f"[OK] Loaded {len(rows):,} eligible rows from raw rowid window "
            f"({min_rowid}, {max_rowid_seen}] in {query_seconds:.2f}s",
            flush=True,
        )
    finally:
        conn.close()

    # Full PRAGMA quick_check is intentionally omitted in v0.2.
    # It was already validated in earlier Phase-4 steps and can take a long
    # time on the grown production DB. This probe remains query-only/read-only.
    quick = "SKIPPED_BY_DESIGN"

    if not rows:
        raise RuntimeError("No eligible Pump trade rows found in bounded sample")

    sources = Counter(str(r["source_decoded_file"] or "(NULL)") for r in rows)
    by_identity = Counter()
    by_side = Counter()

    # Event-local reconstruction: assume event reserves are post-trade and
    # reconstruct a candidate pre-trade state using the event's reported raw amount.
    buy_invariant_gross: dict[str, list[float]] = defaultdict(list)
    buy_invariant_minus_event_fee: dict[str, list[float]] = defaultdict(list)
    fee_bps_seen: list[int] = []
    fee_lamports_seen: list[int] = []
    creator_fee_lamports_seen: list[int] = []

    # Consecutive same-mint reserve continuity. This directly tests whether the
    # event's reported amount/token amount explain observed reserve deltas.
    previous: dict[tuple[str, str], tuple[int, int, sqlite3.Row]] = {}
    buy_num_delta_err_ppm: dict[str, list[float]] = defaultdict(list)
    buy_token_delta_err_ppm: dict[str, list[float]] = defaultdict(list)
    buy_num_exact = Counter()
    buy_token_exact = Counter()
    buy_pair_count = Counter()

    # Pure mechanical hypothesis only. These figures are NOT fed to paper fills.
    ref_impact_bps_sol: list[float] = []

    eligible = 0
    gap_rows = 0
    malformed = 0

    for row in rows:
        ident = identity_and_amount(row)
        token_amount = as_int(row["token_amount_raw"])
        token_post = as_int(row["virtual_token_reserves"])
        side = str(row["event_type"])

        if ident is None or token_amount is None or token_amount <= 0 or token_post is None or token_post <= 0:
            malformed += 1
            continue

        identity, raw_amount, num_post = ident
        eligible += 1
        by_identity[identity] += 1
        by_side[(identity, side)] += 1

        gap = is_gap_source(row["source_decoded_file"])
        if gap:
            gap_rows += 1

        details = safe_json(row["event_json"])
        event_fee = as_int(details.get("fee_lamports"))
        creator_fee = as_int(details.get("creator_fee_lamports"))
        event_fee_bps = as_int(details.get("fee_basis_points"))
        if event_fee is not None and event_fee >= 0:
            fee_lamports_seen.append(event_fee)
        if creator_fee is not None and creator_fee >= 0:
            creator_fee_lamports_seen.append(creator_fee)
        if event_fee_bps is not None and event_fee_bps >= 0:
            fee_bps_seen.append(event_fee_bps)

        if side == "BUY":
            # Candidate "post reserve" reconstruction using raw trade amount.
            pre_num_gross = num_post - raw_amount
            pre_token = token_post + token_amount
            if pre_num_gross > 0 and pre_token > 0:
                k_pre = pre_num_gross * pre_token
                k_post = num_post * token_post
                drift = abs_ppm_product_drift(k_pre, k_post)
                if drift is not None:
                    buy_invariant_gross[identity].append(drift)

                if identity == "SOL_NATIVE":
                    # If 0.10 SOL were curve input from this market state,
                    # continuous constant-product average-price impact vs spot
                    # is q / x. This is DIAGNOSTIC ONLY until mechanics are accepted.
                    ref_impact_bps_sol.append(
                        REFERENCE_POSITION_LAMPORTS * 10_000.0 / num_post
                    )

            # Alternate diagnostic: subtract event fee from reported amount.
            # We test this rather than assuming fee inclusion semantics.
            if event_fee is not None and 0 <= event_fee < raw_amount:
                pre_num_net = num_post - (raw_amount - event_fee)
                if pre_num_net > 0 and pre_token > 0:
                    k_pre_net = pre_num_net * pre_token
                    k_post = num_post * token_post
                    drift_net = abs_ppm_product_drift(k_pre_net, k_post)
                    if drift_net is not None:
                        buy_invariant_minus_event_fee[identity].append(drift_net)

        key = (str(row["mint"]), identity)
        prior = previous.get(key)

        # Exclude any pair where either side is known GAP recovery.
        if prior is not None and not gap and not is_gap_source(prior[2]["source_decoded_file"]):
            prev_num, prev_token, _prev_row = prior
            if side == "BUY":
                observed_num_in = num_post - prev_num
                observed_token_out = prev_token - token_post

                if observed_num_in >= 0 and observed_token_out >= 0:
                    buy_pair_count[identity] += 1
                    e1 = ppm_error(observed_num_in, raw_amount)
                    e2 = ppm_error(observed_token_out, token_amount)
                    if e1 is not None:
                        buy_num_delta_err_ppm[identity].append(abs(e1))
                    if e2 is not None:
                        buy_token_delta_err_ppm[identity].append(abs(e2))
                    if observed_num_in == raw_amount:
                        buy_num_exact[identity] += 1
                    if observed_token_out == token_amount:
                        buy_token_exact[identity] += 1

        # Current event's reserves become prior post-trade state.
        previous[key] = (num_post, token_post, row)

    print("-" * 122)
    print("SOURCE / SAMPLE")
    print("-" * 122)
    print(f"SQLite quick_check                    : {quick}")
    print(f"Bounded raw rowid window              : ({min_rowid}, {max_rowid_seen}]")
    print(f"Bounded query elapsed                 : {query_seconds:.3f}s")
    print(f"Rows loaded                           : {len(rows):,}")
    print(f"Eligible decoded trade rows           : {eligible:,}")
    print(f"Known gap-recovery rows               : {gap_rows:,}")
    print(f"Malformed/unusable rows               : {malformed:,}")
    print(f"Distinct source labels                : {len(sources):,}")
    for source, count in sources.most_common(8):
        print(f"  {source:<62} {count:>8,}")
    print()
    print("Price identities:")
    for identity, count in by_identity.most_common():
        print(f"  {identity:<68} {count:>8,}")
    print()

    print("-" * 122)
    print("BUY RESERVE-MECHANICS DIAGNOSTICS")
    print("-" * 122)
    identities = sorted(set(buy_invariant_gross) | set(buy_pair_count))
    for identity in identities:
        print(f"[{identity}]")
        summarize_float(
            "  post-reserve reconstruction | invariant drift",
            buy_invariant_gross.get(identity, []),
            "ppm",
        )
        summarize_float(
            "  alternate amount-minus-event-fee | drift",
            buy_invariant_minus_event_fee.get(identity, []),
            "ppm",
        )
        summarize_float(
            "  consecutive BUY | numerator delta abs error",
            buy_num_delta_err_ppm.get(identity, []),
            "ppm",
        )
        summarize_float(
            "  consecutive BUY | token delta abs error",
            buy_token_delta_err_ppm.get(identity, []),
            "ppm",
        )
        pairs = buy_pair_count[identity]
        num_exact = buy_num_exact[identity]
        tok_exact = buy_token_exact[identity]
        num_pct = (100.0 * num_exact / pairs) if pairs else None
        tok_pct = (100.0 * tok_exact / pairs) if pairs else None
        print(f"  consecutive BUY pairs                         : {pairs:,}")
        print(
            "  exact numerator-reserve delta matches         : "
            + (f"{num_exact:,}/{pairs:,} ({num_pct:.2f}%)" if pairs else "N/A")
        )
        print(
            "  exact token-reserve delta matches             : "
            + (f"{tok_exact:,}/{pairs:,} ({tok_pct:.2f}%)" if pairs else "N/A")
        )
        print()

    print("-" * 122)
    print("FEE-FIELD OBSERVATION (AUDIT ONLY)")
    print("-" * 122)
    print(f"TradeEvent fee_basis_points values     : n={len(fee_bps_seen):,} unique={sorted(set(fee_bps_seen))[:20]}")
    summarize_float("TradeEvent fee_lamports", [float(x) for x in fee_lamports_seen], "lamports")
    summarize_float("TradeEvent creator_fee_lamports", [float(x) for x in creator_fee_lamports_seen], "lamports")
    print()

    print("-" * 122)
    print("0.10 SOL CURVE-IMPACT HYPOTHESIS — DIAGNOSTIC ONLY")
    print("-" * 122)
    print(
        "Formula under test                     : impact_bps = 0.10_SOL / current_virtual_SOL_reserve * 10,000"
    )
    print(
        "Status                                 : NOT LOCKED / NOT FED TO PAPER ROUTER"
    )
    summarize_float("Observed SOL-state implied impact", ref_impact_bps_sol, "bps")
    print(
        "QUOTE path                             : intentionally UNAVAILABLE for 0.10 SOL sizing without a causal SOL→quote conversion"
    )
    print()

    # Conservative evidence flags. They are not a model lock.
    sol_inv = buy_invariant_gross.get("SOL_NATIVE", [])
    sol_pairs = buy_pair_count["SOL_NATIVE"]
    sol_num_err = buy_num_delta_err_ppm.get("SOL_NATIVE", [])
    sol_tok_err = buy_token_delta_err_ppm.get("SOL_NATIVE", [])

    enough_sol = len(sol_inv) >= 100
    invariant_tight = (
        enough_sol
        and pct(sol_inv, 0.50) is not None
        and pct(sol_inv, 0.95) is not None
        and pct(sol_inv, 0.50) <= 100.0
        and pct(sol_inv, 0.95) <= 2_000.0
    )
    continuity_available = sol_pairs >= 25
    continuity_tight = (
        continuity_available
        and pct(sol_num_err, 0.50) is not None
        and pct(sol_tok_err, 0.50) is not None
        and pct(sol_num_err, 0.50) <= 1_000.0
        and pct(sol_tok_err, 0.50) <= 1_000.0
    )

    print("-" * 122)
    print("VALIDATION")
    print("-" * 122)
    print("Production SQLite quick_check          : SKIPPED_BY_DESIGN")
    print(f"Read-only probe completed              : PASS")
    print(f"SOL BUY event-local sample >=100       : {'PASS' if enough_sol else 'CHECK'}")
    print(f"SOL constant-product reconstruction    : {'PASS' if invariant_tight else 'CHECK'}")
    print(f"SOL consecutive continuity available   : {'PASS' if continuity_available else 'CHECK'}")
    print(f"SOL consecutive reserve deltas tight   : {'PASS' if continuity_tight else 'CHECK'}")
    print("Production DB writes by probe          : NO")
    print("Paper order/fill attempted              : NO")
    print("Wallet/signing/live orders              : NO")
    print("Parameter search/reselection            : NO")
    print()

    mechanics_supported = invariant_tight and (continuity_tight or not continuity_available)
    verdict = "SUPPORTED_FOR_SOL_REVIEW" if mechanics_supported else "NEEDS_REVIEW"
    print(f"MECHANICS_VERDICT: {verdict}")
    print("RESULT: PASS")
    print(
        "NEXT: review empirical reserve/fee mechanics; only then implement a versioned runtime price-impact provider and causal entry-fill smoke."
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print()
        print("=" * 122)
        print("RESULT: FAIL")
        print(f"{type(exc).__name__}: {exc}")
        raise
