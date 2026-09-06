from __future__ import annotations

import ast
import hashlib
import json
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
SCRIPTS_ROOT = PROJECT_ROOT / "scripts"
for import_root in (SRC_ROOT, SCRIPTS_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from phase4.post24h_counterfactual_replay_v0_1 import (  # noqa: E402
    CounterfactualMarketObservationV01,
)
from phase6 import frozen_oos_hypothesis_evaluation_v0_1 as oos  # noqa: E402
import phase4_continuous_market_source_selftest_v0_2 as source_fixture  # noqa: E402


BASE = datetime(2026, 9, 7, tzinfo=timezone.utc)
FAKE_SHA = "0" * 64


def _window(*, days: int = 3) -> oos.FrozenOOSWindowV01:
    return oos.FrozenOOSWindowV01(
        start_at=BASE,
        end_at=BASE + timedelta(days=days),
        source_start_after_ingest_seq=1,
        source_end_ingest_seq=1_000,
        source_db_sha256=FAKE_SHA,
    )


def _candidate(
    signal_at: datetime,
    *,
    signal_key: str = "signal-1",
    fill_at: datetime | None = None,
    adverse_bps: int | None = 600,
    state: str = "FILLED",
) -> oos.FrozenOOSCandidateV01:
    actual_fill = fill_at or signal_at + timedelta(seconds=1)
    filled = state == "FILLED"
    return oos.FrozenOOSCandidateV01(
        route_id=f"route-{signal_key}",
        signal_key=signal_key,
        mint=f"mint-{signal_key}",
        strategy_version="v1.1",
        parameter_set_id=oos.LOCKED_PARAMETER_SET_BY_ROLE["CONTROL"],
        role="CONTROL",
        candidate_source_event_key="signal-source:CANDIDATE:CONTROL",
        signal_observed_at=signal_at.isoformat(timespec="microseconds"),
        signal_ingest_seq=10,
        reference_price_identity="SOL_NATIVE",
        reference_price_numerator_raw=100,
        reference_price_denominator_raw=1,
        requested_size_lamports=1_000_000,
        execution_ready_at=(signal_at + timedelta(milliseconds=500)).isoformat(
            timespec="microseconds"
        ),
        entry_state=state,
        state_reason="ENTRY_FILLED" if filled else "ENTRY_SLIPPAGE_REJECTED",
        selected_market_observed_at=(
            actual_fill.isoformat(timespec="microseconds") if filled else None
        ),
        selected_market_ingest_seq=11 if filled else None,
        selected_source_event_key="entry:MARKET" if filled else None,
        simulated_entry_price_numerator_raw=100 if filled else None,
        simulated_entry_price_denominator_raw=1 if filled else None,
        total_entry_explicit_cost_lamports=2_250_000 if filled else None,
        entry_adverse_slippage_bps=adverse_bps,
    )


def _metrics(**overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "eligible_entry_count": 150,
        "fill_rate": "0.950000000000",
        "net_pnl_lamports": 1_000,
        "expectancy_per_filled_trade_lamports": "10.000000000000",
        "profit_factor": "1.200000000000",
        "max_realized_closed_equity_drawdown_lamports": 1_000,
        "complete_utc_days": 3,
        "positive_complete_utc_days": 2,
    }
    values.update(overrides)
    return values


def _assert_raises(call: Callable[[], Any], expected: type[BaseException]) -> None:
    try:
        call()
    except expected:
        return
    raise AssertionError(f"expected {expected.__name__}")


def _synthetic_row(
    *,
    classification: str,
    net: int | None,
    signal_at: datetime,
    seq: int,
) -> dict[str, Any]:
    filled = classification == "FILLED"
    return {
        "classification": classification,
        "rejection_classification": None if filled else "NO_CAUSAL_FILL",
        "exit_reason": "TAKE_PROFIT" if filled else "FALLBACK",
        "signal_key": f"signal-{seq}",
        "signal_observed_at": signal_at.isoformat(timespec="microseconds"),
        "entry_state": "FILLED",
        "attempted": True,
        "exit_fill_observed_at": (
            (signal_at + timedelta(seconds=2)).isoformat(timespec="microseconds")
            if filled else None
        ),
        "exit_fill_source_row": seq if filled else None,
        "gross_execution_pnl_lamports": (net + 20 if filled and net is not None else None),
        "entry_explicit_cost_lamports": 10,
        "exit_explicit_cost_lamports": 10 if filled else None,
        "total_explicit_cost_lamports": 20 if filled else 10,
        "net_pnl_lamports": net,
        "holding_time_us": 2_000_000 if filled else None,
    }


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _test_boundaries(checks: dict[str, bool]) -> None:
    h1 = oos.POLICIES["H1_SIMPLE_TIME_TP10_T30"]
    checks["01_H1_includes_00_00_00_UTC"] = oos.candidate_is_eligible_v0_1(
        _candidate(BASE), h1
    )
    checks["02_H1_includes_05_59_59_fractional_UTC"] = oos.candidate_is_eligible_v0_1(
        _candidate(BASE + timedelta(hours=5, minutes=59, seconds=59, microseconds=999999)), h1
    )
    checks["03_H1_excludes_06_00_00_UTC"] = not oos.candidate_is_eligible_v0_1(
        _candidate(BASE + timedelta(hours=6)), h1
    )
    checks["04_H1_excludes_23_59_59_UTC"] = not oos.candidate_is_eligible_v0_1(
        _candidate(BASE + timedelta(hours=23, minutes=59, seconds=59)), h1
    )
    cross_bucket = _candidate(BASE + timedelta(hours=5, minutes=59), fill_at=BASE + timedelta(hours=6, minutes=1))
    checks["05_H1_uses_signal_time_when_fill_in_other_bucket"] = (
        oos.candidate_is_eligible_v0_1(cross_bucket, h1)
    )
    changed_post_entry = _candidate(
        BASE + timedelta(hours=5, minutes=59),
        fill_at=BASE + timedelta(hours=22),
        adverse_bps=0,
    )
    checks["06_post_entry_timestamps_and_outcomes_cannot_change_H1_membership"] = (
        oos.candidate_is_eligible_v0_1(changed_post_entry, h1)
        and oos.candidate_is_eligible_v0_1(cross_bucket, h1)
    )


def _test_policy_firewall(checks: dict[str, bool]) -> None:
    diagnostics = (
        "H2_TIME_OR_Q4_TP20_T30",
        "CTRL_Q4_TP20_T30",
        "H3_EXCLUSION_TRAIL10_GB3_T30",
    )
    checks["07_outcome_filtered_policies_are_explicit_nonpromotable_diagnostics"] = all(
        oos.POLICIES[name].classification == oos.DIAGNOSTIC_CLASSIFICATION
        and not oos.POLICIES[name].promotion_eligible
        for name in diagnostics
    )
    bad_exit = oos._exit_policy("BAD_DIAGNOSTIC", tp_bps=1_000)
    blocked = False
    try:
        oos.FrozenOOSPolicyV01(
            "BAD_DIAGNOSTIC",
            oos.ELIGIBILITY_Q4,
            bad_exit,
            oos.PROMOTION_CLASSIFICATION,
            True,
        )
    except ValueError:
        blocked = True
    checks["08_structural_diagnostic_promotion_attempt_fails_closed"] = blocked
    registry_immutable = False
    try:
        oos.POLICIES["MUTATION"] = oos.POLICIES["CTRL_ALL_TP10_T30"]  # type: ignore[index]
    except TypeError:
        registry_immutable = True
    checks["08b_frozen_policy_registry_is_runtime_immutable"] = registry_immutable
    review = oos.protocol_review_payload_v0_1()
    checks["09_matched_control_differs_from_H1_only_by_time_cohort_gate"] = (
        review["h1_and_matched_control_exit_contract_exact"]
        and review["h1_and_control_only_intended_cohort_difference"]
    )
    checks["10_H1_has_no_adverse_slippage_input"] = (
        oos.POLICIES["H1_SIMPLE_TIME_TP10_T30"].eligibility_rule == oos.ELIGIBILITY_H1_TIME
        and "entry_adverse_slippage_bps" in oos.PROTOCOL_DEFINITION["h1_forbidden_inputs"]
    )
    q3 = _candidate(BASE + timedelta(hours=8), adverse_bps=585)
    q4 = _candidate(BASE + timedelta(hours=8), adverse_bps=586)
    checks["10b_frozen_adverse_quartile_boundaries_are_exact"] = (
        not oos.candidate_is_eligible_v0_1(q3, oos.POLICIES["CTRL_Q4_TP20_T30"])
        and oos.candidate_is_eligible_v0_1(q4, oos.POLICIES["CTRL_Q4_TP20_T30"])
        and not oos.candidate_is_eligible_v0_1(q3, oos.POLICIES["H3_EXCLUSION_TRAIL10_GB3_T30"])
        and oos.candidate_is_eligible_v0_1(q4, oos.POLICIES["H3_EXCLUSION_TRAIL10_GB3_T30"])
    )


def _test_gates(checks: dict[str, bool]) -> None:
    window = _window()
    control = _metrics(expectancy_per_filled_trade_lamports="9.000000000000")

    def disposition(**changes: Any) -> str:
        return oos.evaluate_h1_gates_v0_1(_metrics(**changes), control, window)["disposition"]

    checks["11_fill_rate_below_90_is_NO_GO"] = disposition(fill_rate="0.899999999999") == "NO_GO"
    checks["12_zero_net_is_NO_GO"] = disposition(net_pnl_lamports=0) == "NO_GO"
    checks["13_negative_net_is_NO_GO"] = disposition(net_pnl_lamports=-1) == "NO_GO"
    checks["14_nonpositive_expectancy_is_NO_GO"] = disposition(
        expectancy_per_filled_trade_lamports="0.000000000000"
    ) == "NO_GO"
    checks["15_profit_factor_below_1_10_is_NO_GO"] = disposition(
        profit_factor="1.099999999999"
    ) == "NO_GO"
    checks["16_drawdown_ratio_below_0_50_is_NO_GO"] = disposition(
        net_pnl_lamports=499,
        max_realized_closed_equity_drawdown_lamports=1_000,
    ) == "NO_GO"
    checks["17_daily_robustness_failure_has_no_GO"] = disposition(
        positive_complete_utc_days=1
    ) == "CONDITIONAL_POSITIVE_BUT_INSUFFICIENT_ROBUSTNESS"
    checks["18_H1_expectancy_not_above_control_is_NO_GO"] = disposition(
        expectancy_per_filled_trade_lamports="9.000000000000"
    ) == "NO_GO"
    checks["19_insufficient_sample_has_no_promotion"] = disposition(
        eligible_entry_count=149
    ) == "CONDITIONAL_POSITIVE_BUT_INSUFFICIENT_ROBUSTNESS"
    zero_dd = oos.evaluate_h1_gates_v0_1(
        _metrics(max_realized_closed_equity_drawdown_lamports=0), control, window
    )
    checks["20_positive_net_zero_drawdown_is_infinite_and_passes"] = (
        zero_dd["net_to_max_drawdown_ratio"] == "INFINITE"
        and zero_dd["gates"]["minimum_net_to_drawdown"]
    )
    checks["21_all_frozen_gates_yield_GO_TO_PAPER_GATE_only"] = (
        oos.evaluate_h1_gates_v0_1(_metrics(), control, window)["disposition"]
        == "GO_TO_PAPER_GATE"
    )
    exact_boundaries = oos.evaluate_h1_gates_v0_1(
        _metrics(
            fill_rate="0.900000000000",
            profit_factor="1.100000000000",
            net_pnl_lamports=500,
            max_realized_closed_equity_drawdown_lamports=1_000,
        ),
        control,
        window,
    )
    checks["21b_fill_PF_and_drawdown_exact_gate_boundaries_pass"] = all(
        exact_boundaries["gates"][name]
        for name in ("minimum_fill_rate", "minimum_profit_factor", "minimum_net_to_drawdown")
    )
    more_days = oos.evaluate_h1_gates_v0_1(
        _metrics(complete_utc_days=5, positive_complete_utc_days=3),
        control,
        _window(days=5),
    )
    checks["21c_more_than_three_days_uses_exact_60_percent_rule"] = (
        more_days["gates"]["daily_robustness"]
        and more_days["daily_rule_applied"]
        == "MORE_THAN_3_DAYS_POSITIVE_FRACTION_GTE_0.60"
    )
    exact_counts_override_display = oos.evaluate_h1_gates_v0_1(
        _metrics(
            fill_rate="0.900000000000",
            fill_rate_numerator=89,
            fill_rate_denominator=99,
            expectancy_numerator_lamports=1_000,
            expectancy_denominator_filled_trades=89,
            profit_factor_gross_profit_lamports=1_200,
            profit_factor_gross_loss_lamports=1_000,
        ),
        control,
        window,
    )
    checks["21d_exact_counts_not_rounded_display_control_gate"] = (
        not exact_counts_override_display["gates"]["minimum_fill_rate"]
        and exact_counts_override_display["disposition"] == "NO_GO"
    )


def _test_metrics_and_replay(checks: dict[str, bool]) -> None:
    window = _window()
    policy = oos.POLICIES["H1_SIMPLE_TIME_TP10_T30"]
    rows = [
        _synthetic_row(classification="FILLED", net=100, signal_at=BASE, seq=12),
        _synthetic_row(classification="EXIT_NO_CAUSAL_FILL", net=None, signal_at=BASE, seq=13),
    ]
    metrics, daily = oos.policy_metrics_v0_1(rows, policy, window)
    checks["22_unresolved_exit_not_counted_as_zero_PnL_fill"] = (
        metrics["filled_entries"] == 1
        and metrics["unresolved_or_rejected_entries"] == 1
        and metrics["net_pnl_lamports"] == 100
        and metrics["expectancy_per_filled_trade_lamports"] == "100.000000000000"
        and metrics["entry_explicit_cost_lamports"] == 20
        and metrics["total_explicit_cost_lamports"] == 30
    )
    checks["23_complete_UTC_day_metrics_are_signal_clock_attributed"] = (
        len(daily) == 3
        and daily[0]["net_pnl_lamports"] == 100
        and daily[0]["attribution_clock"] == "CandidateSignal.signal_observed_at UTC"
    )

    candidate = _candidate(BASE + timedelta(hours=1))
    mint = candidate.mint
    entry_at = datetime.fromisoformat(str(candidate.selected_market_observed_at))
    observations = (
        CounterfactualMarketObservationV01(
            mint=mint,
            observed_at=entry_at + timedelta(seconds=2),
            ingest_seq=12,
            event_key="trigger",
            price_identity="SOL_NATIVE",
            price_numerator_raw=110,
            price_denominator_raw=1,
            current_virtual_token_reserve_raw=1_000_000_000,
        ),
        CounterfactualMarketObservationV01(
            mint=mint,
            observed_at=entry_at + timedelta(seconds=2, milliseconds=500),
            ingest_seq=13,
            event_key="fill",
            price_identity="SOL_NATIVE",
            price_numerator_raw=110,
            price_denominator_raw=1,
            current_virtual_token_reserve_raw=1_000_000_000,
        ),
    )
    replay_rows = oos.evaluate_policy_rows_v0_1(
        [candidate], {mint: observations}, policy, frozen_watermark=1_000
    )
    checks["24_exit_delegates_to_accepted_Phase4_replay_and_execution"] = (
        len(replay_rows) == 1
        and replay_rows[0]["exit_reason"] == "TAKE_PROFIT"
        and replay_rows[0]["classification"] == "FILLED"
        and replay_rows[0]["exit_fill_source_row"] == 13
    )
    entry_observation = CounterfactualMarketObservationV01(
        mint=mint,
        observed_at=entry_at,
        ingest_seq=11,
        event_key="entry",
        price_identity="SOL_NATIVE",
        price_numerator_raw=100,
        price_denominator_raw=1,
        current_virtual_token_reserve_raw=1_000_000_000,
    )
    signal_observation = CounterfactualMarketObservationV01(
        mint=mint,
        observed_at=candidate.signal_at,
        ingest_seq=10,
        event_key="signal-source",
        price_identity="SOL_NATIVE",
        price_numerator_raw=100,
        price_denominator_raw=1,
        current_virtual_token_reserve_raw=1_000_000_000,
    )
    checks["24b_paper_entry_has_exact_normalized_source_lineage"] = (
        oos.validate_entry_source_lineage_v0_1(
            [candidate], {mint: (signal_observation, entry_observation, *observations)}
        ) == 1
    )
    mismatch_blocked = False
    try:
        oos.validate_entry_source_lineage_v0_1([candidate], {mint: observations})
    except oos.OOSBindingError:
        mismatch_blocked = True
    checks["24c_mismatched_paper_and_source_inputs_fail_closed"] = mismatch_blocked


def _test_binding_readonly_and_determinism(checks: dict[str, bool]) -> None:
    with tempfile.TemporaryDirectory(prefix="phase6_oos_selftest_") as raw:
        root = Path(raw)
        source = root / "source.sqlite3"
        conn = sqlite3.connect(source)
        conn.execute("CREATE TABLE evidence(id INTEGER PRIMARY KEY, value TEXT)")
        conn.execute("INSERT INTO evidence(value) VALUES('frozen')")
        conn.commit()
        conn.close()
        before = _sha(source)
        ro = oos._open_readonly(source)
        try:
            query_only = int(ro.execute("PRAGMA query_only").fetchone()[0]) == 1
            write_blocked = False
            try:
                ro.execute("UPDATE evidence SET value='changed'")
            except sqlite3.OperationalError:
                write_blocked = True
        finally:
            ro.close()
        checks["25_SQLite_source_opened_mode_ro_and_query_only"] = query_only and write_blocked
        checks["26_source_database_hash_unchanged"] = before == _sha(source)

        mismatch_window = oos.FrozenOOSWindowV01(
            start_at=BASE,
            end_at=BASE + timedelta(days=3),
            source_start_after_ingest_seq=1,
            source_end_ingest_seq=2,
            source_db_sha256="f" * 64,
        )
        mismatch_blocked = False
        try:
            oos.load_frozen_market_paths_v0_1(source, mismatch_window)
        except oos.OOSBindingError:
            mismatch_blocked = True
        checks["27_source_window_binding_mismatch_fails_closed"] = mismatch_blocked
        _assert_raises(
            lambda: oos.FrozenOOSWindowV01(
                start_at=BASE,
                end_at=BASE + timedelta(hours=71, minutes=59),
                source_start_after_ingest_seq=1,
                source_end_ingest_seq=2,
                source_db_sha256=FAKE_SHA,
            ),
            ValueError,
        )
        checks["28_less_than_72_contiguous_hours_rejected"] = True

        bounded_source = root / "bounded_source.sqlite3"
        source_fixture.create_fixture(bounded_source)
        bounded_window = oos.FrozenOOSWindowV01(
            start_at=BASE,
            end_at=BASE + timedelta(days=3),
            source_start_after_ingest_seq=2,
            source_end_ingest_seq=12,
            source_db_sha256=_sha(bounded_source),
        )
        bounded_paths, bounded_evidence = oos.load_frozen_market_paths_v0_1(
            bounded_source, bounded_window
        )
        checks["28b_source_normalization_never_inspects_post_watermark_row"] = (
            bounded_evidence["raw_rows_scanned"] == 10
            and all(
                observation.ingest_seq <= 12
                for path in bounded_paths.values()
                for observation in path
            )
        )

        paper = root / "paper.sqlite3"
        paper_conn = sqlite3.connect(paper)
        paper_conn.executescript(
            """
            CREATE TABLE paper_continuous_signal_contexts_v0_1(
                signal_key TEXT, route_id TEXT, mint TEXT,
                strategy_version TEXT, parameter_set_id TEXT, role TEXT,
                source_cursor INTEGER, source_event_key TEXT
            );
            CREATE TABLE paper_entry_routes(
                route_id TEXT, candidate_id TEXT, mint TEXT,
                strategy_version TEXT, parameter_set_id TEXT,
                signal_observed_at TEXT, signal_ingest_seq INTEGER,
                reference_price_identity TEXT,
                reference_price_numerator_raw TEXT,
                reference_price_denominator_raw TEXT,
                requested_size_lamports INTEGER, execution_ready_at TEXT,
                state TEXT, state_reason TEXT,
                selected_market_observed_at TEXT,
                selected_market_ingest_seq INTEGER,
                selected_source_event_key TEXT,
                simulated_entry_price_numerator_raw TEXT,
                simulated_entry_price_denominator_raw TEXT,
                total_entry_explicit_cost_lamports INTEGER,
                adverse_slippage_bps INTEGER
            );
            """
        )
        parameter_set = oos.LOCKED_PARAMETER_SET_BY_ROLE["CONTROL"]
        paper_conn.execute(
            "INSERT INTO paper_continuous_signal_contexts_v0_1 VALUES(?,?,?,?,?,?,?,?)",
            (
                "candidate-e2e", "route-e2e", "MINT-B", "v1.1", parameter_set,
                "CONTROL", 4, "sig-4:0:event-4:CANDIDATE:CONTROL",
            ),
        )
        paper_conn.execute(
            "INSERT INTO paper_entry_routes VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "route-e2e", "candidate-e2e", "MINT-B", "v1.1", parameter_set,
                "2026-08-24T00:00:04.000000+00:00", 4, "SOL_NATIVE",
                "10000000000", "100000000000", 1_000_000,
                "2026-08-24T00:00:04.500000+00:00", "FILLED", "ENTRY_FILLED",
                "2026-08-24T00:00:05.000000+00:00", 5,
                "sig-5:0:event-5:MARKET", "100", "1", 2_250_000, 0,
            ),
        )
        paper_conn.commit()
        paper_conn.close()
        end_to_end_window = oos.FrozenOOSWindowV01(
            start_at=datetime(2026, 8, 24, tzinfo=timezone.utc),
            end_at=datetime(2026, 8, 27, tzinfo=timezone.utc),
            source_start_after_ingest_seq=2,
            source_end_ingest_seq=12,
            source_db_sha256=_sha(bounded_source),
        )
        paper_before = _sha(paper)
        source_before = _sha(bounded_source)
        end_to_end_output = root / "phase6" / "sqlite-end-to-end"
        end_to_end = oos.run_sqlite_evaluation_v0_1(
            paper_db=paper,
            source_db=bounded_source,
            output_dir=end_to_end_output,
            window=end_to_end_window,
            expected_paper_db_sha256=paper_before,
        )
        checks["28c_full_SQLite_loader_replay_artifact_path_is_read_only_and_bound"] = (
            end_to_end["input_immutability"]
            == {
                "paper_db_sha256_before_after_equal": True,
                "source_db_sha256_before_after_equal": True,
            }
            and _sha(paper) == paper_before
            and _sha(bounded_source) == source_before
            and end_to_end_output.is_dir()
            and (end_to_end_output / "artifact_manifest.json").is_file()
            and not any(end_to_end_output.parent.glob(".sqlite-end-to-end.staging-*"))
        )
        corrupt_conn = sqlite3.connect(paper)
        corrupt_conn.execute(
            "INSERT INTO paper_continuous_signal_contexts_v0_1 VALUES(?,?,?,?,?,?,?,?)",
            (
                "orphan", "missing-route", "MINT-B", "v1.1", parameter_set,
                "CONTROL", 4, "sig-4:0:event-4:CANDIDATE:CONTROL",
            ),
        )
        corrupt_conn.commit()
        corrupt_conn.close()
        orphan_blocked = False
        try:
            oos.load_frozen_candidates_v0_1(paper, end_to_end_window)
        except oos.OOSBindingError:
            orphan_blocked = True
        checks["28d_orphan_candidate_or_route_rows_fail_closed"] = orphan_blocked

        window = _window()
        evidence = {
            "source_db_sha256": FAKE_SHA,
            "source_model_id": oos.SOURCE_MODEL_ID,
            "source_model_fingerprint": oos.SOURCE_MODEL_FINGERPRINT,
            "connection_mode": "mode=ro; PRAGMA query_only=ON",
        }
        output_a = root / "phase6" / "run-a"
        output_b = root / "phase6" / "run-b"
        frozen_candidates = [
            _candidate(BASE + timedelta(hours=1), signal_key="order-a", state="REJECTED"),
            _candidate(BASE + timedelta(hours=2), signal_key="order-b", state="REJECTED"),
        ]
        frozen_paths = {
            candidate.mint: (
                CounterfactualMarketObservationV01(
                    mint=candidate.mint,
                    observed_at=candidate.signal_at,
                    ingest_seq=candidate.signal_ingest_seq,
                    event_key="signal-source",
                    price_identity="SOL_NATIVE",
                    price_numerator_raw=100,
                    price_denominator_raw=1,
                    current_virtual_token_reserve_raw=1_000_000_000,
                ),
            )
            for candidate in frozen_candidates
        }
        result_a = oos.evaluate_frozen_inputs_v0_1(
            candidates=frozen_candidates, paths=frozen_paths, window=window, output_dir=output_a,
            source_evidence=evidence, paper_db_sha256="1" * 64,
        )
        result_b = oos.evaluate_frozen_inputs_v0_1(
            candidates=tuple(reversed(frozen_candidates)), paths=frozen_paths, window=window, output_dir=output_b,
            source_evidence=evidence, paper_db_sha256="1" * 64,
        )
        files_a = {path.name: path.read_bytes() for path in output_a.iterdir()}
        files_b = {path.name: path.read_bytes() for path in output_b.iterdir()}
        checks["29_exact_rerun_artifact_and_digest_determinism"] = (
            files_a == files_b
            and result_a["manifest"]["run_digest"] == result_b["manifest"]["run_digest"]
        )
        checks["30_complete_deterministic_artifact_set_emitted"] = set(files_a) == {
            "protocol_manifest.json", "source_window_manifest.json",
            "policy_definitions.json", "per_trade_evaluation_rows.json",
            "policy_metrics.json", "daily_metrics.json", "gate_evaluation.json",
            "final_disposition.json", "artifact_manifest.json",
        }
        _assert_raises(
            lambda: oos._assert_separate_phase6_output(root / "not_research", source),
            oos.OOSBindingError,
        )
        checks["31_non_Phase6_output_path_rejected"] = True


def _test_capability_absence(checks: dict[str, bool]) -> None:
    paths = (
        PROJECT_ROOT / "src/phase6/frozen_oos_hypothesis_evaluation_v0_1.py",
        PROJECT_ROOT / "scripts/phase6_frozen_oos_hypothesis_evaluation_v0_1.py",
    )
    forbidden = {
        "sign", "sign_transaction", "send", "send_transaction", "send_raw_transaction",
        "broadcast", "broadcast_transaction", "keypair", "private_key", "wallet",
    }
    discovered: set[str] = set()
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                discovered.add(node.id.lower())
            elif isinstance(node, ast.Attribute):
                discovered.add(node.attr.lower())
    checks["32_no_signer_send_broadcast_or_custody_capability"] = not (
        forbidden & discovered
    )
    expected_protocol = {
        **oos.PROTOCOL_DEFINITION,
        "model_fingerprint": oos.MODEL_FINGERPRINT,
        "policy_set_sha256": oos.protocol_review_payload_v0_1()["policy_set_sha256"],
    }
    checked_protocol = json.loads((
        PROJECT_ROOT / "data/research/phase6/P6_OOS_PROTOCOL_0001/protocol_manifest.json"
    ).read_text(encoding="utf-8"))
    checked_policies = json.loads((
        PROJECT_ROOT / "data/research/phase6/P6_OOS_PROTOCOL_0001/policy_definitions.json"
    ).read_text(encoding="utf-8"))
    checks["33_checked_protocol_artifacts_exactly_match_implementation"] = (
        checked_protocol == expected_protocol
        and checked_policies == oos.policy_definitions_v0_1()
    )


def main() -> int:
    checks: dict[str, bool] = {}
    _test_boundaries(checks)
    _test_policy_firewall(checks)
    _test_gates(checks)
    _test_metrics_and_replay(checks)
    _test_binding_readonly_and_determinism(checks)
    _test_capability_absence(checks)
    failed = [name for name, passed in checks.items() if not passed]
    for name, passed in checks.items():
        print(f"{'PASS' if passed else 'FAIL'}: {name}")
    print(f"MODEL_ID: {oos.MODEL_ID}")
    print(f"MODEL_FINGERPRINT: {oos.MODEL_FINGERPRINT}")
    print(f"POLICY_SET_SHA256: {oos.protocol_review_payload_v0_1()['policy_set_sha256']}")
    print(f"CHECKS: {len(checks)}")
    print(f"RESULT: {'PASS' if not failed else 'FAIL'}")
    if failed:
        print("FAILED_CHECKS: " + json.dumps(failed))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
