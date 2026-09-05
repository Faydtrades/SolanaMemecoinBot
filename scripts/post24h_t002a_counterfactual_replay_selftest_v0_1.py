from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from phase4.paper_cost_baseline_v0_1 import P4_COST_BASELINE_0001  # noqa: E402
from phase4.post24h_counterfactual_replay_v0_1 import (  # noqa: E402
    BASELINE_POLICIES,
    EXPECTED_COHORT_SHA256,
    EXPECTED_PHYSICAL_ENTRIES,
    EXPECTED_T001_REPLAY_SHA256,
    EXIT_IMPACT_FINGERPRINT,
    EXIT_IMPACT_MODEL_ID,
    MODEL_FINGERPRINT,
    MODEL_ID,
    T001_MODEL_FINGERPRINT,
    CounterfactualExitPolicyV01,
    CounterfactualExitReason,
    CounterfactualMarketObservationV01,
    PolicyFamily,
    ReplayMismatch,
    TimeoutOrigin,
    evaluate_counterfactual_intent_v0_1,
    execute_counterfactual_intent_v0_1,
    replay_counterfactual_v0_1,
    result_payload,
    run_frozen_baseline_gate_v0_1,
)


PAPER = ROOT / "data/paper/multihour/phase4_firstpullback_multihour_20260827T123022_384349Z.sqlite3"
SOURCE = ROOT / "data/db/tradingbot.sqlite3"
EVIDENCE = (
    ROOT
    / "data/research/post24h/POST-24H-ANALYSIS-001/POST24H_T002A_foundation_evidence.json"
)
EXPECTED_HEAD = "d89df67f91b46400fcef1232f4741c328b9f9dfe"
BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class SyntheticEntry:
    signal_key: str = "synthetic-signal"
    mint: str = "synthetic-mint"
    signal_observed_at: str = BASE.isoformat(timespec="microseconds")
    signal_ingest_seq: int = 10
    reference_price_identity: str = "SOL_NATIVE"
    reference_price_numerator_raw: int = 100
    reference_price_denominator_raw: int = 1
    entry_observed_at: str = (BASE + timedelta(seconds=1)).isoformat(
        timespec="microseconds"
    )
    entry_ingest_seq: int = 11
    source_event_key: str = "entry:MARKET"
    principal_lamports: int = 1_000_000
    entry_price_numerator_raw: int = 100
    entry_price_denominator_raw: int = 1
    entry_explicit_cost_lamports: int = 2_250_000


def _obs(
    seconds: float,
    seq: int,
    price: int,
    *,
    mint: str = "synthetic-mint",
    identity: str = "SOL_NATIVE",
    gap: bool = False,
    suffix: str | None = None,
) -> CounterfactualMarketObservationV01:
    return CounterfactualMarketObservationV01(
        mint=mint,
        observed_at=BASE + timedelta(seconds=seconds),
        ingest_seq=seq,
        event_key=f"event-{seq}-{suffix or price}",
        price_identity=identity,
        price_numerator_raw=price,
        price_denominator_raw=1,
        current_virtual_token_reserve_raw=1_000_000_000,
        is_gap_recovery=gap,
    )


def _policy(**overrides: object) -> CounterfactualExitPolicyV01:
    values: dict[str, object] = {
        "policy_id": "synthetic-policy",
        "timeout_ms": 5_000,
        "timeout_origin": TimeoutOrigin.PHASE4_SIGNAL_CLOCK,
    }
    values.update(overrides)
    return CounterfactualExitPolicyV01(**values)  # type: ignore[arg-type]


def _raises(call: Callable[[], object], expected: type[BaseException] = ValueError) -> None:
    try:
        call()
    except expected:
        return
    raise AssertionError(f"expected {expected.__name__}")


def _test_timeout_only() -> None:
    intent = evaluate_counterfactual_intent_v0_1(
        SyntheticEntry(), [_obs(2, 12, 105)], _policy()
    )
    assert intent.reason is CounterfactualExitReason.FALLBACK
    assert intent.reference_source == "LAST_FRESH_MARKET"
    assert intent.trigger_observed_at is None
    assert intent.requested_at == BASE + timedelta(seconds=5, microseconds=1)


def _test_tp_timeout() -> None:
    intent = evaluate_counterfactual_intent_v0_1(
        SyntheticEntry(), [_obs(2, 12, 109), _obs(3, 13, 110)], _policy(tp_bps=1_000)
    )
    assert intent.reason is CounterfactualExitReason.TAKE_PROFIT
    assert intent.trigger_ingest_seq == 13 and intent.rule_return_bps == 1_000


def _test_sl_timeout() -> None:
    result = replay_counterfactual_v0_1(
        SyntheticEntry(),
        [_obs(2, 12, 91), _obs(3, 13, 90), _obs(3.5, 14, 90)],
        _policy(sl_bps=1_000),
    )
    intent = result.intent
    assert intent.reason is CounterfactualExitReason.STOP_LOSS
    assert intent.trigger_ingest_seq == 13 and intent.rule_return_bps == -1_000
    assert result.classification == "FILLED"
    assert result.execution.fill_source_row == 14


def _test_tp_sl_first_crossing() -> None:
    intent = evaluate_counterfactual_intent_v0_1(
        SyntheticEntry(),
        [_obs(3, 13, 120), _obs(2, 12, 90)],
        _policy(tp_bps=1_000, sl_bps=500),
    )
    assert intent.reason is CounterfactualExitReason.STOP_LOSS
    assert intent.trigger_ingest_seq == 12


def _test_exact_deadline() -> None:
    intent = evaluate_counterfactual_intent_v0_1(
        SyntheticEntry(), [_obs(5, 12, 110)], _policy(tp_bps=1_000)
    )
    assert intent.reason is CounterfactualExitReason.TAKE_PROFIT
    assert intent.requested_at == BASE + timedelta(seconds=5)


def _test_post_deadline() -> None:
    intent = evaluate_counterfactual_intent_v0_1(
        SyntheticEntry(),
        [_obs(5.000001, 12, 110)],
        _policy(tp_bps=1_000),
    )
    assert intent.reason is CounterfactualExitReason.FALLBACK
    assert intent.trigger_ingest_seq is None
    assert intent.requested_at == BASE + timedelta(seconds=5, microseconds=1)


def _test_trailing() -> None:
    intent = evaluate_counterfactual_intent_v0_1(
        SyntheticEntry(),
        [_obs(2, 12, 110), _obs(3, 13, 120), _obs(4, 14, 117)],
        _policy(trailing_activation_bps=1_000, trailing_giveback_bps=300),
    )
    assert intent.reason is CounterfactualExitReason.TRAIL
    assert intent.trigger_ingest_seq == 14
    assert intent.rule_return_bps == 1_700
    assert intent.trail_peak_return_bps == 2_000


def _test_pre_entry_ignored() -> None:
    intent = evaluate_counterfactual_intent_v0_1(
        SyntheticEntry(),
        [_obs(0.5, 20, 120), _obs(2, 21, 100)],
        _policy(tp_bps=1_000),
    )
    assert intent.reason is CounterfactualExitReason.FALLBACK
    assert intent.last_fresh_ingest_seq == 21


def _test_gap_ignored() -> None:
    intent = evaluate_counterfactual_intent_v0_1(
        SyntheticEntry(),
        [_obs(2, 12, 120, gap=True), _obs(3, 13, 100)],
        _policy(tp_bps=1_000),
    )
    assert intent.reason is CounterfactualExitReason.FALLBACK
    assert intent.last_fresh_ingest_seq == 13


def _test_wrong_identity_ignored() -> None:
    intent = evaluate_counterfactual_intent_v0_1(
        SyntheticEntry(),
        [_obs(2, 12, 120, identity="QUOTE:USD"), _obs(3, 13, 100)],
        _policy(tp_bps=1_000),
    )
    assert intent.reason is CounterfactualExitReason.FALLBACK
    assert intent.last_fresh_ingest_seq == 13


def _test_cross_mint_ignored() -> None:
    intent = evaluate_counterfactual_intent_v0_1(
        SyntheticEntry(),
        [_obs(2, 12, 120, mint="other"), _obs(3, 13, 100)],
        _policy(tp_bps=1_000),
    )
    assert intent.reason is CounterfactualExitReason.FALLBACK
    assert intent.last_fresh_ingest_seq == 13


def _triggered_path() -> tuple[CounterfactualExitPolicyV01, list[CounterfactualMarketObservationV01]]:
    return _policy(tp_bps=1_000), [_obs(2, 12, 110)]


def _test_latency_and_exact_ready() -> None:
    policy, path = _triggered_path()
    path.extend([_obs(2.499999, 13, 110), _obs(2.5, 14, 110)])
    result = replay_counterfactual_v0_1(SyntheticEntry(), path, policy)
    assert result.execution.exit_state == "FILLED"
    assert result.classification == "FILLED"
    assert result.execution.fill_source_row == 14
    assert result.execution.attempt_count == 1


def _test_rejected_then_filled() -> None:
    policy, path = _triggered_path()
    path.extend([_obs(2.5, 13, 70), _obs(2.6, 14, 110)])
    result = replay_counterfactual_v0_1(SyntheticEntry(), path, policy)
    assert result.execution.exit_state == "FILLED"
    assert result.execution.fill_source_row == 14
    assert result.execution.attempt_count == 2
    assert result.execution.rejection_state == "REJECTED_THEN_FILLED"


def _test_repeated_slippage_unresolved() -> None:
    policy, path = _triggered_path()
    path.extend([_obs(2.5, 13, 70), _obs(2.6, 14, 75)])
    result = replay_counterfactual_v0_1(SyntheticEntry(), path, policy)
    assert result.execution.exit_state == "UNRESOLVED"
    assert result.classification == "REJECTED_SLIPPAGE"
    assert result.execution.attempt_count == 2
    assert result.execution.rejection_state == "REJECTED_SLIPPAGE"


def _test_no_post_ready_price() -> None:
    policy, path = _triggered_path()
    result = replay_counterfactual_v0_1(SyntheticEntry(), path, policy)
    assert result.execution.exit_state == "UNRESOLVED"
    assert result.classification == "NO_CAUSAL_FILL"
    assert result.execution.attempt_count == 0
    assert result.execution.rejection_state == "NO_CAUSAL_FILL"


def _test_invalid_policies() -> None:
    _raises(lambda: _policy(timeout_ms=0))
    _raises(lambda: _policy(timeout_origin="PHASE4_SIGNAL_CLOCK"))
    _raises(lambda: _policy(tp_bps=0))
    _raises(lambda: _policy(sl_bps=-1))
    _raises(lambda: _policy(trailing_activation_bps=1_000))
    _raises(
        lambda: _policy(
            tp_bps=1_000,
            trailing_activation_bps=1_000,
            trailing_giveback_bps=300,
        )
    )


def _test_timeout_origins_distinct() -> None:
    signal = evaluate_counterfactual_intent_v0_1(
        SyntheticEntry(), [], _policy(timeout_origin=TimeoutOrigin.PHASE4_SIGNAL_CLOCK)
    )
    entry = evaluate_counterfactual_intent_v0_1(
        SyntheticEntry(), [], _policy(timeout_origin=TimeoutOrigin.ENTRY_FILL_CLOCK)
    )
    assert signal.timeout_deadline_at == BASE + timedelta(seconds=5)
    assert entry.timeout_deadline_at == BASE + timedelta(seconds=6)
    assert entry.requested_at - signal.requested_at == timedelta(seconds=1)


def _test_rule_reference_not_entry_price() -> None:
    entry = SyntheticEntry(entry_price_numerator_raw=200)
    intent = evaluate_counterfactual_intent_v0_1(
        entry, [_obs(2, 12, 110)], _policy(tp_bps=1_000)
    )
    assert intent.reason is CounterfactualExitReason.TAKE_PROFIT
    assert intent.rule_return_bps == 1_000


def _test_watermark_enforced() -> None:
    _raises(
        lambda: evaluate_counterfactual_intent_v0_1(
            SyntheticEntry(),
            [_obs(2, 12, 110), _obs(3, 101, 100)],
            _policy(tp_bps=1_000),
            frozen_watermark=100,
        ),
        ReplayMismatch,
    )


def _test_conflicting_duplicate_identity() -> None:
    first = _obs(2, 12, 105, suffix="a")
    second = _obs(2, 12, 106, suffix="b")
    _raises(
        lambda: evaluate_counterfactual_intent_v0_1(
            SyntheticEntry(), [first, second], _policy()
        ),
        ReplayMismatch,
    )


def _test_execution_watermark_after_fill_enforced() -> None:
    entry = SyntheticEntry()
    intent = evaluate_counterfactual_intent_v0_1(
        entry, [_obs(2, 12, 110)], _policy(tp_bps=1_000), frozen_watermark=100
    )
    _raises(
        lambda: execute_counterfactual_intent_v0_1(
            entry,
            [_obs(2, 12, 110), _obs(2.5, 13, 110), _obs(3, 101, 100)],
            intent,
            frozen_watermark=100,
        ),
        ReplayMismatch,
    )


def _test_deterministic_rerun() -> None:
    policy = _policy(trailing_activation_bps=1_000, trailing_giveback_bps=300)
    path = [_obs(4, 14, 117), _obs(2, 12, 110), _obs(3, 13, 120), _obs(4.5, 15, 117)]
    first = replay_counterfactual_v0_1(SyntheticEntry(), path, policy)
    second = replay_counterfactual_v0_1(SyntheticEntry(), tuple(reversed(path)), policy)
    assert result_payload(first) == result_payload(second)


SYNTHETIC_TESTS: tuple[tuple[str, Callable[[], None]], ...] = (
    ("timeout-only policy", _test_timeout_only),
    ("TP-only plus timeout", _test_tp_timeout),
    ("SL-only plus timeout", _test_sl_timeout),
    ("TP+SL first causal crossing", _test_tp_sl_first_crossing),
    ("exact-deadline market observation beats timeout", _test_exact_deadline),
    ("post-deadline observation loses to timeout", _test_post_deadline),
    ("trailing activation peak giveback", _test_trailing),
    ("pre-entry observation ignored", _test_pre_entry_ignored),
    ("gap recovery cannot trigger", _test_gap_ignored),
    ("wrong price identity cannot trigger", _test_wrong_identity_ignored),
    ("cross-mint observation cannot trigger", _test_cross_mint_ignored),
    ("500ms latency and exact-ready eligibility", _test_latency_and_exact_ready),
    ("rejected slippage later fills", _test_rejected_then_filled),
    ("repeated slippage remains unresolved", _test_repeated_slippage_unresolved),
    ("no post-ready price is no causal fill", _test_no_post_ready_price),
    ("invalid policy configurations fail closed", _test_invalid_policies),
    ("timeout origins are distinct", _test_timeout_origins_distinct),
    ("threshold uses frozen rule reference", _test_rule_reference_not_entry_price),
    ("frozen watermark enforced", _test_watermark_enforced),
    ("conflicting duplicate identity fails closed", _test_conflicting_duplicate_identity),
    ("execution validates watermark beyond terminal fill", _test_execution_watermark_after_fill_enforced),
    ("deterministic rerun", _test_deterministic_rerun),
)


def _head() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def main() -> int:
    test_results: list[dict[str, object]] = []
    failed: list[str] = []
    for name, test in SYNTHETIC_TESTS:
        try:
            test()
            passed = True
            detail = "exact"
        except Exception as exc:  # deterministic evidence needs the failing label
            passed = False
            detail = f"{type(exc).__name__}: {exc}"
            failed.append(name)
        test_results.append({"name": name, "passed": passed, "detail": detail})
        print(f"TEST: {name}: {'PASS' if passed else 'FAIL'}")
        if not passed:
            print(f"DETAIL: {detail}")

    baseline: dict[str, object] | None = None
    baseline_name = "frozen FINAL-A FINAL-B SENS-C exact baseline"
    try:
        baseline = run_frozen_baseline_gate_v0_1(PAPER, SOURCE)
        baseline_ok = (
            baseline["exact_comparison"] is True
            and baseline["positions"] == 1_693
            and baseline["fills"] == 1_608
            and baseline["unresolved"] == 85
            and baseline["t001_replay_digest"] == EXPECTED_T001_REPLAY_SHA256
            and baseline["generic_baseline_digest"] == EXPECTED_T001_REPLAY_SHA256
            and baseline["cohort"]["physical_entry_count"] == EXPECTED_PHYSICAL_ENTRIES
            and baseline["cohort"]["computed_accepted_cohort_sha256"]
            == EXPECTED_COHORT_SHA256
        )
        if not baseline_ok:
            raise AssertionError("frozen baseline aggregate gate mismatch")
        passed = True
        detail = "1693 positions / 1608 fills / 85 unresolved / exact field comparison"
    except Exception as exc:
        passed = False
        detail = f"{type(exc).__name__}: {exc}"
        failed.append(baseline_name)
    test_results.append({"name": baseline_name, "passed": passed, "detail": detail})
    print(f"TEST: {baseline_name}: {'PASS' if passed else 'FAIL'}")
    if not passed:
        print(f"DETAIL: {detail}")

    head = _head()
    head_ok = head == EXPECTED_HEAD
    test_results.append(
        {
            "name": "required Git HEAD",
            "passed": head_ok,
            "detail": head,
        }
    )
    print(f"TEST: required Git HEAD: {'PASS' if head_ok else 'FAIL'}")
    if not head_ok:
        failed.append("required Git HEAD")

    if failed or baseline is None:
        print(f"TESTS: {len(test_results)}")
        print(f"PASSED: {len(test_results) - len(failed)}")
        print("RESULT: FAIL")
        return 1

    evidence = {
        "task_id": "POST24H-T002A",
        "model_id": MODEL_ID,
        "model_fingerprint": MODEL_FINGERPRINT,
        "head": head,
        "accepted_cohort": {
            "count": baseline["cohort"]["physical_entry_count"],
            "expected_sha256": EXPECTED_COHORT_SHA256,
            "computed_sha256": baseline["cohort"]["computed_accepted_cohort_sha256"],
            "exact_match": baseline["cohort"]["accepted_cohort_sha256_exact_match"],
        },
        "frozen_baseline": {
            "exact_comparison": baseline["exact_comparison"],
            "positions": baseline["positions"],
            "fills": baseline["fills"],
            "unresolved": baseline["unresolved"],
            "summary": baseline["summary"],
            "policies": baseline["policies"],
            "comparison_fields": baseline["comparison_fields"],
        },
        "t001_replay_digest": baseline["t001_replay_digest"],
        "generic_baseline_digest": baseline["generic_baseline_digest"],
        "t002a_deterministic_digest": baseline["t002a_deterministic_digest"],
        "deterministic_rerun": baseline["deterministic_rerun"],
        "execution_models": {
            "t001_model_fingerprint": T001_MODEL_FINGERPRINT,
            "cost_model_fingerprint": P4_COST_BASELINE_0001.fingerprint,
            "exit_impact_model_id": EXIT_IMPACT_MODEL_ID,
            "exit_impact_fingerprint": EXIT_IMPACT_FINGERPRINT,
            "exit_latency_ms": 500,
            "slippage_cap_bps": 2_000,
        },
        "timeout_origin_semantics": {
            "PHASE4_SIGNAL_CLOCK": "signal_observed_at + timeout_ms",
            "ENTRY_FILL_CLOCK": "entry_observed_at + timeout_ms",
            "timeout_fire_offset_us": 1,
            "exact_deadline_market_eligible": True,
        },
        "actual_exit_route_required_for_counterfactual_execution": False,
        "test_results": test_results,
        "test_count": len(test_results),
        "passed": len(test_results),
        "result": "PASS",
    }
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE.write_text(
        json.dumps(evidence, sort_keys=True, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(f"EVIDENCE: {EVIDENCE}")
    print(f"MODEL: {MODEL_ID}")
    print(f"FINGERPRINT: {MODEL_FINGERPRINT}")
    print(f"T001_REPLAY_DIGEST: {baseline['t001_replay_digest']}")
    print(f"T002A_DETERMINISTIC_DIGEST: {baseline['t002a_deterministic_digest']}")
    print(f"TESTS: {len(test_results)}")
    print(f"PASSED: {len(test_results)}")
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
