"""Offline checks of the new exact pending-row decision; no qualification run."""
from __future__ import annotations

import copy
import hashlib
import json
import sys
import tempfile
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "scripts")]
from live.runtime_dry_profile_v0_1 import read_reference, _validate_pending_guard_decision
from live.operations_degradation_v0_1 import DegradationPolicy, ResourceLimit, resource_condition


def main():
    review = json.loads((ROOT / "docs/live/MEME_LIVE_FIX2_APPROVED_PENDING_GUARD_V1.json").read_text())
    measured = read_reference(review["measurement"])
    observed = read_reference(measured["producer_observation"])
    source = read_reference(observed["source_binding"])
    limits = read_reference(review["accepted_extension"])["limits"]
    proposed = review["guard_limits"]
    start = dict(binding=source["binding"], profile=source["source_profile"],
        start_after_p1_rowid=source["start_after_p1_rowid"], database_identity=source["database_identity"])
    checks = {}

    def check(name, value):
        assert value, name
        checks[name] = True

    def reject(name, *, m=None, s=None, p=None):
        try:
            _validate_pending_guard_decision(m or measured, s or start, limits, p or proposed)
        except (ValueError, KeyError, TypeError):
            check(name, True)
        else:
            check(name, False)

    _validate_pending_guard_decision(measured, start, limits, proposed)
    check("actual_reviewed_cut_binds", True)
    policy = DegradationPolicy("0" * 64, 1, 1,
        tuple(ResourceLimit(k, v) for k, v in sorted(proposed.items())))
    check("all_13_recorded_producer_dimensions_fit", len(measured["maxima"]) == 13 and all(
        resource_condition(policy, k, v, configuration_digest="0" * 64) is None
        for k, v in measured["maxima"].items()))
    check("next_row_still_denied", resource_condition(policy, "PRODUCER_PENDING_ROWS", 2422,
        configuration_digest="0" * 64) == "RESOURCE_EXCEEDED")
    reject("headroom_denied", p=dict(proposed, PRODUCER_PENDING_ROWS=2422))
    reject("unapproved_history_change_denied", p=dict(proposed, HISTORY_BYTES=9087))
    reject("unproven_tombstone_change_denied", p=dict(proposed, TOMBSTONE_ROWS=2))
    reject("non_t010_scope_denied", m=dict(measured, approval_scope=["T011"]))
    reject("different_source_cut_denied", m=dict(measured, source_cut_p1_rowid=3143388))
    reject("different_source_start_denied", s=dict(start, start_after_p1_rowid=3141902))
    reject("source_policy_change_denied", s=dict(start, profile=dict(start["profile"], freshness_seconds=31)))
    reject("inflated_summary_denied", m=dict(measured, maxima=dict(measured["maxima"], HISTORY_BYTES=1)))
    reject("unbound_evidence_denied", m=dict(measured,
        producer_observation=dict(measured["producer_observation"], sha256="0" * 64)))
    with tempfile.TemporaryDirectory(prefix="fix2-pending-negative-codec-") as directory:
        # These edited copies test denial only. They are never capacity evidence.
        for field, value in (("synthetic_observations", True), ("source_readonly", False),
                ("signer_send_broadcast", True), ("T010_executed", True)):
            negative = copy.deepcopy(observed)
            negative[field] = value
            path = Path(directory) / (field + ".json")
            path.write_text(json.dumps(negative))
            ref = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            reject(field + "_denied", m=dict(measured, producer_observation=ref))
    # Constructor wiring only, using the existing disposable baseline fixture.
    # It supplies no capacity evidence and never runs public qualification.
    import httpx
    import live_runtime_dry_profile_selftest_v0_1 as fixtures
    from live.execution_signer_v0_1 import AutonomousLocalSigner
    from live.execution_send_v0_1 import SolanaSendTransport
    refs = fixtures.references()
    with tempfile.TemporaryDirectory(prefix="fix2-pending-constructor-") as directory, \
            fixtures.accepted_public_fixture(refs) as accepted, ExitStack() as stack:
        for owner, method in ((httpx.HTTPTransport, "handle_request"),
                (AutonomousLocalSigner, "__init__"), (SolanaSendTransport, "__init__")):
            stack.enter_context(patch.object(owner, method, side_effect=AssertionError("FORBIDDEN_EXTERNAL_ACTION")))
        f = fixtures.owned.fixture(Path(directory), "pending-codec-only")
        try:
            inputs = fixtures.inputs_for(f, refs, accepted)
            inputs["source_start"].update(start, read_path=start["database_identity"])
            inputs["qualification_substitutions"] = dict(scope="PUBLIC_ENVIRONMENT_REVIEWED",
                synthetic_baseline_and_rpc=False, synthetic_source_binding=False,
                source_read_path=start["database_identity"])
            path = ROOT / "docs/live/MEME_LIVE_FIX2_APPROVED_PENDING_GUARD_V1.json"
            inputs["guard_amendment"] = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            inputs["monitor"]["resource_limits"] = proposed
            public = read_reference({"path": accepted["target"]["public_configuration_path"],
                "sha256": accepted["target"]["public_configuration_sha256"]})
            inputs["driver"]["endpoint"] = public["public_rpc"]["url"]
            profile = fixtures.profile_api.build_profile(inputs)
            check("profile_constructor_binds_exact_guard", profile.record["inputs"]["monitor"]["resource_limits"] == proposed)
            check("constructor_does_not_initialize_or_grant", profile.record["store_initialization"] is False
                and profile.record["grants_permission"] is False)
        finally:
            fixtures.owned.fixtures.close(f)
    print(json.dumps({"scope": "OFFLINE_PENDING_DECISION_DELTA_ONLY", "checks": checks,
        "check_count": len(checks), "public_qualification": False}, sort_keys=True))


if __name__ == "__main__":
    main()
