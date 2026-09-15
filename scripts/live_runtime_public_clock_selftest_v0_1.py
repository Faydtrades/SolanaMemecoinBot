"""Deterministic external clock failures/continuity; all policy numbers are fixtures."""
from __future__ import annotations
from contextlib import nullcontext
from dataclasses import asdict, replace
import json
from pathlib import Path
import struct
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from live import runtime_public_clock_v0_1 as clock
from live.acceptance_dossier_v0_1 import canonical_bytes, sha256
from live.authority_controls_v0_1 import ClockPolicy, AuthorityState, utc_from_us

CHECKS = {}


def check(name, value):
    if not value:
        raise AssertionError(name)
    CHECKS[name] = True


def deny(name, fn, reason):
    try:
        fn()
    except (ValueError, TypeError, KeyError) as exc:
        check(name, reason in str(exc))
    else:
        raise AssertionError(name + ": accepted")


def qualification(directory):
    p = clock.NtpProvider("SYNTHETIC_CLOCK_TEST", "test.invalid", "192.0.2.1", 123, 0,
        1000, 1000000, 2000000, clock.TRUST, 4)
    nonce = b"12345678"
    second = clock.NTP_UNIX_DELTA + 1700000000
    stamp = struct.pack("!II", second, 0)
    packet = bytearray(48)
    packet[:4] = struct.pack("!BBBb", 0x24, 2, 4, -20)
    packet[8:12] = struct.pack("!I", 64)
    packet[16:24], packet[24:32], packet[32:40], packet[40:48] = stamp, nonce, stamp, stamp
    raw = bytes(packet)
    observation = clock.decode_observation(p, raw, nonce, 1000000000, 1020000000)
    check("full_rtt_interval_not_symmetric_midpoint", observation["round_trip_us"] == 20000
        and clock.utc_microseconds(observation["utc_upper_utc"])-clock.utc_microseconds(observation["utc_lower_utc"])
        > 20000 and observation["authenticated_transport"] is False)
    check("provider_fingerprint_binds_exact_endpoint", replace(p, address="192.0.2.2").fingerprint != p.fingerprint)
    for name, offset, value, reason in (
        ("unsynchronized", 0, 0xe4, "UNSYNCHRONIZED"), ("leap_pending", 0, 0x64, "UNSYNCHRONIZED"),
        ("wrong_mode", 0, 0x23, "SERVER_MODE"), ("wrong_version", 0, 0x1c, "SERVER_MODE"),
        ("kiss_of_death", 1, 0, "UNSYNCHRONIZED"), ("wrong_nonce", 24, 0, "CORRELATION")):
        changed = bytearray(raw)
        changed[offset] = value
        deny(name, lambda: clock.decode_observation(p, bytes(changed), nonce, 1000000000, 1020000000), reason)
    deny("trailing_packet_denied", lambda: clock.decode_observation(p, raw+b"x", nonce, 1, 2), "EXACT_SNTP")
    deny("monotonic_regression", lambda: clock.decode_observation(p, raw, nonce, 2, 1), "MONOTONIC")
    deny("network_deadline", lambda: clock.decode_observation(p, raw, nonce, 0, 2000000000), "TIMEOUT")
    changed = bytearray(raw)
    changed[40:48] = b"\0"*8
    deny("zero_transmit", lambda: clock.decode_observation(p, bytes(changed), nonce, 1, 2), "ZERO_SERVER")
    path = Path(directory)/"synthetic-clock-review.json"
    policy = ClockPolicy(p.provider_id, p.fingerprint, 100000, 100, 3000000)
    record = {"schema": clock.REVIEW_SCHEMA, "status": "APPROVED_FOR_PUBLIC_DRY", "origin": "HUMAN_EXTERNAL",
        "provider": asdict(p), "clock_policy": asdict(policy), "host_identity_digest": "a"*64,
        "review_reference": "SYNTHETIC_TEST_ONLY_NOT_A_PROJECT_DECISION"}

    def ref(value=record):
        path.write_bytes(canonical_bytes(value))
        return {"path": str(path.resolve()), "sha256": sha256(path.read_bytes())}

    reference = ref()
    deny("unapproved_provider_denied", lambda: clock.RealPublicClock(ref(dict(record,
        status="PROVIDER_CANDIDATE_NOT_APPROVED")), "a"*64), "NOT_APPROVED")
    reference = ref()
    deny("wrong_host_denied", lambda: clock.RealPublicClock(reference, "b"*64), "NOT_APPROVED")
    fake = lambda _: dict(observation)
    source = clock.RealPublicClock(reference, "a"*64, test_observer=fake)
    with patch.object(clock.time, "perf_counter_ns", return_value=1020000000), \
         patch.object(clock.time, "time_ns", side_effect=AssertionError("wall truth forbidden")):
        first = source.sample()
    check("explicit_fake_observations_never_public", source.scope == "TEST_ONLY_FAKE_EXTERNAL_TRANSPORT"
          and source.last_observation["scope"] == source.scope)
    check("first_sample_no_invented_checkpoint", first.previous_sample_digest == "0"*64 and first.reconciliation is None)
    public_observation = dict(observation, scope="PUBLIC_ENVIRONMENT_REVIEWED")
    clock.validate_sample_observation(public_observation, first, source)
    check("offline_packet_and_projection_recomputed", True)
    deny("unknown_clock_status_denied_offline", lambda: clock.validate_sample_observation(public_observation,
        replace(first, status="UNKNOWN"), source), "QUALIFIED_SAMPLE_REQUIRED")
    deny("altered_packet_interval_denied", lambda: clock.validate_sample_observation(
        dict(public_observation, root_distance_us=0), first, source), "PACKET_OBSERVATION")
    deny("altered_sample_interval_denied", lambda: clock.validate_sample_observation(public_observation,
        replace(first, utc_lower_utc=utc_from_us(clock.utc_microseconds(first.utc_lower_utc)+1)), source), "SAMPLE_INTERVAL")
    bounds = clock.projected_bounds(observation, observation["monotonic_ns"]+1, p, replace(policy, maximum_drift_ppm=0))
    check("sub_microsecond_projection_rounds_outward", bounds[0] == observation["utc_lower_utc"]
        and clock.utc_microseconds(bounds[1]) == clock.utc_microseconds(observation["utc_upper_utc"])+1)
    delayed = clock.projected_bounds(observation, observation["monotonic_ns"], p, policy)
    check("asymmetric_reply_delay_includes_approved_counter_drift", delayed[0] == observation["utc_lower_utc"]
        and clock.utc_microseconds(delayed[1]) == clock.utc_microseconds(observation["utc_upper_utc"])+2)
    deny("nominal_rtt_only_sample_denied_offline", lambda: clock.validate_sample_observation(public_observation,
        replace(first, utc_upper_utc=observation["utc_upper_utc"]), source), "SAMPLE_INTERVAL")
    state = AuthorityState("d"*64, policy=SimpleNamespace(clock=policy), last_qualified_clock=first)
    ledger = SimpleNamespace(_authority=state, _trusted_read=lambda:nullcontext())
    started = SimpleNamespace(runtime=SimpleNamespace(ledger=ledger))
    with patch.object(clock.time, "perf_counter_ns", return_value=1120000000):
        second_sample = source.sample(started)
    check("original_checkpoint_continuity", second_sample.previous_sample_digest == first.content_digest
          and second_sample.reconciliation is None and not clock.clock_reasons(state, second_sample))
    fresh = clock.RealPublicClock(reference, "a"*64, test_observer=lambda _:dict(observation,
        monotonic_ns=2020000000, utc_lower_utc=utc_from_us(clock.utc_microseconds(observation["utc_lower_utc"])+1000000),
        utc_upper_utc=utc_from_us(clock.utc_microseconds(observation["utc_upper_utc"])+1000000)))
    with patch.object(clock.time, "perf_counter_ns", return_value=2020000000):
        recovered = fresh.sample(started)
    check("cold_restart_reconciles_exact_original_checkpoint", recovered.reconciliation is not None
          and recovered.reconciliation.previous_sample_digest == first.content_digest
          and not clock.clock_reasons(state, recovered))
    # A fresh SNTP packet cannot renew a long same-epoch checkpoint gap.
    # Only the qualifier's explicit close/reopen boundary gets a new instance.
    late_packet = bytearray(raw)
    late_stamp = struct.pack("!II", second+4, 0)
    for offset in (16, 32, 40):
        late_packet[offset:offset+8] = late_stamp
    late_observation = clock.decode_observation(p, bytes(late_packet), nonce, 5000000000, 5020000000)
    source.test_observer = lambda _:dict(late_observation)
    from live.runtime_dry_public_facts_v0_1 import ReviewedPublicFacts
    facts = object.__new__(ReviewedPublicFacts)
    facts.settings = {"clock_profile":reference}
    facts.config = {"degradation_config":SimpleNamespace(host_identity_digest="a"*64)}
    facts.public_clock, facts.startup_us = source, 123
    retained_observations = [first]
    facts._recent_clock_observations = retained_observations
    with patch.object(clock.time, "perf_counter_ns", return_value=5020000000):
        deny("fresh_packet_does_not_renew_same_epoch_gap", lambda:source.sample(started), "ORIGINAL_CONTINUITY_DENIED")
        facts.begin_cold_reopen()
        reconciled = facts.public_clock.sample(started)
    check("explicit_cold_reopen_new_epoch_after_long_gap", reconciled.epoch_id != first.epoch_id
        and reconciled.reconciliation.previous_sample_digest == first.content_digest
        and reconciled.previous_sample_digest == first.content_digest
        and not clock.clock_reasons(state, reconciled))
    check("cold_reopen_preserves_reviewed_policy_and_evidence", facts.public_clock.policy == policy
        and facts.public_clock.provider == p and facts.public_clock.reviewed_reference == reference
        and facts._recent_clock_observations is retained_observations
        and facts.public_clock.scope == "TEST_ONLY_FAKE_EXTERNAL_TRANSPORT"
        and facts.startup_us is None and ledger._authority is state)
    wrong = SimpleNamespace(runtime=SimpleNamespace(ledger=SimpleNamespace(_trusted_read=lambda:nullcontext(),
        _authority=replace(state, policy=SimpleNamespace(clock=replace(policy, provider_id="WRONG"))))))
    deny("wrong_installed_policy", lambda: source.sample(wrong), "ORIGINAL_POLICY")
    source.test_observer = lambda _: (_ for _ in ()).throw(ValueError("PUBLIC_CLOCK_OBSERVATION_UNAVAILABLE"))
    with patch.object(clock.time, "perf_counter_ns", return_value=7020000000):
        deny("refresh_failure_no_cached_or_wall_fallback", lambda: source.sample(), "UNAVAILABLE")
    check("failed_refresh_invalidates_calibration", source.last_observation is None)
    huge = clock.RealPublicClock(reference, "a"*64, test_observer=lambda _: dict(observation,
        utc_upper_utc=utc_from_us(clock.utc_microseconds(observation["utc_lower_utc"])+200000)))
    with patch.object(clock.time, "perf_counter_ns", return_value=1020000000):
        deny("uncertainty_exceeds_policy", lambda: huge.sample(), "UNCERTAINTY")
    return dict(CHECKS)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="fix2-clock-selftest-") as directory:
        qualification(directory)
    print(json.dumps({"checks": CHECKS, "check_count": len(CHECKS), "all_checks_true": all(CHECKS.values()),
        "network": False, "production_clock_policy_approved": False, "T010_executed": False}, sort_keys=True))
