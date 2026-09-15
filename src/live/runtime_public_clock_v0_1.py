"""Explicit read-only SNTP observations; no OS clock setting or fallback.

This is a bounded client of an explicitly approved honest-server/network trust
profile, not NTS authentication or an NTP discipline daemon. Discovery produces
observations only. An exact external review record is required for Authority
samples. Numerical ClockPolicy values have no production defaults.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import ipaddress
import os
import secrets
import socket
import struct
import time
from uuid import uuid4

from phase5.shadow_domain_v0_1 import content_fingerprint
from .authority_controls_v0_1 import (ClockPolicy, ClockReconciliation,
    TrustedClockSample, clock_reasons, require, utc_from_us)
from .ledger_actions_v0_1 import utc_microseconds

VERSION = "live_public_sntp_clock_v0.1"
TRUST = "UNAUTHENTICATED_SNTP_HONEST_SERVER_AND_NETWORK_REQUIRED_V1"
REVIEW_SCHEMA = "MEME_LIVE_PUBLIC_CLOCK_REVIEW_V1"
NTP_UNIX_DELTA = 2208988800


@dataclass(frozen=True, slots=True)
class NtpProvider:
    provider_id: str
    server_name: str
    address: str
    port: int
    ntp_era: int
    timeout_ms: int
    refresh_interval_us: int
    maximum_holdover_us: int
    trust_profile: str
    protocol_version: int

    def __post_init__(self):
        from .authority_controls_v0_1 import public_reference
        public_reference(self.provider_id)
        public_reference(self.server_name)
        ipaddress.ip_address(self.address)
        require(type(self.port) is int and self.port == 123 and type(self.ntp_era) is int
                and self.ntp_era in (0, 1) and type(self.protocol_version) is int
                and self.protocol_version in (3, 4), "PUBLIC_CLOCK_EXPLICIT_NTP_ENDPOINT_REQUIRED")
        require(type(self.timeout_ms) is int and 1 <= self.timeout_ms <= 10000
                and type(self.refresh_interval_us) is int
                and type(self.maximum_holdover_us) is int
                and 1000000 <= self.refresh_interval_us <= self.maximum_holdover_us <= 300000000,
                "PUBLIC_CLOCK_FINITE_OBSERVATION_BOUNDS_REQUIRED")
        require(self.trust_profile == TRUST, "PUBLIC_CLOCK_EXPLICIT_TRUST_PROFILE_REQUIRED")

    @property
    def fingerprint(self):
        return content_fingerprint({"implementation": VERSION, "counter": "perf_counter_ns", "provider": asdict(self)})


def _stamp(raw, era):
    seconds, fraction = struct.unpack("!II", raw)
    require(seconds != 0 or fraction != 0, "PUBLIC_CLOCK_ZERO_SERVER_TIMESTAMP")
    return ((seconds + era * (1 << 32) - NTP_UNIX_DELTA) * 1000000
            + fraction * 1000000 // (1 << 32))


def decode_observation(provider, packet, nonce, started_ns, received_ns):
    """Receipt calibration using nominal QPC elapsed, including asymmetry.

    Server transmit UTC <= receive UTC <= server transmit UTC + full client
    elapsed time. Root delay/2, dispersion and precision expand both sides.
    The approved ClockPolicy widens the RTT for counter drift in projected_bounds.
    No symmetric-network assumption, local UTC input, or midpoint-as-truth.
    """
    require(type(provider) is NtpProvider and type(packet) is bytes and len(packet) == 48
            and type(nonce) is bytes and len(nonce) == 8, "PUBLIC_CLOCK_EXACT_SNTP_PACKET_REQUIRED")
    require(type(started_ns) is int and type(received_ns) is int
            and 0 <= started_ns <= received_ns, "PUBLIC_CLOCK_MONOTONIC_REGRESSION")
    elapsed_us = (received_ns - started_ns + 999) // 1000
    require(elapsed_us <= provider.timeout_ms * 1000, "PUBLIC_CLOCK_OBSERVATION_TIMEOUT")
    flags, stratum, _, precision = struct.unpack("!BBBb", packet[:4])
    require(flags >> 6 == 0 and (flags >> 3) & 7 == provider.protocol_version and flags & 7 == 4
            and 1 <= stratum <= 15, "PUBLIC_CLOCK_UNSYNCHRONIZED_LEAP_OR_SERVER_MODE")
    require(packet[24:32] == nonce, "PUBLIC_CLOCK_REQUEST_CORRELATION_CONFLICT")
    root_delay = struct.unpack("!i", packet[4:8])[0]
    dispersion = struct.unpack("!I", packet[8:12])[0]
    require(root_delay >= 0 and -32 <= precision <= 0, "PUBLIC_CLOCK_INVALID_ROOT_DISTANCE")
    ref, receive, transmit = (_stamp(packet[offset:offset+8], provider.ntp_era)
                              for offset in (16, 32, 40))
    require(0 <= ref <= receive <= transmit and transmit-receive <= elapsed_us + 2,
            "PUBLIC_CLOCK_SERVER_TIMESTAMPS_CONFLICT")
    root_us = (root_delay * 1000000 + 131071) // 131072
    dispersion_us = (dispersion * 1000000 + 65535) // 65536
    precision_us = (1000000 + (1 << -precision) - 1) // (1 << -precision)
    uncertainty = root_us + dispersion_us + precision_us + 2
    return {"schema": "MEME_LIVE_SNTP_OBSERVATION_V1", "scope": "ACTUAL_PUBLIC_OBSERVATION",
        "provider_id": provider.provider_id, "provider_fingerprint": provider.fingerprint,
        "server_name": provider.server_name, "address": provider.address,
        "monotonic_ns": received_ns, "request_monotonic_ns": started_ns,
        "utc_lower_utc": utc_from_us(transmit-uncertainty),
        "utc_upper_utc": utc_from_us(transmit+elapsed_us+uncertainty),
        "round_trip_us": elapsed_us, "root_distance_us": uncertainty,
        "stratum": stratum, "packet_hex": packet.hex(), "packet_sha256": hashlib.sha256(packet).hexdigest(),
        "request_nonce_sha256": hashlib.sha256(nonce).hexdigest(),
        "authenticated_transport": False, "trust_profile": provider.trust_profile,
        "sets_system_time": False, "grants_permission": False}


def observe_provider(provider):
    """Exactly one read-only query to a pinned IP, with a finite UDP timeout."""
    require(type(provider) is NtpProvider, "PUBLIC_CLOCK_EXPLICIT_PROVIDER_REQUIRED")
    nonce = secrets.token_bytes(8)
    request = bytes([(provider.protocol_version << 3) | 3]) + b"\0" * 39 + nonce
    counter = time.get_clock_info("perf_counter")
    require(counter.monotonic and not counter.adjustable and counter.resolution <= 0.000001,
            "PUBLIC_CLOCK_HIGH_RESOLUTION_MONOTONIC_REQUIRED")
    family = socket.AF_INET6 if ipaddress.ip_address(provider.address).version == 6 else socket.AF_INET
    try:
        with socket.socket(family, socket.SOCK_DGRAM) as stream:
            stream.settimeout(provider.timeout_ms / 1000)
            stream.connect((provider.address, provider.port))
            started_ns = time.perf_counter_ns()
            stream.send(request)
            packet = stream.recv(512)
            received_ns = time.perf_counter_ns()
    except OSError:
        raise ValueError("PUBLIC_CLOCK_OBSERVATION_UNAVAILABLE") from None
    return decode_observation(provider, packet, nonce, started_ns, received_ns)


def projected_bounds(observation, monotonic_ns, provider, policy):
    delta = monotonic_ns - observation["monotonic_ns"]
    elapsed = (delta + 999) // 1000
    require(delta >= 0 and elapsed <= provider.maximum_holdover_us, "PUBLIC_CLOCK_STALE_OR_REGRESSED_CALIBRATION")
    drift = (elapsed * policy.maximum_drift_ppm + 999999) // 1000000
    # The response can spend the entire observed RTT on its return path.
    # Authority defines UTC elapsed within nominal elapsed +/- approved ppm;
    # that applies during observation as well as during later holdover.
    observed_elapsed = observation["round_trip_us"]
    require(type(observed_elapsed) is int and 0 <= observed_elapsed <= provider.timeout_ms * 1000,
        "PUBLIC_CLOCK_OBSERVATION_ELAPSED_INVALID")
    observation_drift = (observed_elapsed * policy.maximum_drift_ppm + 999999) // 1000000
    lower = utc_microseconds(observation["utc_lower_utc"]) + delta // 1000 - drift
    upper = utc_microseconds(observation["utc_upper_utc"]) + observation_drift + elapsed + drift
    require(0 <= upper - lower <= policy.maximum_uncertainty_us, "PUBLIC_CLOCK_UNCERTAINTY_EXCEEDED")
    return utc_from_us(lower), utc_from_us(upper)


def validate_sample_observation(observation, sample, clock):
    """Recompute the received packet interval and its exact monotonic projection."""
    require(sample.status == "QUALIFIED", "PUBLIC_CLOCK_QUALIFIED_SAMPLE_REQUIRED")
    packet = bytes.fromhex(observation["packet_hex"])
    decoded = decode_observation(clock.provider, packet, packet[24:32],
        observation["request_monotonic_ns"], observation["monotonic_ns"])
    require(dict(decoded, scope="PUBLIC_ENVIRONMENT_REVIEWED") == observation,
            "PUBLIC_CLOCK_PACKET_OBSERVATION_CONFLICT")
    require(projected_bounds(observation, sample.monotonic_ns, clock.provider, clock.policy)
            == (sample.utc_lower_utc, sample.utc_upper_utc), "PUBLIC_CLOCK_SAMPLE_INTERVAL_CONFLICT")


class RealPublicClock:
    """Fresh observations + bounded monotonic projection, original checkpoints.

    A new process/instance has a new epoch. Reconciliation references the exact
    durable last sample; it never resets Authority history or renews grants.
    An observation failure holds immediately, even if a cache was available.
    """
    def __init__(self, reviewed_reference, host_identity_digest, *, test_observer=None):
        from .runtime_dry_profile_v0_1 import read_reference, _keys
        record = read_reference(reviewed_reference)
        _keys(record, ("schema", "status", "origin", "provider", "clock_policy",
            "host_identity_digest", "review_reference"), "PUBLIC_CLOCK_EXPLICIT_REVIEW_REQUIRED")
        require(record["schema"] == REVIEW_SCHEMA and record["status"] == "APPROVED_FOR_PUBLIC_DRY"
                and record["origin"] == "HUMAN_EXTERNAL" and bool(record["review_reference"])
                and record["host_identity_digest"] == host_identity_digest,
                "PUBLIC_CLOCK_PROVIDER_POLICY_NOT_APPROVED")
        self.provider = NtpProvider(**record["provider"])
        self.policy = ClockPolicy(**record["clock_policy"])
        require((self.policy.provider_id, self.policy.provider_fingerprint) ==
                (self.provider.provider_id, self.provider.fingerprint), "PUBLIC_CLOCK_APPROVED_IDENTITY_CONFLICT")
        require(self.provider.maximum_holdover_us <= self.policy.maximum_checkpoint_elapsed_us,
                "PUBLIC_CLOCK_HOLDOVER_EXCEEDS_APPROVED_POLICY")
        self.reviewed_reference = dict(reviewed_reference)
        self.test_observer = test_observer
        self.scope = "TEST_ONLY_FAKE_EXTERNAL_TRANSPORT" if test_observer is not None else "PUBLIC_ENVIRONMENT_REVIEWED"
        self.epoch = "PUBLIC_CLOCK_" + uuid4().hex
        self.last_observation = None

    def sample(self, started=None):
        previous, state = None, None
        if started is not None:
            ledger = started.runtime.ledger
            with ledger._trusted_read():
                state = ledger._authority
            require(state.policy is not None and state.policy.clock == self.policy,
                    "PUBLIC_CLOCK_ORIGINAL_POLICY_PROVIDER_CONFLICT")
            previous = state.last_qualified_clock
        now = time.perf_counter_ns()
        cached = self.last_observation
        if cached is None or now-cached["monotonic_ns"] >= self.provider.refresh_interval_us*1000:
            # Never return a retained calibration when the fresh provider fails.
            self.last_observation = None
            observation = (observe_provider(self.provider) if self.test_observer is None
                           else self.test_observer(self.provider))
            require(observation["provider_fingerprint"] == self.provider.fingerprint
                    and observation["provider_id"] == self.provider.provider_id,
                    "PUBLIC_CLOCK_OBSERVATION_PROVIDER_CONFLICT")
            observation = dict(observation, scope=self.scope)
            self.last_observation = observation
            cached = observation
            now = time.perf_counter_ns()
        lower, upper = projected_bounds(cached, now, self.provider, self.policy)
        epoch = self.epoch + "_" + str(os.getpid())
        reconciliation = None
        if previous is not None and (previous.epoch_id != epoch or state.clock_requires_reconciliation):
            reconciliation = ClockReconciliation(previous.content_digest, previous.epoch_id, previous.monotonic_ns,
                content_fingerprint({"review": self.reviewed_reference, "fresh_observation": cached,
                                     "previous_checkpoint": asdict(previous)}))
        sample = TrustedClockSample(self.provider.provider_id, self.provider.fingerprint, epoch, now,
            lower, upper, content_fingerprint({"review": self.reviewed_reference,
                "observation": cached, "sample_monotonic_ns": now}),
            "0"*64 if previous is None else previous.content_digest, "QUALIFIED", reconciliation)
        if state is not None:
            require(not clock_reasons(state, sample), "PUBLIC_CLOCK_ORIGINAL_CONTINUITY_DENIED")
        return sample

    __call__ = sample
