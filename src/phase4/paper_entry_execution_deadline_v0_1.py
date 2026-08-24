from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


MODEL_ID = "P4-PAPER-ENTRY-EXECUTION-DEADLINE-0001"
SCHEMA_VERSION = "phase4_paper_entry_execution_deadline_v0.1"
LOCKED_EXIT_SPEC_FINGERPRINT = (
    "0bde5378f4ff2b63c6a96ad64140840ef8b2770c3be772a7aa71f8bdeb070129"
)
TRACK_ORDER = ("FINAL-A", "FINAL-B", "SENS-C")
ELIGIBILITY_WINDOW_US = {
    "FINAL-A": 15_000_000,
    "FINAL-B": 15_000_000,
    "SENS-C": 5_000_000,
}


POLICY_SPEC = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "locked_exit_spec_fingerprint": LOCKED_EXIT_SPEC_FINGERPRINT,
    "track_order": TRACK_ORDER,
    "eligibility_window_us": ELIGIBILITY_WINDOW_US,
    "boundary": "INCLUSIVE_AT_DEADLINE",
    "expiry_clock_offset_us": 1,
    "fallback_anchor": "CANDIDATE_SIGNAL_OBSERVED_AT_UNCHANGED",
    "track_independence": True,
    "paper_only": True,
}
MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(POLICY_SPEC, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()


@dataclass(frozen=True, slots=True)
class EntryExecutionDeadline:
    track_id: str
    signal_observed_at: datetime
    eligible_through_at: datetime
    expire_at: datetime

    def is_eligible(self, observed_at: datetime) -> bool:
        return _utc(observed_at) <= self.eligible_through_at

    def is_due(self, now: datetime) -> bool:
        return _utc(now) >= self.expire_at


def deadline_for(track_id: str, signal_observed_at: datetime) -> EntryExecutionDeadline:
    if track_id not in ELIGIBILITY_WINDOW_US:
        raise ValueError(f"unsupported Phase-4 track: {track_id}")
    signal = _utc(signal_observed_at)
    eligible = signal + timedelta(microseconds=ELIGIBILITY_WINDOW_US[track_id])
    return EntryExecutionDeadline(
        track_id=track_id,
        signal_observed_at=signal,
        eligible_through_at=eligible,
        expire_at=eligible + timedelta(microseconds=1),
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)
