from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from fractions import Fraction
from typing import Iterable, Sequence


SCHEMA_VERSION = "phase4_paper_exit_orchestrator_v0.1"
ENGINE_VERSION = "PaperExitOrchestratorV01"

# Phase-3 COMPLETE -> Phase-4 locked handoff.  These are not tunable here.
LOCKED_TRACK_ORDER: tuple[str, ...] = ("FINAL-A", "FINAL-B", "SENS-C")


class ExitMode(StrEnum):
    FIXED_TP = "FIXED_TP"
    TRAILING = "TRAILING"


class ExitReason(StrEnum):
    TAKE_PROFIT = "TAKE_PROFIT"
    TRAIL = "TRAIL"
    FALLBACK = "FALLBACK"


class ExitTrackState(StrEnum):
    MONITORING = "MONITORING"
    EXIT_INTENT = "EXIT_INTENT"


@dataclass(frozen=True, slots=True)
class ExitTrackSpec:
    track_id: str
    exit_variant: str
    mode: ExitMode
    fallback_ms: int
    tp_bps: int | None = None
    trail_activation_bps: int | None = None
    trail_giveback_bps: int | None = None
    sensitivity_only: bool = False

    def __post_init__(self) -> None:
        if not self.track_id or not self.exit_variant:
            raise ValueError("track_id and exit_variant are required")
        if self.fallback_ms <= 0:
            raise ValueError("fallback_ms must be > 0")
        if self.mode is ExitMode.FIXED_TP:
            if self.tp_bps is None or self.tp_bps <= 0:
                raise ValueError("FIXED_TP requires tp_bps > 0")
            if self.trail_activation_bps is not None or self.trail_giveback_bps is not None:
                raise ValueError("FIXED_TP cannot define trailing parameters")
        elif self.mode is ExitMode.TRAILING:
            if self.tp_bps is not None:
                raise ValueError("TRAILING cannot define fixed TP")
            if self.trail_activation_bps is None or self.trail_activation_bps <= 0:
                raise ValueError("TRAILING requires trail_activation_bps > 0")
            if self.trail_giveback_bps is None or self.trail_giveback_bps <= 0:
                raise ValueError("TRAILING requires trail_giveback_bps > 0")


LOCKED_EXIT_SPECS: dict[str, ExitTrackSpec] = {
    "FINAL-A": ExitTrackSpec(
        track_id="FINAL-A",
        exit_variant="E3_TP10_T15",
        mode=ExitMode.FIXED_TP,
        tp_bps=1000,
        fallback_ms=15_000,
    ),
    "FINAL-B": ExitTrackSpec(
        track_id="FINAL-B",
        exit_variant="E4_ACT10_GB03_T15",
        mode=ExitMode.TRAILING,
        trail_activation_bps=1000,
        trail_giveback_bps=300,
        fallback_ms=15_000,
    ),
    "SENS-C": ExitTrackSpec(
        track_id="SENS-C",
        exit_variant="SENS_TP20_T5",
        mode=ExitMode.FIXED_TP,
        tp_bps=2000,
        fallback_ms=5_000,
        sensitivity_only=True,
    ),
}


def _spec_fingerprint_payload() -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for track_id in LOCKED_TRACK_ORDER:
        s = LOCKED_EXIT_SPECS[track_id]
        out.append(
            {
                "track_id": s.track_id,
                "exit_variant": s.exit_variant,
                "mode": s.mode.value,
                "fallback_ms": s.fallback_ms,
                "tp_bps": s.tp_bps,
                "trail_activation_bps": s.trail_activation_bps,
                "trail_giveback_bps": s.trail_giveback_bps,
                "sensitivity_only": s.sensitivity_only,
            }
        )
    return out


def locked_exit_spec_fingerprint() -> str:
    body = json.dumps(
        _spec_fingerprint_payload(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


LOCKED_EXIT_SPEC_FINGERPRINT = locked_exit_spec_fingerprint()


@dataclass(frozen=True, slots=True)
class ExitPositionRef:
    """Paper-position binding for one locked Phase-4 exit track.

    Rule semantics intentionally preserve the Phase-3 finalist definition:
    - threshold returns are measured from the causal CandidateSignal reference;
    - fallback clock is anchored to signal_observed_at;
    - the position cannot react to observations before its actual paper entry fill.

    The simulated entry price is retained for audit/accounting and later PnL, but
    is NOT silently substituted as a new exit-parameter reference in v0.1.
    """

    paper_position_id: str
    paper_order_id: str
    signal_key: str
    mint: str
    track_id: str
    exit_variant: str
    price_identity: str
    signal_observed_at: datetime
    signal_ingest_seq: int
    rule_reference_price_numerator_raw: int
    rule_reference_price_denominator_raw: int
    entry_market_observed_at: datetime
    entry_market_ingest_seq: int
    open_at: datetime
    entry_price_numerator_raw: int
    entry_price_denominator_raw: int

    def __post_init__(self) -> None:
        for label, value in (
            ("paper_position_id", self.paper_position_id),
            ("paper_order_id", self.paper_order_id),
            ("signal_key", self.signal_key),
            ("mint", self.mint),
            ("track_id", self.track_id),
            ("exit_variant", self.exit_variant),
            ("price_identity", self.price_identity),
        ):
            if not value:
                raise ValueError(f"{label} is required")
        _require_utc(self.signal_observed_at)
        _require_utc(self.entry_market_observed_at)
        _require_utc(self.open_at)
        if self.signal_ingest_seq < 0 or self.entry_market_ingest_seq < 0:
            raise ValueError("ingest_seq values must be >= 0")
        for label, value in (
            ("rule reference numerator", self.rule_reference_price_numerator_raw),
            ("rule reference denominator", self.rule_reference_price_denominator_raw),
            ("entry price numerator", self.entry_price_numerator_raw),
            ("entry price denominator", self.entry_price_denominator_raw),
        ):
            if value <= 0:
                raise ValueError(f"{label} must be > 0")
        spec = LOCKED_EXIT_SPECS.get(self.track_id)
        if spec is None:
            raise ValueError(f"unknown locked Phase-4 track: {self.track_id}")
        if self.exit_variant != spec.exit_variant:
            raise ValueError(
                f"exit_variant mismatch for {self.track_id}: "
                f"expected {spec.exit_variant}, got {self.exit_variant}"
            )
        if _obs_key(self.entry_market_observed_at, self.entry_market_ingest_seq) < _obs_key(
            self.signal_observed_at, self.signal_ingest_seq
        ):
            raise ValueError("entry market observation cannot precede CandidateSignal")
        if self.open_at < self.entry_market_observed_at:
            raise ValueError("open_at cannot precede the market observation used for entry fill")


@dataclass(frozen=True, slots=True)
class ExitMarketObservation:
    mint: str
    observed_at: datetime
    ingest_seq: int
    price_identity: str
    price_numerator_raw: int
    price_denominator_raw: int
    is_gap_recovery: bool = False
    source_event_key: str | None = None

    def __post_init__(self) -> None:
        if not self.mint:
            raise ValueError("mint is required")
        _require_utc(self.observed_at)
        if self.ingest_seq < 0:
            raise ValueError("ingest_seq must be >= 0")
        if not self.price_identity:
            raise ValueError("price_identity is required")
        if self.price_numerator_raw <= 0 or self.price_denominator_raw <= 0:
            raise ValueError("price numerator/denominator must be > 0")


@dataclass(frozen=True, slots=True)
class PersistedExitTrack:
    paper_position_id: str
    paper_order_id: str
    signal_key: str
    mint: str
    track_id: str
    exit_variant: str
    price_identity: str
    signal_observed_at: datetime
    signal_ingest_seq: int
    rule_reference_price_numerator_raw: int
    rule_reference_price_denominator_raw: int
    entry_market_observed_at: datetime
    entry_market_ingest_seq: int
    open_at: datetime
    entry_price_numerator_raw: int
    entry_price_denominator_raw: int
    fallback_deadline_at: datetime
    fallback_fire_at: datetime
    state: ExitTrackState
    trail_activated: bool
    activation_at: datetime | None
    activation_ingest_seq: int | None
    peak_return_bps: int | None
    peak_at: datetime | None
    peak_ingest_seq: int | None
    last_seen_at: datetime | None
    last_seen_ingest_seq: int | None
    last_seen_fingerprint: str | None
    last_fresh_at: datetime | None
    last_fresh_ingest_seq: int | None
    last_fresh_source_event_key: str | None
    last_fresh_price_numerator_raw: int | None
    last_fresh_price_denominator_raw: int | None
    last_fresh_return_bps: int | None
    exit_intent_id: str | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ExitIntent:
    exit_intent_id: str
    paper_position_id: str
    paper_order_id: str
    signal_key: str
    mint: str
    track_id: str
    exit_variant: str
    reason: ExitReason
    requested_at: datetime
    trigger_observed_at: datetime | None
    trigger_ingest_seq: int | None
    trigger_source_event_key: str | None
    trigger_price_numerator_raw: int | None
    trigger_price_denominator_raw: int | None
    rule_return_bps: int | None
    trail_peak_return_bps: int | None
    last_fresh_observed_at: datetime | None
    last_fresh_ingest_seq: int | None
    last_fresh_return_bps: int | None


@dataclass(frozen=True, slots=True)
class ExitEvaluation:
    track: PersistedExitTrack
    intent: ExitIntent | None
    observation_accepted: bool
    ignored_reason: str | None


class ExitDeterminismConflict(RuntimeError):
    pass


class PaperExitOrchestratorV01:
    """Persistent deterministic exit-intent foundation.

    This layer decides *when an exit should be requested*.  It intentionally
    does not fabricate an exit fill, mutate paper_positions, calculate PnL, use
    a wallet, sign, broadcast, access RPC, or tune/reselect parameters.

    Exact boundary policy:
    - market observation at exactly the fallback deadline remains eligible;
    - the fallback timer fires at deadline + 1 microsecond, matching the
      project's existing inclusive-boundary / +1us deterministic clock style;
    - an observation after the deadline cannot retroactively defeat fallback.

    Trailing semantics:
    - activate on first fresh same-identity observed point with return >=
      activation threshold;
    - keep the highest observed rule return after activation;
    - trigger on first observed point whose return is at least giveback_bps
      below that observed peak; no interpolation.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        _create_schema(self.conn)

    def bind_position(self, ref: ExitPositionRef) -> PersistedExitTrack:
        existing = self.get_track(ref.paper_position_id)
        if existing is not None:
            self._assert_ref_matches(existing, ref)
            return existing

        spec = LOCKED_EXIT_SPECS[ref.track_id]
        deadline = ref.signal_observed_at + timedelta(milliseconds=spec.fallback_ms)
        fire_at = deadline + timedelta(microseconds=1)
        now = max(ref.open_at, ref.entry_market_observed_at)

        with self.conn:
            self.conn.execute(
                """
                INSERT INTO paper_exit_track_states(
                    paper_position_id, paper_order_id, signal_key, mint, track_id,
                    exit_variant, price_identity, signal_observed_at,
                    signal_ingest_seq, rule_reference_price_numerator_raw,
                    rule_reference_price_denominator_raw,
                    entry_market_observed_at, entry_market_ingest_seq, open_at,
                    entry_price_numerator_raw, entry_price_denominator_raw,
                    fallback_deadline_at, fallback_fire_at, state,
                    trail_activated, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    ref.paper_position_id,
                    ref.paper_order_id,
                    ref.signal_key,
                    ref.mint,
                    ref.track_id,
                    ref.exit_variant,
                    ref.price_identity,
                    _dt_text(ref.signal_observed_at),
                    ref.signal_ingest_seq,
                    str(ref.rule_reference_price_numerator_raw),
                    str(ref.rule_reference_price_denominator_raw),
                    _dt_text(ref.entry_market_observed_at),
                    ref.entry_market_ingest_seq,
                    _dt_text(ref.open_at),
                    str(ref.entry_price_numerator_raw),
                    str(ref.entry_price_denominator_raw),
                    _dt_text(deadline),
                    _dt_text(fire_at),
                    ExitTrackState.MONITORING.value,
                    _dt_text(now),
                ),
            )
        return self._require_track(ref.paper_position_id)

    def bind_parallel_positions(
        self, refs: Sequence[ExitPositionRef]
    ) -> tuple[PersistedExitTrack, ...]:
        by_track = {r.track_id: r for r in refs}
        if set(by_track) != set(LOCKED_TRACK_ORDER) or len(refs) != len(LOCKED_TRACK_ORDER):
            raise ValueError(
                f"parallel binding requires exactly {LOCKED_TRACK_ORDER}; "
                f"got {tuple(sorted(by_track))}"
            )

        ordered_refs = tuple(by_track[t] for t in LOCKED_TRACK_ORDER)
        first = ordered_refs[0]
        common = (
            first.signal_key,
            first.mint,
            first.price_identity,
            first.signal_observed_at,
            first.signal_ingest_seq,
            first.rule_reference_price_numerator_raw,
            first.rule_reference_price_denominator_raw,
            first.entry_market_observed_at,
            first.entry_market_ingest_seq,
            first.open_at,
        )
        for ref in ordered_refs[1:]:
            other = (
                ref.signal_key,
                ref.mint,
                ref.price_identity,
                ref.signal_observed_at,
                ref.signal_ingest_seq,
                ref.rule_reference_price_numerator_raw,
                ref.rule_reference_price_denominator_raw,
                ref.entry_market_observed_at,
                ref.entry_market_ingest_seq,
                ref.open_at,
            )
            if other != common:
                raise ValueError("parallel Phase-4 tracks must share one causal entry lineage")

        return tuple(self.bind_position(ref) for ref in ordered_refs)

    def on_market_observation(
        self,
        ref: ExitPositionRef,
        obs: ExitMarketObservation,
    ) -> ExitEvaluation:
        track = self.bind_position(ref)
        terminal = self.get_intent_for_position(ref.paper_position_id)
        if terminal is not None:
            return ExitEvaluation(track, terminal, False, "TERMINAL_REPLAY")

        if obs.mint != ref.mint:
            return ExitEvaluation(track, None, False, "CROSS_MINT")

        # A position cannot react to a market point at/before the actual entry
        # fill observation, even though Phase-3 rule thresholds remain anchored
        # to the CandidateSignal reference.
        if _obs_key(obs.observed_at, obs.ingest_seq) <= _obs_key(
            ref.entry_market_observed_at, ref.entry_market_ingest_seq
        ):
            return ExitEvaluation(track, None, False, "AT_OR_BEFORE_ENTRY_FILL")

        # Exact fallback deadline remains market-eligible.  Anything later must
        # first cause the deterministic fallback clock to fire.
        if obs.observed_at > track.fallback_deadline_at:
            clock_eval = self.on_clock(ref, track.fallback_fire_at)
            if clock_eval.intent is not None:
                return ExitEvaluation(clock_eval.track, clock_eval.intent, False, "FALLBACK_PRECEDES_LATE_OBSERVATION")
            track = clock_eval.track

        fingerprint = _observation_fingerprint(obs)
        if track.last_seen_at is not None and track.last_seen_ingest_seq is not None:
            old_key = _obs_key(track.last_seen_at, track.last_seen_ingest_seq)
            new_key = _obs_key(obs.observed_at, obs.ingest_seq)
            if new_key < old_key:
                return ExitEvaluation(track, None, False, "OUT_OF_ORDER_REPLAY")
            if new_key == old_key:
                if track.last_seen_fingerprint != fingerprint:
                    raise ExitDeterminismConflict(
                        "same observed_at + ingest_seq replayed with different content"
                    )
                return ExitEvaluation(track, None, False, "EXACT_REPLAY")

        # Same-mint observations are provenance even when unusable for price.
        self._update_last_seen(ref.paper_position_id, obs, fingerprint)
        track = self._require_track(ref.paper_position_id)

        if obs.is_gap_recovery:
            return ExitEvaluation(track, None, False, "GAP_RECOVERY_NOT_FRESH")
        if obs.price_identity != ref.price_identity:
            return ExitEvaluation(track, None, False, "PRICE_IDENTITY_MISMATCH")

        ret_bps = _return_bps(
            ref.rule_reference_price_numerator_raw,
            ref.rule_reference_price_denominator_raw,
            obs.price_numerator_raw,
            obs.price_denominator_raw,
        )
        self._update_last_fresh(ref.paper_position_id, obs, ret_bps)
        track = self._require_track(ref.paper_position_id)
        spec = LOCKED_EXIT_SPECS[ref.track_id]

        if spec.mode is ExitMode.FIXED_TP:
            assert spec.tp_bps is not None
            if ret_bps >= spec.tp_bps:
                intent = self._create_intent(
                    ref,
                    reason=ExitReason.TAKE_PROFIT,
                    requested_at=obs.observed_at,
                    trigger=obs,
                    rule_return_bps=ret_bps,
                    trail_peak_return_bps=None,
                )
                return ExitEvaluation(self._require_track(ref.paper_position_id), intent, True, None)
            return ExitEvaluation(track, None, True, None)

        assert spec.mode is ExitMode.TRAILING
        assert spec.trail_activation_bps is not None
        assert spec.trail_giveback_bps is not None

        if not track.trail_activated:
            if ret_bps >= spec.trail_activation_bps:
                self._activate_trail(ref.paper_position_id, obs, ret_bps)
                track = self._require_track(ref.paper_position_id)
            return ExitEvaluation(track, None, True, None)

        peak = track.peak_return_bps
        if peak is None:
            raise ExitDeterminismConflict("activated trailing track has no persisted peak")

        if ret_bps > peak:
            self._update_peak(ref.paper_position_id, obs, ret_bps)
            track = self._require_track(ref.paper_position_id)
            peak = ret_bps

        if peak - ret_bps >= spec.trail_giveback_bps:
            intent = self._create_intent(
                ref,
                reason=ExitReason.TRAIL,
                requested_at=obs.observed_at,
                trigger=obs,
                rule_return_bps=ret_bps,
                trail_peak_return_bps=peak,
            )
            return ExitEvaluation(self._require_track(ref.paper_position_id), intent, True, None)

        return ExitEvaluation(track, None, True, None)

    def on_market_observation_parallel(
        self,
        refs: Sequence[ExitPositionRef],
        obs: ExitMarketObservation,
    ) -> tuple[ExitEvaluation, ...]:
        self.bind_parallel_positions(refs)
        by_track = {r.track_id: r for r in refs}
        return tuple(self.on_market_observation(by_track[t], obs) for t in LOCKED_TRACK_ORDER)

    def on_clock(self, ref: ExitPositionRef, now: datetime) -> ExitEvaluation:
        _require_utc(now)
        track = self.bind_position(ref)
        terminal = self.get_intent_for_position(ref.paper_position_id)
        if terminal is not None:
            return ExitEvaluation(track, terminal, False, "TERMINAL_REPLAY")
        if now < track.fallback_fire_at:
            return ExitEvaluation(track, None, False, "FALLBACK_NOT_DUE")

        # Timer itself is sufficient to request an exit.  No synthetic price is
        # invented here; a later causal exit-fill layer must obtain a real fresh
        # market observation after its own exit latency.
        intent = self._create_intent(
            ref,
            reason=ExitReason.FALLBACK,
            requested_at=track.fallback_fire_at,
            trigger=None,
            rule_return_bps=None,
            trail_peak_return_bps=track.peak_return_bps,
        )
        return ExitEvaluation(self._require_track(ref.paper_position_id), intent, False, None)

    def on_clock_parallel(
        self,
        refs: Sequence[ExitPositionRef],
        now: datetime,
    ) -> tuple[ExitEvaluation, ...]:
        self.bind_parallel_positions(refs)
        by_track = {r.track_id: r for r in refs}
        return tuple(self.on_clock(by_track[t], now) for t in LOCKED_TRACK_ORDER)

    def get_track(self, paper_position_id: str) -> PersistedExitTrack | None:
        row = self.conn.execute(
            "SELECT * FROM paper_exit_track_states WHERE paper_position_id=?",
            (paper_position_id,),
        ).fetchone()
        return None if row is None else _track_from_row(row)

    def get_intent_for_position(self, paper_position_id: str) -> ExitIntent | None:
        row = self.conn.execute(
            "SELECT * FROM paper_exit_intents WHERE paper_position_id=?",
            (paper_position_id,),
        ).fetchone()
        return None if row is None else _intent_from_row(row)

    def list_tracks(self) -> tuple[PersistedExitTrack, ...]:
        rows = self.conn.execute(
            "SELECT * FROM paper_exit_track_states"
        ).fetchall()
        mapped = {row["track_id"]: _track_from_row(row) for row in rows}
        ordered = [mapped[t] for t in LOCKED_TRACK_ORDER if t in mapped]
        extras = sorted(
            (_track_from_row(r) for r in rows if r["track_id"] not in LOCKED_TRACK_ORDER),
            key=lambda x: (x.track_id, x.paper_position_id),
        )
        return tuple(ordered + extras)

    def canonical_digest(self) -> str:
        tracks = self.conn.execute(
            "SELECT * FROM paper_exit_track_states ORDER BY track_id, paper_position_id"
        ).fetchall()
        intents = self.conn.execute(
            "SELECT * FROM paper_exit_intents ORDER BY track_id, paper_position_id"
        ).fetchall()
        payload = {
            "schema_version": SCHEMA_VERSION,
            "spec_fingerprint": LOCKED_EXIT_SPEC_FINGERPRINT,
            "tracks": [{k: row[k] for k in row.keys()} for row in tracks],
            "intents": [{k: row[k] for k in row.keys()} for row in intents],
        }
        body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(body.encode("utf-8")).hexdigest()

    def _assert_ref_matches(self, track: PersistedExitTrack, ref: ExitPositionRef) -> None:
        expected = (
            ref.paper_order_id,
            ref.signal_key,
            ref.mint,
            ref.track_id,
            ref.exit_variant,
            ref.price_identity,
            _dt_text(ref.signal_observed_at),
            ref.signal_ingest_seq,
            ref.rule_reference_price_numerator_raw,
            ref.rule_reference_price_denominator_raw,
            _dt_text(ref.entry_market_observed_at),
            ref.entry_market_ingest_seq,
            _dt_text(ref.open_at),
            ref.entry_price_numerator_raw,
            ref.entry_price_denominator_raw,
        )
        actual = (
            track.paper_order_id,
            track.signal_key,
            track.mint,
            track.track_id,
            track.exit_variant,
            track.price_identity,
            _dt_text(track.signal_observed_at),
            track.signal_ingest_seq,
            track.rule_reference_price_numerator_raw,
            track.rule_reference_price_denominator_raw,
            _dt_text(track.entry_market_observed_at),
            track.entry_market_ingest_seq,
            _dt_text(track.open_at),
            track.entry_price_numerator_raw,
            track.entry_price_denominator_raw,
        )
        if actual != expected:
            raise ExitDeterminismConflict("replayed ExitPositionRef differs from persisted binding")

    def _update_last_seen(
        self,
        paper_position_id: str,
        obs: ExitMarketObservation,
        fingerprint: str,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE paper_exit_track_states
                SET last_seen_at=?, last_seen_ingest_seq=?, last_seen_fingerprint=?, updated_at=?
                WHERE paper_position_id=?
                """,
                (
                    _dt_text(obs.observed_at),
                    obs.ingest_seq,
                    fingerprint,
                    _dt_text(obs.observed_at),
                    paper_position_id,
                ),
            )

    def _update_last_fresh(
        self,
        paper_position_id: str,
        obs: ExitMarketObservation,
        ret_bps: int,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE paper_exit_track_states
                SET last_fresh_at=?, last_fresh_ingest_seq=?,
                    last_fresh_source_event_key=?,
                    last_fresh_price_numerator_raw=?,
                    last_fresh_price_denominator_raw=?,
                    last_fresh_return_bps=?, updated_at=?
                WHERE paper_position_id=?
                """,
                (
                    _dt_text(obs.observed_at),
                    obs.ingest_seq,
                    obs.source_event_key,
                    str(obs.price_numerator_raw),
                    str(obs.price_denominator_raw),
                    ret_bps,
                    _dt_text(obs.observed_at),
                    paper_position_id,
                ),
            )

    def _activate_trail(
        self,
        paper_position_id: str,
        obs: ExitMarketObservation,
        ret_bps: int,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE paper_exit_track_states
                SET trail_activated=1, activation_at=?, activation_ingest_seq=?,
                    peak_return_bps=?, peak_at=?, peak_ingest_seq=?, updated_at=?
                WHERE paper_position_id=?
                """,
                (
                    _dt_text(obs.observed_at),
                    obs.ingest_seq,
                    ret_bps,
                    _dt_text(obs.observed_at),
                    obs.ingest_seq,
                    _dt_text(obs.observed_at),
                    paper_position_id,
                ),
            )

    def _update_peak(
        self,
        paper_position_id: str,
        obs: ExitMarketObservation,
        ret_bps: int,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE paper_exit_track_states
                SET peak_return_bps=?, peak_at=?, peak_ingest_seq=?, updated_at=?
                WHERE paper_position_id=?
                """,
                (
                    ret_bps,
                    _dt_text(obs.observed_at),
                    obs.ingest_seq,
                    _dt_text(obs.observed_at),
                    paper_position_id,
                ),
            )

    def _create_intent(
        self,
        ref: ExitPositionRef,
        *,
        reason: ExitReason,
        requested_at: datetime,
        trigger: ExitMarketObservation | None,
        rule_return_bps: int | None,
        trail_peak_return_bps: int | None,
    ) -> ExitIntent:
        existing = self.get_intent_for_position(ref.paper_position_id)
        if existing is not None:
            return existing

        track = self._require_track(ref.paper_position_id)
        trigger_key = (
            "<TIMER>"
            if trigger is None
            else f"{_dt_text(trigger.observed_at)}|{trigger.ingest_seq}|{trigger.source_event_key or ''}"
        )
        intent_id = _hash_id(
            "PXI",
            SCHEMA_VERSION,
            ref.paper_position_id,
            ref.exit_variant,
            reason.value,
            _dt_text(requested_at),
            trigger_key,
        )

        with self.conn:
            self.conn.execute(
                """
                INSERT INTO paper_exit_intents(
                    exit_intent_id, paper_position_id, paper_order_id, signal_key,
                    mint, track_id, exit_variant, reason, requested_at,
                    trigger_observed_at, trigger_ingest_seq, trigger_source_event_key,
                    trigger_price_numerator_raw, trigger_price_denominator_raw,
                    rule_return_bps, trail_peak_return_bps,
                    last_fresh_observed_at, last_fresh_ingest_seq,
                    last_fresh_return_bps
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    intent_id,
                    ref.paper_position_id,
                    ref.paper_order_id,
                    ref.signal_key,
                    ref.mint,
                    ref.track_id,
                    ref.exit_variant,
                    reason.value,
                    _dt_text(requested_at),
                    None if trigger is None else _dt_text(trigger.observed_at),
                    None if trigger is None else trigger.ingest_seq,
                    None if trigger is None else trigger.source_event_key,
                    None if trigger is None else str(trigger.price_numerator_raw),
                    None if trigger is None else str(trigger.price_denominator_raw),
                    rule_return_bps,
                    trail_peak_return_bps,
                    None if track.last_fresh_at is None else _dt_text(track.last_fresh_at),
                    track.last_fresh_ingest_seq,
                    track.last_fresh_return_bps,
                ),
            )
            self.conn.execute(
                """
                UPDATE paper_exit_track_states
                SET state=?, exit_intent_id=?, updated_at=?
                WHERE paper_position_id=?
                """,
                (
                    ExitTrackState.EXIT_INTENT.value,
                    intent_id,
                    _dt_text(requested_at),
                    ref.paper_position_id,
                ),
            )

        return self._require_intent(intent_id)

    def _require_track(self, paper_position_id: str) -> PersistedExitTrack:
        track = self.get_track(paper_position_id)
        if track is None:
            raise KeyError(f"unknown paper_position_id: {paper_position_id}")
        return track

    def _require_intent(self, exit_intent_id: str) -> ExitIntent:
        row = self.conn.execute(
            "SELECT * FROM paper_exit_intents WHERE exit_intent_id=?",
            (exit_intent_id,),
        ).fetchone()
        if row is None:
            raise KeyError(exit_intent_id)
        return _intent_from_row(row)


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS paper_exit_schema_meta (
            schema_version TEXT PRIMARY KEY,
            engine_version TEXT NOT NULL,
            locked_spec_fingerprint TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS paper_exit_track_states (
            paper_position_id TEXT PRIMARY KEY,
            paper_order_id TEXT NOT NULL,
            signal_key TEXT NOT NULL,
            mint TEXT NOT NULL,
            track_id TEXT NOT NULL,
            exit_variant TEXT NOT NULL,
            price_identity TEXT NOT NULL,
            signal_observed_at TEXT NOT NULL,
            signal_ingest_seq INTEGER NOT NULL,
            rule_reference_price_numerator_raw TEXT NOT NULL,
            rule_reference_price_denominator_raw TEXT NOT NULL,
            entry_market_observed_at TEXT NOT NULL,
            entry_market_ingest_seq INTEGER NOT NULL,
            open_at TEXT NOT NULL,
            entry_price_numerator_raw TEXT NOT NULL,
            entry_price_denominator_raw TEXT NOT NULL,
            fallback_deadline_at TEXT NOT NULL,
            fallback_fire_at TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('MONITORING','EXIT_INTENT')),
            trail_activated INTEGER NOT NULL CHECK(trail_activated IN (0,1)),
            activation_at TEXT,
            activation_ingest_seq INTEGER,
            peak_return_bps INTEGER,
            peak_at TEXT,
            peak_ingest_seq INTEGER,
            last_seen_at TEXT,
            last_seen_ingest_seq INTEGER,
            last_seen_fingerprint TEXT,
            last_fresh_at TEXT,
            last_fresh_ingest_seq INTEGER,
            last_fresh_source_event_key TEXT,
            last_fresh_price_numerator_raw TEXT,
            last_fresh_price_denominator_raw TEXT,
            last_fresh_return_bps INTEGER,
            exit_intent_id TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE(signal_key, track_id)
        );

        CREATE TABLE IF NOT EXISTS paper_exit_intents (
            exit_intent_id TEXT PRIMARY KEY,
            paper_position_id TEXT NOT NULL UNIQUE,
            paper_order_id TEXT NOT NULL,
            signal_key TEXT NOT NULL,
            mint TEXT NOT NULL,
            track_id TEXT NOT NULL,
            exit_variant TEXT NOT NULL,
            reason TEXT NOT NULL CHECK(reason IN ('TAKE_PROFIT','TRAIL','FALLBACK')),
            requested_at TEXT NOT NULL,
            trigger_observed_at TEXT,
            trigger_ingest_seq INTEGER,
            trigger_source_event_key TEXT,
            trigger_price_numerator_raw TEXT,
            trigger_price_denominator_raw TEXT,
            rule_return_bps INTEGER,
            trail_peak_return_bps INTEGER,
            last_fresh_observed_at TEXT,
            last_fresh_ingest_seq INTEGER,
            last_fresh_return_bps INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_paper_exit_track_state
            ON paper_exit_track_states(state, track_id);
        CREATE INDEX IF NOT EXISTS idx_paper_exit_intent_requested
            ON paper_exit_intents(requested_at, track_id);
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO paper_exit_schema_meta(
            schema_version, engine_version, locked_spec_fingerprint
        ) VALUES (?, ?, ?)
        """,
        (SCHEMA_VERSION, ENGINE_VERSION, LOCKED_EXIT_SPEC_FINGERPRINT),
    )
    row = conn.execute(
        "SELECT engine_version, locked_spec_fingerprint FROM paper_exit_schema_meta WHERE schema_version=?",
        (SCHEMA_VERSION,),
    ).fetchone()
    if row is None:
        raise RuntimeError("paper exit schema metadata missing")
    if row["engine_version"] != ENGINE_VERSION or row["locked_spec_fingerprint"] != LOCKED_EXIT_SPEC_FINGERPRINT:
        raise ExitDeterminismConflict("paper exit schema metadata conflicts with locked v0.1 engine")
    conn.commit()


def _track_from_row(row: sqlite3.Row) -> PersistedExitTrack:
    return PersistedExitTrack(
        paper_position_id=row["paper_position_id"],
        paper_order_id=row["paper_order_id"],
        signal_key=row["signal_key"],
        mint=row["mint"],
        track_id=row["track_id"],
        exit_variant=row["exit_variant"],
        price_identity=row["price_identity"],
        signal_observed_at=_dt_parse(row["signal_observed_at"]),
        signal_ingest_seq=int(row["signal_ingest_seq"]),
        rule_reference_price_numerator_raw=int(row["rule_reference_price_numerator_raw"]),
        rule_reference_price_denominator_raw=int(row["rule_reference_price_denominator_raw"]),
        entry_market_observed_at=_dt_parse(row["entry_market_observed_at"]),
        entry_market_ingest_seq=int(row["entry_market_ingest_seq"]),
        open_at=_dt_parse(row["open_at"]),
        entry_price_numerator_raw=int(row["entry_price_numerator_raw"]),
        entry_price_denominator_raw=int(row["entry_price_denominator_raw"]),
        fallback_deadline_at=_dt_parse(row["fallback_deadline_at"]),
        fallback_fire_at=_dt_parse(row["fallback_fire_at"]),
        state=ExitTrackState(row["state"]),
        trail_activated=bool(row["trail_activated"]),
        activation_at=_dt_parse_optional(row["activation_at"]),
        activation_ingest_seq=_int_optional(row["activation_ingest_seq"]),
        peak_return_bps=_int_optional(row["peak_return_bps"]),
        peak_at=_dt_parse_optional(row["peak_at"]),
        peak_ingest_seq=_int_optional(row["peak_ingest_seq"]),
        last_seen_at=_dt_parse_optional(row["last_seen_at"]),
        last_seen_ingest_seq=_int_optional(row["last_seen_ingest_seq"]),
        last_seen_fingerprint=row["last_seen_fingerprint"],
        last_fresh_at=_dt_parse_optional(row["last_fresh_at"]),
        last_fresh_ingest_seq=_int_optional(row["last_fresh_ingest_seq"]),
        last_fresh_source_event_key=row["last_fresh_source_event_key"],
        last_fresh_price_numerator_raw=_int_optional(row["last_fresh_price_numerator_raw"]),
        last_fresh_price_denominator_raw=_int_optional(row["last_fresh_price_denominator_raw"]),
        last_fresh_return_bps=_int_optional(row["last_fresh_return_bps"]),
        exit_intent_id=row["exit_intent_id"],
        updated_at=_dt_parse(row["updated_at"]),
    )


def _intent_from_row(row: sqlite3.Row) -> ExitIntent:
    return ExitIntent(
        exit_intent_id=row["exit_intent_id"],
        paper_position_id=row["paper_position_id"],
        paper_order_id=row["paper_order_id"],
        signal_key=row["signal_key"],
        mint=row["mint"],
        track_id=row["track_id"],
        exit_variant=row["exit_variant"],
        reason=ExitReason(row["reason"]),
        requested_at=_dt_parse(row["requested_at"]),
        trigger_observed_at=_dt_parse_optional(row["trigger_observed_at"]),
        trigger_ingest_seq=_int_optional(row["trigger_ingest_seq"]),
        trigger_source_event_key=row["trigger_source_event_key"],
        trigger_price_numerator_raw=_int_optional(row["trigger_price_numerator_raw"]),
        trigger_price_denominator_raw=_int_optional(row["trigger_price_denominator_raw"]),
        rule_return_bps=_int_optional(row["rule_return_bps"]),
        trail_peak_return_bps=_int_optional(row["trail_peak_return_bps"]),
        last_fresh_observed_at=_dt_parse_optional(row["last_fresh_observed_at"]),
        last_fresh_ingest_seq=_int_optional(row["last_fresh_ingest_seq"]),
        last_fresh_return_bps=_int_optional(row["last_fresh_return_bps"]),
    )


def _return_bps(ref_num: int, ref_den: int, px_num: int, px_den: int) -> int:
    """Exact Phase-3 OutcomeReplay return-bps semantics."""
    ref = Fraction(ref_num, ref_den)
    px = Fraction(px_num, px_den)
    delta = (px / ref - 1) * 10_000
    return _round_fraction_half_away_from_zero(delta)


def _round_fraction_half_away_from_zero(value: Fraction) -> int:
    sign = -1 if value < 0 else 1
    x = abs(value)
    q, r = divmod(x.numerator, x.denominator)
    if r * 2 >= x.denominator:
        q += 1
    return sign * q


def _observation_fingerprint(obs: ExitMarketObservation) -> str:
    payload = {
        "mint": obs.mint,
        "observed_at": _dt_text(obs.observed_at),
        "ingest_seq": obs.ingest_seq,
        "price_identity": obs.price_identity,
        "price_numerator_raw": obs.price_numerator_raw,
        "price_denominator_raw": obs.price_denominator_raw,
        "is_gap_recovery": obs.is_gap_recovery,
        "source_event_key": obs.source_event_key,
    }
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _hash_id(prefix: str, *parts: object) -> str:
    body = "\x1f".join(str(x) for x in parts)
    return f"{prefix}-{hashlib.sha256(body.encode('utf-8')).hexdigest()[:32]}"


def _obs_key(at: datetime, seq: int) -> tuple[datetime, int]:
    return (_utc(at), int(seq))


def _require_utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ValueError("datetime must be UTC")


def _utc(value: datetime) -> datetime:
    _require_utc(value)
    return value.astimezone(timezone.utc)


def _dt_text(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


def _dt_parse(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return _utc(parsed)


def _dt_parse_optional(value: str | None) -> datetime | None:
    return None if value is None else _dt_parse(value)


def _int_optional(value: object | None) -> int | None:
    return None if value is None else int(value)
