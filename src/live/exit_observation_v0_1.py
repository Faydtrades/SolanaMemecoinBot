"""B2 fixed LIVE source/order knowledge cuts, without inventory or execution.

The producer supplies original ordered outputs. Evidence supplies canonical
transaction order. Runtime records their exact bounded cut in Ledger's common
journal, then commits the selected evaluator result. A timer needs no source
connection. Reopening replays retained bytes at their original evidence times.
"""
from __future__ import annotations

import json
import re
from contextlib import closing
from dataclasses import asdict, dataclass

from phase4.paper_exit_orchestrator_v0_1 import LOCKED_EXIT_SPECS, ExitMode, _return_bps
from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from .candidate_handoff_v0_1 import CandidateHandoffV01
from .continuous_producer_v0_1 import TABLES, accepted, MODEL_ID as PRODUCER_ID, MODEL_FINGERPRINT as PRODUCER_FINGERPRINT
from .ledger_actions_v0_1 import utc_microseconds, public_reference
from .ledger_chain_codec_v0_1 import chain_observation_from_json, chain_observation_to_json
from .ledger_domain_v0_1 import LedgerContractError, ledger_utc, digest_value
from .ledger_finality_v0_1 import _exact_metadata_compatible
from .ledger_ports_v0_1 import ConsumerCut, require_common_cut
from .public_rpc_v0_1 import primary_signature, u64
from .transaction_evidence_v0_1 import TransactionObservation, runtime_transaction_order, _known_finalized_anchors_consistent

VERSION = "live_exit_observation_v0.1"
EVIDENCE = "RUNTIME_EXIT_EVIDENCE"
EVALUATION = "RUNTIME_EXIT_EVALUATION"
KINDS = frozenset((EVIDENCE, EVALUATION))
MAX_SOURCE_ROWS = 256
MAX_RECORD_BYTES = 8 * 1024 * 1024


def require(condition, reason):
    if not condition:
        raise LedgerContractError("RUNTIME_EXIT_" + reason)


@dataclass(frozen=True, slots=True)
class ExitJournalRecord:
    sequence: int
    kind: str
    command_id: str
    binding_id: str
    payload_json: str

    def __post_init__(self):
        require(type(self.sequence) is int and self.sequence > 0 and self.kind in KINDS, "RECORD_KIND")
        public_reference(self.command_id)
        digest_value(self.binding_id)
        require(type(self.payload_json) is str and len(self.payload_json.encode()) <= MAX_RECORD_BYTES, "RECORD_BOUND")
        require(canonical_json(json.loads(self.payload_json)) == self.payload_json, "CANONICAL_RECORD")

    @property
    def payload(self):
        # Fresh decoded copy; callers cannot mutate durable controller state.
        return json.loads(self.payload_json)

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


def _binding(ledger, binding):
    from .position_controller_v0_1 import bind_actual_position
    return bind_actual_position(ledger, position_id=binding.position_id, root_id=binding.root_id,
        mint=binding.mint, selected_track=binding.spec.track_id, policy_digest=binding.policy_digest,
        acquisition_application_key=binding.acquisition_application_key, previous=binding)


def _base(snapshot, at):
    return {"version": VERSION, "binding": canonical_json(asdict(snapshot.binding)),
        "cut": asdict(snapshot.cut), "recorded_at_utc": ledger_utc(at)}


def _history(records):
    sources, observations, proofs, latest = [], {}, {}, None
    for record in records:
        payload = record.payload
        if record.kind == EVIDENCE:
            source = payload["source"]
            if source is not None:
                sources.append(source)
                for observation in source["observations"]:
                    key = observation["event_sequence"]
                    require(key not in observations, "DUPLICATE_SOURCE_OBSERVATION")
                    observations[key] = observation
            for proof in payload["proofs"]:
                proofs[proof["event_sequence"]] = (record.sequence, proof)
        else:
            latest = record
    return sources, observations, proofs, latest


def _source_capture(handoff, binding, previous):
    require(type(handoff) is CandidateHandoffV01 and handoff.ledger.domain.economic_domain_id == binding.economic_domain_id,
        "ORIGINAL_HANDOFF_REQUIRED")
    p = handoff.producer
    with p._transaction():
        handoff._validate_binding()
        require(binding.candidate.producer_lineage_id == p.source_identity, "PRODUCER_LINEAGE")
        original = p.conn.execute(f"SELECT * FROM {TABLES[5]} WHERE event_key=?", (binding.candidate.signal_key,)).fetchone()
        require(original is not None and handoff._candidate(dict(original)) == binding.candidate, "ORIGINAL_CANDIDATE")
        launch = p._launch(binding.mint)
        require(launch is not None, "ORIGINAL_LAUNCH")
        after = original["event_sequence"] if previous is None else previous["through_sequence"]
        producer_through = p.next_event_sequence - 1
        rows = p.producer_events(after_sequence=after, limit=min(MAX_SOURCE_ROWS, p.profile.delivery_page_rows))
        through = after if not rows else rows[-1]["event_sequence"]
        observations = []
        with closing(p.market_source.open_readonly(p.market_source.db_path)) as raw:
            raw.execute("BEGIN")
            for output in rows:
                if output["event_type"] != "MarketObservationEvent":
                    continue
                event = json.loads(output["event_json"])
                if event["mint"] != binding.mint:
                    continue
                retained = p.conn.execute(f"SELECT * FROM {TABLES[1]} WHERE input_key=?", (output["input_key"],)).fetchone()
                require(retained is not None and retained["input_kind"] == "MARKET" and retained["complete"] == 1,
                    "ORIGINAL_NORMALIZED_INPUT")
                rowid = retained["production_p1_rowid"]
                # Bound public raw bytes before loading them; use only this read
                # snapshot for signature/slot and accepted re-normalization.
                columns = [r[1] for r in raw.execute("PRAGMA table_info(pump_events)")]
                size_sql = "+".join('coalesce(length(CAST("'+c+'" AS BLOB)),0)' for c in columns)
                size = raw.execute(f"SELECT {size_sql} FROM pump_events WHERE rowid=?", (rowid,)).fetchone()
                require(size is not None and size[0] <= p.profile.record_bytes, "RAW_ROW_MISSING_OR_OVERSIZED")
                row = raw.execute("SELECT rowid AS p1_rowid,* FROM pump_events WHERE rowid=?", (rowid,)).fetchone()
                batch = p.market_source._normalize_rows([row], requested_after=rowid-1, batch_limit=1,
                    launch_by_mint={binding.mint: launch})
                require(len(batch.records) == 1 and batch.records[0].content_fingerprint == retained["content_fingerprint"],
                    "RAW_NORMALIZED_ORIGINAL_FINGERPRINT")
                normalized = batch.records[0]
                require((event["ingest_seq"], event["price_numerator_raw"], event["price_denominator_raw"],
                    event["observed_at"], event["price_identity"], event["is_gap_recovery"]) ==
                    (normalized.ingest_seq, normalized.price_numerator_raw, normalized.price_denominator_raw,
                    accepted._dt_text(normalized.observed_at), normalized.price_identity, normalized.is_gap_recovery),
                    "ORIGINAL_MARKET_OUTPUT")
                signature, slot = row["signature"], row["slot"]
                try:
                    primary_signature(signature)
                    u64(slot)
                    require(re.fullmatch(re.escape(signature)+r":[0-9]+:[0-9a-f]{16}", normalized.event_key) is not None,
                        "ORIGINAL_EVENT_SIGNATURE_UNPROVEN")
                except ValueError:
                    signature, slot = None, None
                observations.append({"event_sequence": output["event_sequence"], "output": output,
                    "input": dict(retained), "normalized": json.loads(accepted._json(normalized)),
                    "raw_signature": signature, "raw_slot": slot})
        manifest = p.conn.execute("SELECT manifest_digest,generation FROM live_producer_checkpoint_v0_1 WHERE singleton=1").fetchone()
        return {"source_identity": p.market_source.source_identity, "lineage": p.source_identity,
            "anchor": p.market_source.start_after_p1_rowid, "launch": launch,
            "generation": manifest["generation"], "manifest_digest": manifest["manifest_digest"],
            "after_sequence": after, "through_sequence": through, "watermark": p.durable_p1_rowid,
            "candidate_sequence": original["event_sequence"], "producer_through_sequence": producer_through,
            "complete": through == producer_through,
            "candidate_output_digest": original["content_fingerprint"], "candidate_output": dict(original),
            "output_page": list(rows), "observations": observations}


def _order_proof(domain, acquisition, observation, pair, at):
    require(type(pair) is tuple and len(pair) == 2 and all(type(x) is TransactionObservation for x in pair),
        "CONCRETE_TRANSACTION_EVIDENCE")
    left, right = pair
    original = acquisition.observation
    require(type(original) is TransactionObservation and original.transaction is not None, "ORIGINAL_ACQUISITION_EVIDENCE")
    require(left.request.signature == original.request.signature and left.transaction is not None, "ACQUISITION_PROOF_REBINDING")
    require(right.request.signature == observation["raw_signature"] and right.transaction is not None
        and right.transaction.slot == observation["raw_slot"], "RAW_SIGNATURE_SLOT_PROOF_BINDING")
    retained_reasons = []
    if (not _exact_metadata_compatible(original.transaction, left.transaction)
            or left.membership_block is not None and left.membership_block != original.membership_block):
        retained_reasons.append("ORIGINAL_ACQUISITION_PROOF_CONTRADICTION")
    for old in (original.root, original.membership_block):
        for new in (left.root, left.membership_block, right.root, right.membership_block):
            if old is not None and new is not None:
                if not _known_finalized_anchors_consistent(old, new):
                    retained_reasons.append("ORIGINAL_FINALIZED_ANCHOR_CONTRADICTION")
    order = runtime_transaction_order(left, right, expected_genesis=domain.genesis_hash,
        expected_profile_fingerprint=domain.expected_profile_fingerprint, now_utc=at)
    reasons = list(order.reasons) + retained_reasons
    if not right.transaction.outcome.succeeded:
        reasons.append("MARKET_TRANSACTION_NOT_SUCCESSFUL")
    relation = order.relation if not reasons else "UNKNOWN"
    return {"event_sequence": observation["event_sequence"], "left": chain_observation_to_json(left),
        "right": chain_observation_to_json(right), "evaluated_at_utc": at,
        "relation": relation, "reasons": sorted(set(reasons))}


def _consistent_retained_proofs(previous, proof):
    reasons = []
    new = [chain_observation_from_json(proof[k]) for k in ("left", "right")]
    for old_proof in previous:
        if any("CONTRADICTION" in reason for reason in old_proof["reasons"]):
            reasons.append("RETAINED_ORDER_CONTRADICTION")
        if old_proof["relation"] == "UNKNOWN":
            continue
        for old in (chain_observation_from_json(old_proof[k]) for k in ("left", "right")):
            for current in new:
                for a in (old.root, old.membership_block):
                    for b in (current.root, current.membership_block):
                        if a is not None and b is not None:
                            if not _known_finalized_anchors_consistent(a, b):
                                reasons.append("RETAINED_FINALIZED_ANCHOR_CONTRADICTION")
                if (old.membership_block is not None and current.membership_block is not None
                        and old.membership_block.slot == current.membership_block.slot):
                    if old.membership_block != current.membership_block:
                        reasons.append("RETAINED_CANONICAL_MEMBERSHIP_CONTRADICTION")
                if old.transaction is not None and current.transaction is not None and old.request.signature == current.request.signature:
                    if not _exact_metadata_compatible(old.transaction, current.transaction):
                        reasons.append("RETAINED_EXACT_TRANSACTION_CONTRADICTION")
    if reasons:
        proof = dict(proof, relation="UNKNOWN", reasons=sorted(set(proof["reasons"]+reasons)))
    return proof


def capture_exit_evidence(ledger, binding, *, handoff, proofs, command_id, recorded_at_utc):
    """Commit a complete bounded source page plus currently available Evidence.

    proofs maps original producer event sequence to (BUY, market) Evidence.
    Missing proofs remain explicitly unproven. No source ACK or new cursor is
    supplied by callers, and a source gap/error never prevents timer dispatch.
    """
    prior = ledger.exit_record(command_id)
    if prior is not None:
        require(prior.kind == EVIDENCE and prior.binding_id == binding.binding_id
            and prior.payload["binding"] == canonical_json(asdict(binding))
            and prior.payload["recorded_at_utc"] == ledger_utc(recorded_at_utc), "COMMAND_CONFLICT")
        _same_evidence_request(prior, proofs)
        return prior
    snapshot = _binding(ledger, binding)
    require(handoff.ledger is ledger, "ORIGINAL_HANDOFF_LEDGER")
    records = ledger.exit_records(binding.binding_id)
    sources, _, _, _ = _history(records)
    source = _source_capture(handoff, binding, sources[-1] if sources else None)
    observations = {o["event_sequence"]: o for o in source["observations"]}
    return _commit_evidence(ledger, snapshot, source, observations, proofs, command_id, recorded_at_utc)


def enrich_exit_evidence(ledger, binding, *, proofs, command_id, recorded_at_utc):
    """Retain late public proof only for already committed original source rows.

    A later evaluation cannot consume a previously evaluated source row again.
    """
    prior = ledger.exit_record(command_id)
    if prior is not None:
        require(prior.kind == EVIDENCE and prior.binding_id == binding.binding_id
            and prior.payload["binding"] == canonical_json(asdict(binding))
            and prior.payload["recorded_at_utc"] == ledger_utc(recorded_at_utc), "COMMAND_CONFLICT")
        _same_evidence_request(prior, proofs)
        return prior
    snapshot = _binding(ledger, binding)
    _, observations, _, _ = _history(ledger.exit_records(binding.binding_id))
    return _commit_evidence(ledger, snapshot, None, observations, proofs, command_id, recorded_at_utc)


def _same_evidence_request(prior, proofs):
    require(type(proofs) is dict and len(proofs) <= MAX_SOURCE_ROWS, "PROOF_SOURCE_SET")
    retained = {p["event_sequence"]: (p["left"], p["right"]) for p in prior.payload["proofs"]}
    require(set(proofs) == set(retained), "COMMAND_CONFLICT")
    for seq, pair in proofs.items():
        require(type(pair) is tuple and len(pair) == 2
            and tuple(chain_observation_to_json(p) for p in pair) == retained[seq], "COMMAND_CONFLICT")


def _commit_evidence(ledger, snapshot, source, observations, proofs, command_id, at):
    binding = snapshot.binding
    require(type(proofs) is dict and set(proofs) <= observations.keys() and len(proofs) <= MAX_SOURCE_ROWS,
        "PROOF_SOURCE_SET")
    at = ledger_utc(at)
    chain = ledger.chain_receipt(binding.acquisition_chain_receipt_key)
    retained = [p for r in ledger.exit_records(binding.binding_id) if r.kind == EVIDENCE for p in r.payload["proofs"]]
    derived = []
    for seq in sorted(proofs):
        proof = _consistent_retained_proofs(retained, _order_proof(ledger.domain, chain, observations[seq], proofs[seq], at))
        derived.append(proof)
        retained.append(proof)
    payload = _base(snapshot, at)
    payload.update(source=source, proofs=derived)
    return ledger._record_exit(EVIDENCE, command_id, binding.binding_id, canonical_json(payload), fence=ledger.write_fence())


def _evaluate(binding, records, at, evidence_sequence):
    sources, observations, proofs, previous = _history(records)
    prior = None if previous is None else previous.payload
    order_conflict = any("CONTRADICTION" in reason for r in records if r.kind == EVIDENCE
        for proof in r.payload["proofs"] for reason in proof["reasons"])
    require(all(at >= utc_microseconds(r.payload["recorded_at_utc"]) for r in records), "KNOWLEDGE_CLOCK_REGRESSION")
    state = {"state": "MONITORING", "trigger_kind": "NONE", "trigger_reason": None,
        "trigger_at_us": None, "trigger_observation_digest": None, "trail_activated": False,
        "peak_return_bps": None, "through_event_sequence": -1, "last_observation_key": None}
    if prior is not None:
        require(at >= prior["timer_fence_us"], "TIMER_FENCE_REGRESSION")
        state.update(prior["state"])
    decisions = []
    for seq, obs in sorted(observations.items()):
        if seq <= state["through_event_sequence"]:
            continue
        normal = obs["normalized"]
        observed = utc_microseconds(normal["observed_at"])
        if observed > at:
            break  # Retain future observations for a later captured timer.
        state["through_event_sequence"] = seq
        proof_sequence, proof = proofs.get(seq, (None, None))
        reason = None
        key = [observed, normal["ingest_seq"]]
        if state["state"] == "DUE":
            reason = "TRIGGER_ALREADY_COMMITTED"
        elif sources and not sources[-1]["complete"]:
            reason = "SOURCE_CAPTURE_INCOMPLETE"
        elif order_conflict:
            reason = "RETAINED_ORDER_CONTRADICTION"
        elif observed > binding.fallback_deadline_us:
            reason = "FALLBACK_PRECEDES_LATE_OBSERVATION"
        elif proof is None or proof["relation"] != "BEFORE":
            reason = "OBSERVATION_ORDER_UNPROVEN" if proof is None or proof["relation"] == "UNKNOWN" else "AT_OR_BEFORE_ACQUISITION"
        elif observed < binding.candidate.generated_at_us:
            reason = "BEFORE_ORIGINAL_SIGNAL"
        elif state["last_observation_key"] is not None and key <= state["last_observation_key"]:
            reason = "OUT_OF_ORDER_REPLAY"
        else:
            # Once canonical acquisition order is known, preserve the accepted
            # evaluator's last-seen gate even for a gap or wrong price identity.
            state["last_observation_key"] = key
            if normal["is_gap_recovery"]:
                reason = "GAP_RECOVERY_NOT_FRESH"
            elif normal["price_identity"] != binding.candidate.price_identity:
                reason = "PRICE_IDENTITY_MISMATCH"
        decision = {"event_sequence": seq, "observation_digest": content_fingerprint(obs),
            "proof_sequence": proof_sequence, "proof_digest": None if proof is None else content_fingerprint(proof),
            "eligible": reason is None, "reason": reason}
        decisions.append(decision)
        if reason is not None:
            continue
        state["last_observation_key"] = key
        ret = _return_bps(binding.candidate.rule_reference_price_numerator_raw,
            binding.candidate.rule_reference_price_denominator_raw,
            normal["price_numerator_raw"], normal["price_denominator_raw"])
        spec, trigger = binding.spec, None
        if spec.mode is ExitMode.FIXED_TP:
            if ret >= spec.tp_bps:
                trigger = "TAKE_PROFIT"
        elif not state["trail_activated"]:
            if ret >= spec.trail_activation_bps:
                state.update(trail_activated=True, peak_return_bps=ret)
        else:
            state["peak_return_bps"] = max(state["peak_return_bps"], ret)
            if state["peak_return_bps"] - ret >= spec.trail_giveback_bps:
                trigger = "TRAIL"
        if trigger is not None:
            state.update(state="DUE", trigger_kind="MARKET", trigger_reason=trigger,
                trigger_at_us=observed, trigger_observation_digest=content_fingerprint(obs))
    if state["state"] != "DUE" and at >= binding.fallback_fire_boundary_us:
        state.update(state="DUE", trigger_kind="FALLBACK", trigger_reason="FALLBACK",
            trigger_at_us=binding.fallback_fire_boundary_us, trigger_observation_digest=None)
    return {"previous_evaluation_digest": None if previous is None else previous.content_digest,
        "source_cut": {"kind": "ORIGINAL_CANDIDATE", "lineage": binding.candidate.producer_lineage_id,
            "watermark": binding.candidate.source_cursor, "source_record_digest": binding.candidate.source_record_digest,
            "complete": False} if not sources else {k: sources[-1][k] for k in
            ("source_identity", "lineage", "generation", "manifest_digest", "through_sequence", "watermark", "complete")},
        "highest_evidence_sequence": evidence_sequence,
        "timer_fence_us": at, "decisions": decisions, "state": state}


def commit_exit_evaluation(ledger, binding, *, command_id, timer_fence_utc):
    """Drain only durable knowledge, then fire the original inclusive timer.

    command_id identifies the original dispatch; exact retry returns that fixed
    record even after more source/Evidence arrives. No caller-selected cut,
    observation subset, modeled position, source connection or quantity exists.
    """
    prior = ledger.exit_record(command_id)
    if prior is not None:
        require(prior.kind == EVALUATION and prior.binding_id == binding.binding_id
            and prior.payload["binding"] == canonical_json(asdict(binding))
            and prior.payload["timer_fence_us"] == utc_microseconds(timer_fence_utc), "COMMAND_CONFLICT")
        return prior
    snapshot = _binding(ledger, binding)
    payload = _base(snapshot, timer_fence_utc)
    payload.update(_evaluate(binding, ledger.exit_records(binding.binding_id), utc_microseconds(timer_fence_utc),
        ledger.exit_evidence_sequence()))
    return ledger._record_exit(EVALUATION, command_id, binding.binding_id, canonical_json(payload), fence=ledger.write_fence())


def _historical_binding(domain, payload, custody, candidates, actions, applications, chains):
    from .position_controller_v0_1 import SelectedExitBinding
    stored = json.loads(payload["binding"])
    candidate = candidates.get(stored["root_id"])
    action = actions.get(stored["buy_action_id"])
    application = applications.get(stored["acquisition_application_key"])
    chain = chains.get(stored["acquisition_chain_receipt_key"])
    position = next((p for p in custody.positions if p.position_id == stored["position_id"]), None)
    require(candidate is not None and action is not None and application is not None and chain is not None and position is not None,
        "ACTUAL_HISTORICAL_ACQUISITION")
    proposal = application.decision.proposal
    require(not custody.quarantine_reasons and action.side == "BUY" and application.decision.disposition == "FINALIZED_SUCCESS_APPLIED"
        and proposal is not None and proposal.action_id == action.action_id and proposal.base_units_delta == position.acquired_units
        and proposal.signature == position.acquisition_signature and proposal.base_units_delta > 0
        and chain.content_digest == application.chain_receipt_digest
        and chain.decision.resulting_state.positive_finality == "FINALIZED_SUCCESS"
        and action.candidate_digest == candidate.content_digest and action.root_id == position.root_id
        and action.position_id == position.position_id and candidate.mint == position.mint,
        "ACTUAL_HISTORICAL_BINDING")
    spec = LOCKED_EXIT_SPECS[action.selected_exit_track]
    deadline = candidate.generated_at_us + spec.fallback_ms * 1000
    binding = SelectedExitBinding(domain.economic_domain_id, position.position_id, position.root_id, position.mint,
        candidate, action.action_id, action.content_digest, action.policy_ref, action.policy_digest, spec,
        application.ingestion_key, application.content_digest, proposal.content_digest, proposal.signature,
        application.chain_receipt_key, application.chain_receipt_digest, application.recorded_at_utc, deadline, deadline+1)
    require(canonical_json(asdict(binding)) == payload["binding"], "ORIGINAL_BINDING_REPLAY")
    require(payload["recorded_at_utc"] >= application.recorded_at_utc, "BEFORE_ACQUISITION_KNOWLEDGE")
    return binding, chain


def replay_exit_record(record, previous, *, domain, custody, candidates, actions, applications, chains, previous_commit_digest,
                       evidence_sequence):
    """Closed Ledger replay hook; historical custody and common revision only."""
    payload = record.payload
    common = {"version", "binding", "cut", "recorded_at_utc"}
    expected = common | ({"source", "proofs"} if record.kind == EVIDENCE else {
        "previous_evaluation_digest", "source_cut", "highest_evidence_sequence", "timer_fence_us", "decisions", "state"})
    require(set(payload) == expected and payload["version"] == VERSION
        and ledger_utc(payload["recorded_at_utc"]) == payload["recorded_at_utc"], "RECORD_SHAPE")
    require(all(payload["recorded_at_utc"] >= c.decision.evaluated_at_utc for c in chains.values()),
        "CHAIN_KNOWLEDGE_CLOCK_REGRESSION")
    require_common_cut(domain, custody, ConsumerCut(**payload["cut"]), record.sequence-1, previous_commit_digest)
    binding, chain = _historical_binding(domain, payload, custody, candidates, actions, applications, chains)
    require(record.binding_id == binding.binding_id, "RECORD_BINDING")
    related = [r for r in previous if r.binding_id == record.binding_id]
    require(all(payload["recorded_at_utc"] >= r.payload["recorded_at_utc"] for r in related), "KNOWLEDGE_CLOCK_REGRESSION")
    sources, observations, _, _ = _history(related)
    if record.kind == EVALUATION:
        expected = _evaluate(binding, related, utc_microseconds(payload["recorded_at_utc"]), evidence_sequence)
        require(all(payload[k] == v for k, v in expected.items()), "FIXED_EVALUATION_REPLAY")
        return
    source = payload["source"]
    if source is not None:
        require(source["lineage"] == binding.candidate.producer_lineage_id
            and source["lineage"] == accepted._fingerprint({"model_id": PRODUCER_ID, "fingerprint": PRODUCER_FINGERPRINT,
                "source_identity": source["source_identity"], "anchor": source["anchor"]})
            and source["anchor"] == binding.candidate.source_binding.anchors[0].rowid
            and source["after_sequence"] == (source["candidate_sequence"] if not sources else sources[-1]["through_sequence"])
            and source["through_sequence"] >= source["after_sequence"]
            and source["complete"] == (source["through_sequence"] == source["producer_through_sequence"])
            and source["watermark"] >= binding.candidate.source_cursor
            and len(source["observations"]) <= MAX_SOURCE_ROWS, "SOURCE_CUT_LINEAGE_OR_ORDER")
        original = source["candidate_output"]
        candidate_event = json.loads(original["event_json"])
        require(original["event_sequence"] == source["candidate_sequence"] and original["event_type"] == "CandidateEvaluationEvent"
            and original["event_key"] == binding.candidate.signal_key
            and original["content_fingerprint"] == source["candidate_output_digest"]
            and accepted._fingerprint({"cursor": original["event_sequence"], "event_key": original["event_key"],
                "event_type": original["event_type"], "event": candidate_event}) == source["candidate_output_digest"]
            and candidate_event["candidate"]["candidate_id"] == binding.candidate.candidate_signal_id
            and candidate_event["candidate"]["signal_ingest_seq"] == binding.candidate.source_cursor,
            "ORIGINAL_CANDIDATE_BOUNDARY")
        page = source["output_page"]
        require(len(page) <= MAX_SOURCE_ROWS and [r["event_sequence"] for r in page]
            == list(range(source["after_sequence"]+1, source["through_sequence"]+1)), "COMPLETE_OUTPUT_PAGE")
        market_sequences = []
        for output in page:
            event = json.loads(output["event_json"])
            require(accepted._fingerprint({"cursor": output["event_sequence"], "event_key": output["event_key"],
                "event_type": output["event_type"], "event": event}) == output["content_fingerprint"], "ORIGINAL_PAGE_DIGEST")
            if output["event_type"] == "MarketObservationEvent" and event["mint"] == binding.mint:
                market_sequences.append(output["event_sequence"])
        require(market_sequences == [o["event_sequence"] for o in source["observations"]], "COMPLETE_POSITION_OBSERVATIONS")
        if sources:
            require(all(source[k] == sources[-1][k] for k in ("source_identity", "lineage", "anchor", "launch", "candidate_output_digest"))
                and source["generation"] >= sources[-1]["generation"] and source["watermark"] >= sources[-1]["watermark"],
                "SOURCE_CUT_REBINDING")
        last = source["after_sequence"]
        for observation in source["observations"]:
            seq, output, original, normal = (observation[k] for k in ("event_sequence", "output", "input", "normalized"))
            require(last < seq <= source["through_sequence"] and seq not in observations, "SOURCE_OUTPUT_DRAIN")
            require(output == page[seq-source["after_sequence"]-1], "SOURCE_PAGE_OBSERVATION_BINDING")
            last = seq
            event = json.loads(output["event_json"])
            require(output["event_sequence"] == seq and output["event_type"] == "MarketObservationEvent"
                and output["input_key"] == original["input_key"] and original["input_kind"] == "MARKET" and original["complete"] == 1
                and accepted._fingerprint({"cursor": seq, "event_key": output["event_key"], "event_type": output["event_type"], "event": event}) == output["content_fingerprint"]
                and accepted._fingerprint(normal) == original["content_fingerprint"]
                and normal["source_identity"] == source["source_identity"] and normal["start_after_p1_rowid"] == source["anchor"]
                and normal["session_launch_p1_rowid"] == source["launch"] and normal["mint"] == binding.mint
                and normal["production_p1_rowid"] == original["production_p1_rowid"] <= source["watermark"]
                and event["mint"] == normal["mint"] and output["event_key"] == normal["event_key"]+":MARKET"
                and all(event[k] == normal[k] for k in ("observed_at", "ingest_seq", "price_identity", "price_numerator_raw", "price_denominator_raw", "is_gap_recovery")),
                "ORIGINAL_SOURCE_REPLAY")
            if observation["raw_signature"] is not None:
                primary_signature(observation["raw_signature"])
                u64(observation["raw_slot"])
                require(re.fullmatch(re.escape(observation["raw_signature"])+r":[0-9]+:[0-9a-f]{16}", normal["event_key"]) is not None,
                    "ORIGINAL_EVENT_SIGNATURE_UNPROVEN")
            else:
                require(observation["raw_slot"] is None, "UNPROVEN_RAW_SLOT")
            observations[seq] = observation
    require(len(payload["proofs"]) <= MAX_SOURCE_ROWS, "PROOF_COUNT")
    seen = set()
    retained_proofs = [p for r in related if r.kind == EVIDENCE for p in r.payload["proofs"]]
    for proof in payload["proofs"]:
        seq = proof["event_sequence"]
        require(seq in observations and seq not in seen, "ORIGINAL_PROOF_SOURCE_REQUIRED")
        seen.add(seq)
        expected = _order_proof(domain, chain, observations[seq],
            (chain_observation_from_json(proof["left"]), chain_observation_from_json(proof["right"])), payload["recorded_at_utc"])
        expected = _consistent_retained_proofs(retained_proofs, expected)
        require(proof == expected, "ORIGINAL_ORDER_PROOF_REPLAY")
        retained_proofs.append(proof)
