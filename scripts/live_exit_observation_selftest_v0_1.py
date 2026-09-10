"""B2 original producer + actual finalized BUY + inert public order fixtures."""
from __future__ import annotations

import copy
import json
import sqlite3
import sys
import tempfile
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "scripts")]
from phase5.shadow_domain_v0_1 import content_fingerprint
import live.exit_observation_v0_1 as boundary
from live.exit_observation_v0_1 import capture_exit_evidence, enrich_exit_evidence, commit_exit_evaluation
from live.ledger_actions_v0_1 import PendingAction, position_identity
from live.ledger_domain_v0_1 import LedgerDomain
from live.ledger_repository_v0_1 import LedgerRepository
from live.position_controller_v0_1 import bind_actual_position
from live.candidate_handoff_v0_1 import CandidateHandoffV01
from live.continuous_producer_v0_1 import connect_producer_db, TABLES
from live.continuous_producer_v0_2 import LiveContinuousProducerV02
from phase4.paper_continuous_market_source_v0_2 import ContinuousMarketSourceV02
from live.transaction_evidence_v0_1 import decode_transaction_result, decode_signature_block
from live.wallet_evidence_v0_1 import WalletEvidenceRequest
from live_ledger_baseline_selftest_v0_1 import observe, ingest
from live_wallet_evidence_selftest_v0_1 import NOW, GENESIS, PROFILE, utc
from live_transaction_evidence_selftest_v0_1 import sig, transaction
import live_candidate_handoff_selftest_v0_1 as hf
import live_ledger_ports_selftest_v0_1 as pf
import live_ledger_custody_selftest_v0_1 as cf
import live_ledger_settlement_selftest_v0_1 as sf

CHECKS = []


def check(name, value):
    if not value:
        raise AssertionError(name)
    CHECKS.append(name)
    print("OK", name)


def reject(name, call):
    try:
        call()
    except (ValueError, RuntimeError, sqlite3.DatabaseError):
        check(name, True)
    else:
        check(name, False)


class Fixture:
    def __init__(self, directory, name, track="FINAL-A", *, same_slot=False, prior_rows=0):
        self.raw, self.ppath, self.path = (directory / (name+s) for s in ("-raw.sqlite", "-producer.sqlite", "-ledger.sqlite"))
        self.source_binding = hf.build_raw(self.raw)
        self.market = sf.Fixture()
        self.market.mint = sf.MINT
        if same_slot:
            self.market.scenario.blocks[108]["signatures"] = [sig(48), sig(49), sig(50)]
        self.domain = LedgerDomain(GENESIS, sf.WALLET, PROFILE.fingerprint, sf.FUNDING, minimum_context_slot=100)
        self.repo = LedgerRepository.initialize(self.path, self.domain)
        ingest(self.repo, observe(self.market.wallet_scenario(), request=WalletEvidenceRequest(sf.WALLET, GENESIS, 100)))
        with closing(sqlite3.connect(self.raw)) as raw, raw:
            for rowid in range(2, prior_rows+2):
                hf.raw_fixture.insert_row(raw,rowid,hf.MINT_B,"LAUNCH" if rowid == 2 else "BUY",
                    observed_offset=rowid-prior_rows)
            for rowid, mint, kind, price in hf.FIRST:
                if mint == hf.MINT_A:
                    hf.raw_fixture.insert_row(raw, rowid+prior_rows, sf.MINT, kind, price_units=price,observed_offset=rowid)
        self.open_producer(initialize=True)
        self.produce()
        roots = []
        while True:
            page = self.handoff.deliver_page()
            roots.extend(page.candidate_roots)
            if not page.scanned_rows:
                break
        if len(roots) != 1:
            self.close()
            raise AssertionError("one original candidate required")
        candidate = self.repo.candidate(roots[0])
        self.action = PendingAction(roots[0], candidate.content_digest, "BUY", sf.MINT, self.market.token_program,
            position_identity(self.domain, roots[0], sf.MINT), self.market.units, "SYNTHETIC_ENGINEERING_TERMS", sf.digest("terms"),
            "SYNTHETIC_ENGINEERING_SELECTION_"+track, sf.digest(track), track, None, 1, (NOW+300)*1_000_000, sf.digest("deadline"))
        self.repo.admit(pf.admission(self.repo, self.action), ingestion_key="admit", fence=self.repo.write_fence())
        self.prep, self.chain, self.support = pf.finalize(self.repo, self.market, self.action)
        cf.apply(self.repo, self.prep, self.support, key="acquire", at=NOW+14)
        self.binding = bind_actual_position(self.repo, position_id=self.action.position_id, root_id=self.action.root_id,
            mint=sf.MINT, selected_track=track, policy_digest=self.action.policy_digest, acquisition_application_key="acquire").binding
        check(name+"_actual_buy_not_fabricated", self.repo.position_history(self.action.position_id).remaining_units == self.market.actual_base)

    def open_producer(self, *, initialize=False):
        self.conn = connect_producer_db(self.ppath)
        self.producer = LiveContinuousProducerV02(self.conn, ContinuousMarketSourceV02(self.raw,
            start_after_p1_rowid=1, database_identity="B2:SYNTHETIC:COLLECTOR"), wall_clock=lambda: hf.BASE)
        self.handoff = CandidateHandoffV01(self.producer, self.repo, self.source_binding,
            database_identity="B2:SYNTHETIC:COLLECTOR", initialize=initialize)

    def produce(self):
        while self.producer.process_next_batch(batch_size=32).raw_rows_fetched:
            pass

    def append(self, rowid, *, n=50, slot=110, price=12000, offset=14, gap=False, key_mismatch=False):
        with closing(sqlite3.connect(self.raw)) as raw, raw:
            hf.raw_fixture.insert_row(raw, rowid, sf.MINT, "BUY", price_units=price,
                observed_offset=offset+14, source="GAP_RECONCILIATION_V0_3_4" if gap else "LIVE_WEBSOCKET_EVENT_V0_3_4")
            raw.execute("UPDATE pump_events SET signature=?,slot=?,event_key=? WHERE rowid=?",
                (sig(n), slot, (sig(n+1) if key_mismatch else sig(n))+":0:0123456789abcdef", rowid))
        self.produce()
        row = self.conn.execute(f"SELECT event_sequence FROM {TABLES[5]} WHERE event_key=?", ((sig(n+1) if key_mismatch else sig(n))+":0:0123456789abcdef:MARKET",)).fetchone()
        assert row is not None
        return row[0]

    def pair(self, n=50, slot=110, *, unknown=False, failed=False):
        left = self.chain.observation
        block = copy.deepcopy(self.market.scenario.blocks[slot])
        if slot != 108:
            block["signatures"] = [sig(i) for i in range(48,60)]
        tx = transaction(n, slot=slot, failed=failed)
        tx["blockTime"] = block["blockTime"]
        right = replace(left, request=replace(left.request, signature=sig(n)),
            status=replace(left.status, signature=sig(n), slot=slot, outcome=decode_transaction_result(sig(n), tx, PROFILE).outcome),
            transaction=decode_transaction_result(sig(n), tx, PROFILE),
            membership_block=None if unknown else decode_signature_block(slot, block, PROFILE))
        return left, right

    def capture(self, key="capture", proofs=None, at=NOW+14):
        return capture_exit_evidence(self.repo, self.binding, handoff=self.handoff, proofs=proofs or {},
            command_id=key, recorded_at_utc=utc(at))

    def evaluate(self, key="evaluate", at=NOW+14):
        return commit_exit_evaluation(self.repo, self.binding, command_id=key, timer_fence_utc=utc(at))

    def close(self):
        self.conn.close()
        self.repo.close()

    def reopen_ledger(self):
        self.repo.close()
        self.repo = LedgerRepository.reopen(self.path, self.domain)
        self.handoff.ledger = self.repo


def main():
    with tempfile.TemporaryDirectory(prefix="live-exit-b2-") as tmp:
        directory = Path(tmp)
        f = Fixture(directory, "later")
        try:
            seq = f.append(15)
            evidence = f.capture(proofs={seq: f.pair()})
            check("identical_evidence_command_replays",f.capture(proofs={seq:f.pair()}) == evidence)
            reject("same_evidence_command_cannot_hide_changed_proof",lambda:f.capture(proofs={seq:f.pair(unknown=True)}))
            check("strictly_later_canonical_slot", evidence.payload["proofs"][0]["relation"] == "BEFORE")
            initial_custody = f.repo.consumer_snapshot()["consumer_cut"].custody_digest
            result = f.evaluate()
            check("selected_tp_original_candidate_reference", result.payload["state"]["trigger_reason"] == "TAKE_PROFIT")
            check("fixed_source_and_evidence_cut", result.payload["source_cut"]["watermark"] == 15
                and result.payload["highest_evidence_sequence"] == evidence.sequence)
            check("no_quantity_or_usable_inventory_write", f.repo.consumer_snapshot()["consumer_cut"].custody_digest == initial_custody
                and f.repo.protection(f.action.position_id) is None)
            f.repo.audit()
            f.reopen_ledger()
            check("reopen_exact_trigger", f.repo.exit_record("evaluate") == result)
            check("same_command_exact_replay", f.evaluate() == result)
            reject("same_command_timer_rebinding", lambda: f.evaluate(at=NOW+15))
            modified = result.payload
            modified["state"]["trigger_kind"] = "NONE"
            check("returned_payload_cannot_mutate", f.repo.exit_record("evaluate") == result)
        finally:
            f.close()

        for label, n, slot, unknown, expected in (("before",48,104,False,"AFTER"),
                ("same_before",48,108,False,"AFTER"), ("same_after",50,108,False,"BEFORE"),
                ("same_unknown",50,108,True,"UNKNOWN"), ("same_transaction",49,108,False,"UNKNOWN")):
            f = Fixture(directory, label, same_slot=True)
            try:
                seq = f.append(15,n=n,slot=slot)
                pair = (f.chain.observation, f.chain.observation) if n == 49 else f.pair(n,slot,unknown=unknown)
                evidence = f.capture(proofs={seq:pair})
                check(label+"_canonical_relation", evidence.payload["proofs"][0]["relation"] == expected)
                result = f.evaluate(at=NOW+16)
                check(label+"_price_or_original_fallback", result.payload["state"]["trigger_kind"] == ("MARKET" if expected == "BEFORE" else "FALLBACK"))
                f.repo.audit()
            finally:
                f.close()

        f = Fixture(directory, "late")
        try:
            seq = f.append(15)
            f.capture()
            old = f.evaluate()
            check("unproven_price_excluded", old.payload["decisions"][-1]["reason"] == "OBSERVATION_ORDER_UNPROVEN")
            enrichment = enrich_exit_evidence(f.repo,f.binding,proofs={seq:f.pair()},command_id="late-proof",recorded_at_utc=utc(NOW+14.5))
            check("late_enrichment_committed_later", enrichment.sequence > old.sequence)
            check("late_enrichment_cannot_rewrite_dispatch", f.evaluate() == old)
            after = f.evaluate("after",NOW+16)
            check("late_enrichment_cannot_reconsume_old_observation", after.payload["state"]["trigger_kind"] == "FALLBACK"
                and not after.payload["decisions"])
            reject("evaluation_cannot_claim_pre_evidence_time",lambda:f.evaluate("backdated",NOW+14))
            reject("evidence_clock_cannot_regress",lambda:enrich_exit_evidence(f.repo,f.binding,proofs={seq:f.pair()},command_id="old-proof",recorded_at_utc=utc(NOW+14)))
            f.reopen_ledger()
            check("late_reopen_exact_history", f.repo.exit_record("evaluate") == old and f.repo.exit_record("after") == after)
        finally:
            f.close()

        for track in ("FINAL-A","FINAL-B","SENS-C"):
            f = Fixture(directory,"no-source-"+track,track)
            try:
                f.conn.close()
                due = f.evaluate(at=NOW+20)
                check(track+"_unavailable_source_original_due", due.payload["state"]["trigger_kind"] == "FALLBACK"
                    and due.payload["state"]["trigger_at_us"] == f.binding.fallback_fire_boundary_us
                    and due.payload["highest_evidence_sequence"] == f.chain.sequence
                    and due.payload["source_cut"]["kind"] == "ORIGINAL_CANDIDATE"
                    and due.payload["source_cut"]["watermark"] == f.binding.candidate.source_cursor)
                f.reopen_ledger()
                check(track+"_timer_reopen_without_producer",f.repo.exit_record("evaluate") == due)
            finally:
                f.close()

        f = Fixture(directory,"trail","FINAL-B")
        try:
            one=f.append(15,n=50,price=12120,offset=14)
            two=f.append(16,n=51,price=11700,offset=14.5)
            f.capture(proofs={one:f.pair(50),two:f.pair(51)})
            result=f.evaluate(at=NOW+14.75)
            check("ordered_trailing_peak_then_giveback",result.payload["state"]["trigger_reason"] == "TRAIL"
                and result.payload["state"]["peak_return_bps"] == 2000)
            f.reopen_ledger()
            check("trailing_reopen_exact", f.repo.exit_record("evaluate") == result)
        finally:
            f.close()

        f = Fixture(directory,"deadline")
        try:
            seq=f.append(15,offset=15)
            f.capture(proofs={seq:f.pair()})
            before=f.evaluate(at=NOW+14)
            check("future_observation_retained",before.payload["state"]["state"] == "MONITORING")
            exact=f.evaluate("exact",NOW+15)
            check("exact_deadline_market_eligible",exact.payload["state"]["trigger_kind"] == "MARKET")
        finally:
            f.close()

        f = Fixture(directory,"key-mismatch")
        try:
            seq=f.append(15,key_mismatch=True)
            evidence=f.capture()
            check("original_event_signature_mismatch_unproven",evidence.payload["source"]["observations"][-1]["raw_signature"] is None)
            reject("arbitrary_raw_signature_not_order_proof",lambda:enrich_exit_evidence(f.repo,f.binding,proofs={seq:f.pair()},command_id="bad-proof",recorded_at_utc=utc(NOW+14)))
            check("unproven_key_fallback",f.evaluate(at=NOW+16).payload["state"]["trigger_kind"] == "FALLBACK")
        finally:
            f.close()

        f = Fixture(directory,"prior-history",prior_rows=300)
        try:
            seq=f.append(315)
            evidence=f.capture(proofs={seq:f.pair()})
            check("large_unrelated_lifetime_history_skipped_at_original_candidate",evidence.payload["source"]["after_sequence"] > 256
                and len(evidence.payload["source"]["output_page"]) == 2 and evidence.payload["source"]["complete"])
            check("large_history_current_market_still_eligible",f.evaluate().payload["state"]["trigger_kind"] == "MARKET")
        finally:
            f.close()

        for evaluate_incomplete in (False,True):
            f = Fixture(directory,"pages-"+str(evaluate_incomplete),"FINAL-B")
            try:
                f.capture("initial")
                one=f.append(15,n=50,price=12120,offset=14)
                two=f.append(16,n=51,price=11700,offset=14.5)
                with patch.object(boundary,"MAX_SOURCE_ROWS",1):
                    first=f.capture("page1",proofs={one:f.pair(50)})
                    check("paged_cut_explicitly_incomplete_"+str(evaluate_incomplete),not first.payload["source"]["complete"])
                    if evaluate_incomplete:
                        early=f.evaluate("early",NOW+16)
                        check("incomplete_capture_excludes_price_but_keeps_due_timer",early.payload["state"]["trigger_kind"] == "FALLBACK"
                            and all(not d["eligible"] for d in early.payload["decisions"]))
                    second=f.capture("page2",proofs={two:f.pair(51)},at=NOW+16 if evaluate_incomplete else NOW+14)
                    check("bounded_pages_join_complete_original_order_"+str(evaluate_incomplete),second.payload["source"]["complete"]
                        and second.payload["source"]["after_sequence"] == first.payload["source"]["through_sequence"])
                final=f.evaluate("final",NOW+16 if evaluate_incomplete else NOW+14.75)
                check("paged_drain_preserves_trigger_"+str(evaluate_incomplete),final.payload["state"]["trigger_reason"] == ("FALLBACK" if evaluate_incomplete else "TRAIL"))
                f.reopen_ledger()
                check("paged_cut_exact_reopen_"+str(evaluate_incomplete),f.repo.exit_record("final") == final)
            finally:
                f.close()

        for conflict in ("membership","original-root","exact-transaction"):
            f=Fixture(directory,"conflict-"+conflict)
            try:
                seq=f.append(15)
                f.capture(proofs={seq:f.pair()})
                left,right=f.pair()
                if conflict == "membership":
                    right=replace(right,membership_block=replace(right.membership_block,signatures=(sig(50),)))
                elif conflict == "original-root":
                    # A coherent new pair still conflicts with the acquisition's
                    # ORIGINAL finalized root; compare all retained facts.
                    root=replace(left.root,blockhash=hf.MINT_C)
                    left,right=replace(left,root=root),replace(right,root=root)
                else:
                    left=replace(left,transaction=replace(left.transaction,fee_lamports=left.transaction.fee_lamports+1))
                bad=enrich_exit_evidence(f.repo,f.binding,proofs={seq:(left,right)},command_id="contradiction",recorded_at_utc=utc(NOW+14))
                check(conflict+"_contradiction_durable_unknown",bad.payload["proofs"][0]["relation"] == "UNKNOWN"
                    and any("CONTRADICTION" in r for r in bad.payload["proofs"][0]["reasons"]))
                retry=enrich_exit_evidence(f.repo,f.binding,proofs={seq:f.pair()},command_id="cannot-clear",recorded_at_utc=utc(NOW+14))
                check(conflict+"_later_positive_cannot_clear_known_contradiction",retry.payload["proofs"][0]["relation"] == "UNKNOWN")
                result=f.evaluate(at=NOW+16)
                check(conflict+"_old_positive_excluded_and_timer_fires",result.payload["state"]["trigger_kind"] == "FALLBACK"
                    and all(not d["eligible"] for d in result.payload["decisions"]))
                f.reopen_ledger()
                check(conflict+"_unresolved_cut_reopens",f.repo.exit_record("evaluate") == result)
            finally:
                f.close()

        f=Fixture(directory,"unknown-enriched-before-cut")
        try:
            seq=f.append(15)
            f.capture(proofs={seq:f.pair(unknown=True)})
            enrich_exit_evidence(f.repo,f.binding,proofs={seq:f.pair()},command_id="supported-before-dispatch",recorded_at_utc=utc(NOW+14))
            check("unknown_may_enrich_before_first_cut",f.evaluate().payload["state"]["trigger_kind"] == "MARKET")
        finally:
            f.close()

        f=Fixture(directory,"gap-last-seen")
        try:
            gap=f.append(15,n=50,offset=14,gap=True)
            old=f.append(16,n=51,offset=13.5)
            f.capture(proofs={gap:f.pair(50),old:f.pair(51)})
            result=f.evaluate()
            reasons=[d["reason"] for d in result.payload["decisions"]]
            check("chain_qualified_gap_advances_accepted_last_seen",reasons[-2:] == ["GAP_RECOVERY_NOT_FRESH","OUT_OF_ORDER_REPLAY"])
            check("gap_does_not_trigger",result.payload["state"]["state"] == "MONITORING")
            f.reopen_ledger()
            check("gap_order_state_replays",f.repo.exit_record("evaluate") == result)
        finally:
            f.close()

        f=Fixture(directory,"atomic")
        try:
            seq=f.append(15)
            before=f.repo.write_fence()
            with patch.object(f.repo,"_append_commit",side_effect=RuntimeError("synthetic commit interruption")):
                reject("evidence_insert_rolls_back_with_common_commit",lambda:f.capture(proofs={seq:f.pair()}))
            check("failed_evidence_has_no_partial_row",f.repo.exit_record("capture") is None and f.repo.write_fence() == before)
            evidence=f.capture(proofs={seq:f.pair()})
            with patch.object(f.repo,"_append_commit",side_effect=RuntimeError("synthetic commit interruption")):
                reject("evaluation_insert_rolls_back_with_common_commit",f.evaluate)
            check("failed_evaluation_has_no_partial_row",f.repo.exit_record("evaluate") is None)
            result=f.evaluate()
            reject("stale_common_cut_cannot_commit",lambda:f.repo._record_exit(boundary.EVIDENCE,"stale",f.binding.binding_id,
                evidence.payload_json,fence=f.repo.write_fence()))
            reject("altered_binding_same_id_cannot_replay",lambda:commit_exit_evaluation(f.repo,
                replace(f.binding,fallback_deadline_us=f.binding.fallback_deadline_us+1),command_id="evaluate",timer_fence_utc=utc(NOW+14)))
            f.reopen_ledger()
            check("atomic_rows_replay_exact",f.repo.exit_record("evaluate") == result)
        finally:
            f.close()

        f=Fixture(directory,"raw-change")
        try:
            f.append(15)
            with closing(sqlite3.connect(f.raw)) as raw,raw:
                raw.execute("UPDATE pump_events SET virtual_sol_reserves='999999999999' WHERE rowid=15")
            reject("raw_renormalization_must_match_original_input_fingerprint",f.capture)
            check("raw_source_recovery_failure_preserves_due_timer",f.evaluate(at=NOW+20).payload["state"]["trigger_kind"] == "FALLBACK")
        finally:
            f.close()

        f=Fixture(directory,"v8-reject")
        f.close()
        with closing(sqlite3.connect(f.path)) as conn,conn:
            conn.execute("PRAGMA user_version=8")
        before=f.path.read_bytes()
        reject("v8_schema_explicitly_rejected_without_migration",lambda:LedgerRepository.reopen(f.path,f.domain))
        check("v8_rejection_preserves_database_bytes",f.path.read_bytes() == before)

    print(json.dumps({"status":"IMPLEMENTED_PENDING_PROJECT_REVIEW","checks":len(CHECKS),
        "check_digest":content_fingerprint(CHECKS),"all_checks":True},sort_keys=True))


if __name__ == "__main__":
    main()
