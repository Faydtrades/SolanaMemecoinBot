"""Actual mocked Wallet Evidence -> durable LIVE opening baseline qualification.

All databases and subprocess crash cuts use one temporary directory. No network,
runtime, production data, key material, signing or transaction send is used.
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from contextlib import closing
from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from phase5.shadow_domain_v0_1 import canonical_json
from phase5.shadow_venue_route_quote_v0_1 import TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID, WSOL_MINT
from live.ledger_domain_v0_1 import LedgerContractError, LedgerDomain
from live.ledger_evidence_codec_v0_1 import (
    LedgerEvidenceCodecError, wallet_observation_from_json, wallet_observation_to_json,
)
from live.ledger_repository_v0_1 import LedgerConflict, LedgerJournalError, LedgerRepository, _DDL
from live.public_rpc_v0_1 import PublicReadOnlyRpc
from live.wallet_evidence_v0_1 import ExpectedTokenAccount, WalletEvidenceAdapter, WalletEvidenceRequest
# Reuse accepted deterministic RPC fixture builders only; do not invoke that suite.
from live_wallet_evidence_selftest_v0_1 import (
    Scenario, WALLET, MINT, TOKEN, MINT22, TOKEN22, OTHER, GENESIS, BLOCK, NOW, PROFILE, ENDPOINT,
    account, key, mint_data, token_data, utc,
)

CHECKS = {}
MAX_U64 = (1 << 64) - 1
FUNDING = 5_000_000_000


def check(name, condition):
    CHECKS[name] = bool(condition)
    if not condition:
        raise AssertionError(name)


def raises(kind, call):
    try:
        call()
    except kind:
        return True
    return False


def domain(**changes):
    return replace(LedgerDomain(GENESIS, WALLET, PROFILE.fingerprint, FUNDING, minimum_context_slot=100), **changes)


def observe(scenario=None, *, request=None, profile=PROFILE, at=NOW):
    scenario = scenario or Scenario()
    request = request or WalletEvidenceRequest(WALLET, GENESIS, 100)
    with PublicReadOnlyRpc(ENDPOINT, profile, transport=httpx.MockTransport(scenario.handle)) as rpc:
        return WalletEvidenceAdapter(rpc, request, clock=lambda: utc(at)).observe()


def ingest(repo, observation, key="opening", *, at=NOW, floor=100, fence=None):
    return repo.ingest_wallet_observation(observation, ingestion_key=key, evaluated_at_utc=utc(at),
        required_min_context_slot=floor, fence=repo.write_fence() if fence is None else fence)


def configured_token(token=TOKEN, mint=MINT, program=TOKEN_PROGRAM_ID):
    expected = (ExpectedTokenAccount(token, mint, program),)
    return domain(expected_empty_token_accounts=expected), WalletEvidenceRequest(WALLET, GENESIS, 100, expected)


def subprocess_cut(mode, path):
    return subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), mode, str(path)],
                          cwd=ROOT, capture_output=True, text=True, timeout=30)


def child(mode, path):
    binding = domain()
    if mode == "--compete":
        try:
            with LedgerRepository.reopen(path, binding) as repo:
                ingest(repo, observe())
        except LedgerConflict:
            return 23
        return 24
    with LedgerRepository.reopen(path, binding) as repo:
        original_commit = repo._commit

        def interrupt_commit():
            if mode == "--crash-before":
                os._exit(71)
            original_commit()
            os._exit(72)

        repo._commit = interrupt_commit
        ingest(repo, observe())
    return 99


def denial_case(directory, name, observation, *, binding=None, expected="QUARANTINED", at=NOW, floor=100):
    binding = binding or domain()
    path = directory / f"denial-{name}.sqlite3"
    with LedgerRepository.initialize(path, binding) as repo:
        receipt = ingest(repo, observation, at=at, floor=floor)
        check(f"{name}_durable_{expected.lower()}", receipt.decision.disposition == expected
              and receipt.decision.owned_native_lamports is None
              and receipt.decision.locked_token_account_lamports is None and repo.baseline() is None
              and receipt.decision.reasons and repo.audit()["revision"] == 1)
    with LedgerRepository.reopen(path, binding) as repo:
        replayed = repo.receipt("opening")
        check(f"{name}_original_denial_replayed", replayed == receipt and replayed.observation == observation
              and repo.baseline() is None)
    return receipt


def main():
    with tempfile.TemporaryDirectory(prefix="live-ledger-baseline-") as temporary:
        directory = Path(temporary)
        binding = domain()
        clean = observe()
        path = directory / "ledger.sqlite3"
        with LedgerRepository.initialize(path, binding) as repo:
            check("verified_WAL_FULL_FK_dedicated_journal", repo.audit()["revision"] == 0
                  and repo._conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
                  and repo._conn.execute("PRAGMA synchronous").fetchone()[0] == 2
                  and repo._conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1)
            initial_fence = repo.write_fence()
            first = ingest(repo, clean, fence=initial_fence)
            check("actual_WalletEvidenceAdapter_to_original_durable_baseline", first.decision.disposition == "ESTABLISHED"
                  and first.decision.owned_native_lamports == FUNDING
                  and first.decision.observed_native_lamports == FUNDING
                  and first.decision.native_account_presence == "PRESENT"
                  and first.decision.context_slot == clean.anchor.slot == 101
                  and first.decision.locked_token_account_lamports == 0
                  and first.observation == clean and repo.baseline() == first)
            check("exact_retry_with_old_revision_is_same_immutable_receipt", ingest(repo, clean, fence=initial_fence) == first
                  and repo.audit()["revision"] == 1)
            check("same_evidence_new_identity_rejected", raises(LedgerConflict, lambda: ingest(repo, clean, "replacement")))
            check("same_identity_changed_time_rejected", raises(LedgerConflict, lambda: ingest(repo, clean, at=NOW+1)))
            changed = observe(at=NOW+1)
            check("same_identity_changed_original_evidence_rejected", raises(LedgerConflict, lambda: ingest(repo, changed)))
            check("competing_clean_baseline_rejected", raises(LedgerConflict,
                  lambda: ingest(repo, changed, "second-baseline", at=NOW+1)))
            failed = Scenario()
            failed.program_failure = TOKEN_2022_PROGRAM_ID
            unknown = observe(failed, at=NOW+1)
            check("new_record_stale_revision_fails_CAS", raises(LedgerConflict,
                  lambda: ingest(repo, unknown, "unknown", at=NOW+1, fence=initial_fence)))
            unknown_receipt = ingest(repo, unknown, "unknown", at=NOW+1)
            check("new_unknown_observation_retained_without_replacing_baseline",
                  unknown_receipt.decision.disposition == "UNRESOLVED"
                  and unknown_receipt.decision.owned_native_lamports is None and repo.baseline() == first
                  and repo.audit()["revision"] == 2)
            check("competing_writer_same_process_excluded", raises(LedgerConflict, lambda: LedgerRepository.reopen(path, binding)))
            result = subprocess_cut("--compete", path)
            check("competing_OS_process_cannot_open_or_mutate", result.returncode == 23
                  and not result.stdout and not result.stderr and repo.audit()["revision"] == 2)
            for table in ("ledger_domain", "ledger_writer_generations", "ledger_wallet_observations", "ledger_baseline_journal"):
                check(f"{table}_SQL_update_rejected", raises(sqlite3.IntegrityError,
                      lambda table=table: repo._conn.execute(f"UPDATE {table} SET payload_json=payload_json")))
                check(f"{table}_SQL_delete_rejected", raises(sqlite3.IntegrityError,
                      lambda table=table: repo._conn.execute(f"DELETE FROM {table}")))
            old_fence = repo.write_fence()
            old_audit = repo.audit()
            check("immutable_domain_and_original_receipt", raises(FrozenInstanceError,
                  lambda: setattr(binding, "wallet", OTHER)) and raises(FrozenInstanceError,
                  lambda: setattr(first.decision, "owned_native_lamports", 1)))
        check("closed_writer_cannot_mutate", raises(LedgerJournalError, lambda: ingest(repo, clean)))
        with LedgerRepository.reopen(path, binding) as repo:
            new_audit = repo.audit()
            check("reopen_reconstructs_durable_evidence_and_original_decision",
                  new_audit["generation"] == old_audit["generation"]+1
                  and new_audit["revision"] == old_audit["revision"]
                  and new_audit["last_receipt_digest"] == old_audit["last_receipt_digest"]
                  and repo.baseline() == first and repo.receipt("unknown") == unknown_receipt)
            check("old_generation_cannot_even_replay_write", raises(LedgerConflict,
                  lambda: ingest(repo, clean, fence=old_fence)))
            check("existing_receipt_reuses_original_time_after_reopen", ingest(repo, clean) == first
                  and first.decision.evaluated_at_utc == utc(NOW))
            stale_new = observe(at=NOW+2)
            stale_receipt = ingest(repo, stale_new, "stale-new", at=NOW+40)
            check("stale_NEW_observation_unresolved_existing_baseline_survives",
                  stale_receipt.decision.disposition == "UNRESOLVED"
                  and "CONSUMER_OBSERVATION_STALE" in stale_receipt.decision.reasons
                  and repo.baseline() == first and repo.audit()["revision"] == 3)
            check("fence_rejects_bool_integer_fields", raises(LedgerContractError,
                  lambda: replace(repo.write_fence(), generation=True)))

        check("metadata_changes_keep_economic_identity", binding.economic_domain_id
              == replace(binding, known_native_wallet_lamports=0).economic_domain_id
              == replace(binding, expected_profile_fingerprint="1"*64).economic_domain_id
              and binding.binding_digest != replace(binding, known_native_wallet_lamports=0).binding_digest)
        for name, changed_domain in (
            ("funding", replace(binding, known_native_wallet_lamports=0)),
            ("profile", replace(binding, expected_profile_fingerprint="1"*64)),
            ("wallet", replace(binding, wallet=OTHER)), ("genesis", replace(binding, genesis_hash=BLOCK)),
            ("minimum_cut", replace(binding, minimum_context_slot=101)),
        ):
            before = path.read_bytes()
            check(f"immutable_{name}_binding_reopen_denied", raises(LedgerConflict,
                  lambda changed_domain=changed_domain: LedgerRepository.reopen(path, changed_domain)))
            check(f"wrong_{name}_binding_did_not_change_journal", path.read_bytes() == before)
        check("mode_and_schema_cannot_create_alternate_identity", raises(LedgerContractError,
              lambda: replace(binding, mode="PAPER")) and raises(LedgerContractError,
              lambda: replace(binding, schema_version="future")))
        check("existing_journal_never_reinitialized", raises(LedgerConflict, lambda: LedgerRepository.initialize(path, binding)))
        missing = directory / "expected-missing.sqlite3"
        check("expected_missing_journal_not_silently_created", raises(LedgerJournalError,
              lambda: LedgerRepository.reopen(missing, binding)) and not missing.exists())
        empty = directory / "uninitialized.sqlite3"
        empty.touch()
        check("interrupted_initialization_requires_explicit_resolution", raises(LedgerJournalError,
              lambda: LedgerRepository.reopen(empty, binding)) and raises(LedgerConflict,
              lambda: LedgerRepository.initialize(empty, binding)) and empty.stat().st_size == 0)

        zero_scenario = Scenario()
        zero_scenario.accounts[WALLET]["lamports"] = 0
        with LedgerRepository.initialize(directory / "zero.sqlite3", domain(known_native_wallet_lamports=0)) as repo:
            zero = ingest(repo, observe(zero_scenario))
            check("present_configured_zero_is_exact_owned_zero", zero.decision.disposition == "ESTABLISHED"
                  and zero.decision.native_account_presence == "PRESENT" and zero.decision.owned_native_lamports == 0)
        absent_scenario = Scenario()
        absent_scenario.accounts[WALLET] = None
        absent = denial_case(directory, "absent_native", observe(absent_scenario), expected="UNRESOLVED")
        unknown_scenario = Scenario()
        unknown_scenario.transform = lambda request, envelope: {
            "jsonrpc": "2.0", "id": request["id"], "error": {"message": "DO_NOT_RETAIN"}}
        unavailable = denial_case(directory, "unknown_native", observe(unknown_scenario))
        check("explicit_absence_distinct_from_unknown_and_zero", absent.decision.native_account_presence == "ABSENT"
              and unavailable.decision.native_account_presence == "UNKNOWN"
              and absent.decision.observed_native_lamports is None and unavailable.decision.observed_native_lamports is None)
        denial_case(directory, "unknown_funding_configuration", clean,
                    binding=domain(known_native_wallet_lamports=None), expected="UNRESOLVED")
        mismatch = Scenario()
        mismatch.accounts[WALLET]["lamports"] = FUNDING+1
        denial_case(directory, "unattributed_native_difference", observe(mismatch))

        known_domain, request = configured_token()
        known_empty = Scenario().add_token(data=token_data(amount=0), lamports=1234567)
        with LedgerRepository.initialize(directory / "known-empty.sqlite3", known_domain) as repo:
            known = ingest(repo, observe(known_empty, request=request))
            check("supported_known_empty_account_retains_locked_lamports_and_identity",
                  known.decision.disposition == "ESTABLISHED"
                  and known.decision.locked_token_account_lamports == 1234567
                  and known.decision.owned_native_lamports == FUNDING
                  and known.decision.known_token_accounts[0].presence == "PRESENT"
                  and known.decision.known_token_accounts[0].mint == MINT
                  and known.decision.known_token_accounts[0].program == TOKEN_PROGRAM_ID
                  and "rent_reserve" not in repr(known.decision.known_token_accounts[0]))
        absent_token = Scenario()
        absent_token.accounts[MINT] = account(TOKEN_PROGRAM_ID, mint_data())
        with LedgerRepository.initialize(directory / "known-absent.sqlite3", known_domain) as repo:
            no_account = ingest(repo, observe(absent_token, request=request))
            check("known_token_explicit_absence_is_preserved_with_complete_coverage",
                  no_account.decision.disposition == "ESTABLISHED"
                  and no_account.decision.known_token_accounts[0].presence == "ABSENT"
                  and no_account.decision.known_token_accounts[0].observed_lamports is None
                  and no_account.decision.locked_token_account_lamports == 0)
        unknown_empty = Scenario().add_token(data=token_data(amount=0))
        denial_case(directory, "unconfigured_empty_token_account", observe(unknown_empty))
        denial_case(directory, "known_nonzero_tokens", observe(Scenario().add_token(), request=request), binding=known_domain)
        denial_case(directory, "known_account_request_omitted", observe(Scenario()), binding=known_domain)
        for name, amount in (("zero", 0), ("nonzero", 20)):
            wsol = Scenario().add_token(mint=WSOL_MINT,
                data=token_data(WSOL_MINT, amount=amount, reserve=99), mint_bytes=mint_data(9), lamports=99+amount)
            denied = denial_case(directory, f"preexisting_{name}_WSOL", observe(wsol))
            check(f"{name}_WSOL_account_rejected_even_if_Evidence_shape_supported",
                  denied.decision.supported_shape == "SUPPORTED"
                  and "PREEXISTING_WSOL_ACCOUNT_UNSUPPORTED" in denied.decision.reasons)
        for name, data in (("frozen", token_data(amount=0, frozen=True)),
                           ("delegated", token_data(amount=0, delegate=OTHER)),
                           ("extension", token_data(amount=0, tail=b"\x02"))):
            denial_case(directory, f"unsupported_{name}", observe(Scenario().add_token(data=data), request=request), binding=known_domain)
        missing_mint = Scenario().add_token(data=token_data(amount=0))
        missing_mint.accounts.pop(MINT)
        denial_case(directory, "missing_mint", observe(missing_mint, request=request), binding=known_domain, expected="UNRESOLVED")
        incomplete = Scenario()
        incomplete.program_failure = TOKEN_2022_PROGRAM_ID
        denial_case(directory, "incomplete_token_coverage", observe(incomplete), expected="UNRESOLVED")
        mixed = Scenario()
        mixed.program_context[TOKEN_2022_PROGRAM_ID] = 102
        denial_case(directory, "mixed_finalized_account_cut", observe(mixed), expected="UNRESOLVED")
        genesis_change = Scenario()
        genesis_change.end_genesis = BLOCK
        denial_case(directory, "changing_genesis", observe(genesis_change))
        denial_case(directory, "wrong_wallet", observe(request=WalletEvidenceRequest(OTHER, GENESIS, 100)))
        denial_case(directory, "wrong_profile", clean, binding=domain(expected_profile_fingerprint="1"*64))
        denial_case(directory, "unproven_context_floor", clean, floor=102, expected="UNRESOLVED")
        denial_case(directory, "floor_below_configuration", clean, floor=99, expected="UNRESOLVED")
        unsupported_native = Scenario()
        unsupported_native.accounts[WALLET]["executable"] = True
        denial_case(directory, "unsupported_native_shape", observe(unsupported_native))

        # Both individual amounts and totals beyond signed SQLite/u64 limits stay exact.
        large = Scenario().add_token(data=token_data(amount=0), lamports=MAX_U64).add_token(
            token=TOKEN22, mint=MINT22, program=TOKEN_2022_PROGRAM_ID,
            data=token_data(MINT22, amount=0), lamports=MAX_U64)
        large.accounts[WALLET]["lamports"] = MAX_U64
        expectations = (ExpectedTokenAccount(TOKEN, MINT, TOKEN_PROGRAM_ID),
                        ExpectedTokenAccount(TOKEN22, MINT22, TOKEN_2022_PROGRAM_ID))
        large_domain = domain(known_native_wallet_lamports=MAX_U64, expected_empty_token_accounts=expectations)
        large_observation = observe(large, request=WalletEvidenceRequest(WALLET, GENESIS, 100, expectations))
        large_path = directory / "full-u64.sqlite3"
        with LedgerRepository.initialize(large_path, large_domain) as repo:
            large_receipt = ingest(repo, large_observation)
            check("full_u64_native_and_greater_than_u64_aggregate_exact",
                  large_receipt.decision.owned_native_lamports == MAX_U64
                  and large_receipt.decision.locked_token_account_lamports == 2*MAX_U64)
            row = repo._conn.execute("SELECT typeof(payload_json),payload_json FROM ledger_baseline_journal").fetchone()
            payload = json.loads(row[1])
            check("money_is_JSON_integer_in_SQLite_TEXT_never_REAL_or_signed64",
                  row[0] == "text" and type(payload["decision"]["owned_native_lamports"]) is int
                  and payload["decision"]["owned_native_lamports"] == MAX_U64
                  and payload["decision"]["locked_token_account_lamports"] == 2*MAX_U64)
        with LedgerRepository.reopen(large_path, large_domain) as repo:
            check("full_u64_original_bytes_and_aggregate_survive_reopen", repo.baseline() == large_receipt
                  and repo.baseline().observation.explicit_read.accounts[0].rent_epoch == MAX_U64
                  and repo.audit()["replayed_receipts"] == 1)

        encoded = wallet_observation_to_json(large_observation)
        check("explicit_original_Evidence_codec_roundtrip", wallet_observation_from_json(encoded) == large_observation
              and "DO_NOT_RETAIN" not in encoded and "fixture.invalid" not in encoded)
        for name, mutate in (
            ("schema", lambda value: value["observation"].update(schema="future")),
            ("codec", lambda value: value.update(codec_version="future")),
            ("evidence_model", lambda value: value.update(evidence_model_id="future")),
            ("content_digest", lambda value: value.update(evidence_digest="1"*64)),
            ("extra_fields", lambda value: value["observation"].update(unaccepted="DO_NOT_RETAIN")),
            ("boolean_lamports", lambda value: value["observation"]["explicit_read"]["accounts"][0].update(lamports=True)),
            ("float_lamports", lambda value: value["observation"]["explicit_read"]["accounts"][0].update(lamports=1.0)),
            ("too_large_lamports", lambda value: value["observation"]["explicit_read"]["accounts"][0].update(lamports=1 << 64)),
            ("bad_base64", lambda value: value["observation"]["inventories"][0]["accounts"][0].update(data_base64="!")),
        ):
            value = json.loads(encoded)
            mutate(value)
            check(f"original_codec_rejects_{name}", raises(LedgerEvidenceCodecError,
                  lambda value=value: wallet_observation_from_json(canonical_json(value))))
        duplicate_json = encoded.replace('"codec_version":', '"codec_version":"bad","codec_version":', 1)
        check("original_codec_rejects_duplicate_keys_and_noncanonical_JSON", raises(LedgerEvidenceCodecError,
              lambda: wallet_observation_from_json(duplicate_json)) and raises(LedgerEvidenceCodecError,
              lambda: wallet_observation_from_json(" " + encoded)))
        for raw in ('{"x":NaN}', '{"x":1e999}', 'DO_NOT_RETAIN', '{"truncated":'):
            try:
                wallet_observation_from_json(raw)
                raise AssertionError("invalid payload accepted")
            except LedgerEvidenceCodecError as exc:
                check("codec_error_codes_are_sanitized", str(exc) == "INVALID_WALLET_EVIDENCE_PAYLOAD")

        for name, table in (("paper", "paper_positions"), ("shadow", "shadow_execution_intents"),
                            ("source", "source_evidence")):
            other = directory / f"unrelated-{name}.sqlite3"
            with closing(sqlite3.connect(other, isolation_level=None)) as connection:
                connection.execute(f"CREATE TABLE {table}(id INTEGER PRIMARY KEY)")
                connection.execute(f"INSERT INTO {table} VALUES(1)")
            original_bytes = other.read_bytes()
            check(f"{name}_database_rejected_by_readonly_preflight", raises(LedgerJournalError,
                  lambda other=other: LedgerRepository.reopen(other, binding)))
            with closing(sqlite3.connect(other, isolation_level=None)) as connection:
                unchanged = connection.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
            check(f"{name}_database_never_normalized_or_mutated", other.read_bytes() == original_bytes
                  and unchanged and not other.with_name(other.name + "-wal").exists())
        corrupt = directory / "not-sqlite.sqlite3"
        corrupt.write_bytes(b"CORRUPTED_TEMP_FIXTURE_ONLY")
        check("corrupt_database_fails_closed", raises(LedgerJournalError, lambda: LedgerRepository.reopen(corrupt, binding)))

        for name, tamper in (
            ("schema", lambda conn: conn.execute("CREATE TABLE unexpected(id INTEGER)")),
            ("storage_version", lambda conn: conn.execute("PRAGMA user_version=999")),
            ("head", lambda conn: conn.execute("UPDATE ledger_head SET revision=99")),
            ("missing_trigger", lambda conn: conn.execute("DROP TRIGGER ledger_baseline_journal_no_delete")),
        ):
            damaged = directory / f"tampered-{name}.sqlite3"
            with LedgerRepository.initialize(damaged, binding) as repo:
                ingest(repo, clean)
            with closing(sqlite3.connect(damaged, isolation_level=None)) as connection:
                tamper(connection)
            check(f"reopen_rejects_{name}_integrity_conflict", raises(LedgerJournalError,
                  lambda damaged=damaged: LedgerRepository.reopen(damaged, binding)))
        for name, table in (("original", "ledger_wallet_observations"), ("receipt", "ledger_baseline_journal"),
                            ("generation", "ledger_writer_generations")):
            damaged = directory / f"content-tampered-{name}.sqlite3"
            with LedgerRepository.initialize(damaged, binding) as repo:
                ingest(repo, clean)
            with closing(sqlite3.connect(damaged, isolation_level=None)) as connection:
                trigger = f"{table}_no_update"
                connection.execute(f"DROP TRIGGER {trigger}")
                connection.execute(f"UPDATE {table} SET payload_json='{{}}'")
                connection.execute(_DDL[trigger])
            check(f"reopen_rederives_and_rejects_changed_{name}_content", raises(LedgerJournalError,
                  lambda damaged=damaged: LedgerRepository.reopen(damaged, binding)))

        crash_evidence = {}
        for mode, exit_code, expected_count in (("--crash-before", 71, 0), ("--crash-after", 72, 1)):
            interrupted = directory / f"{mode[2:]}.sqlite3"
            with LedgerRepository.initialize(interrupted, binding):
                pass
            result = subprocess_cut(mode, interrupted)
            check(f"{mode[2:]}_subprocess_reached_exact_cut", result.returncode == exit_code
                  and not result.stdout and not result.stderr)
            with LedgerRepository.reopen(interrupted, binding) as repo:
                original_count = repo._conn.execute("SELECT COUNT(*) FROM ledger_wallet_observations").fetchone()[0]
                check(f"{mode[2:]}_Evidence_receipt_atomic_recovery", repo.audit()["revision"] == expected_count
                      and original_count == expected_count and (repo.baseline() is None) == (expected_count == 0))
                recovered = ingest(repo, clean)
                check(f"{mode[2:]}_retry_converges_without_duplicate_baseline",
                      recovered.decision == first.decision and repo.audit()["revision"] == 1
                      and ingest(repo, clean) == recovered)
                crash_evidence[mode[2:]] = {"child_exit": result.returncode, "recovered_before_retry": expected_count,
                    "after_retry": repo.audit()["revision"], "decision_digest": recovered.decision.content_digest}
        check("both_crash_cuts_converge_to_same_original_economic_baseline",
              crash_evidence["crash-before"]["decision_digest"] == crash_evidence["crash-after"]["decision_digest"])

        report = {"status": "IMPLEMENTED_PENDING_PROJECT_REVIEW", "checks": CHECKS,
            "failed": [name for name, value in CHECKS.items() if not value], "transport": "HTTPX_MOCK_ONLY",
            "real_network_requests": 0, "database_scope": "TEMPORARY_DIRECTORY_ONLY", "crash_cuts": crash_evidence,
            "original_evaluation_utc": first.decision.evaluated_at_utc, "baseline_replay_digest": first.decision.content_digest,
            "original_Evidence_digest": clean.content_digest}
    check("temporary_databases_and_subprocess_files_cleaned", not directory.exists())
    report["check_count"] = len(CHECKS)
    print(json.dumps(report, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] in ("--compete", "--crash-before", "--crash-after"):
        raise SystemExit(child(sys.argv[1], Path(sys.argv[2])))
    if len(sys.argv) != 1:
        raise SystemExit("unsupported selftest arguments")
    raise SystemExit(main())
