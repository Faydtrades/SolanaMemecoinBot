"""Finite actual-public DRY qualification. Never an activation or live grant.

Fresh isolated stores only; current canonical source is opened by its original
read-only adapter. Candidate admission, simulation and interrupted retirement
belong to the original owners. Missing candidates never become fabricated ones.
"""
from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
import time
from contextlib import closing
from dataclasses import asdict, replace
from pathlib import Path

from phase5.shadow_domain_v0_1 import content_fingerprint, canonical_json
from .authority_controls_v0_1 import require, command_from_record
from .candidate_handoff_v0_1 import CandidateHandoffV01
from .continuous_producer_v0_1 import connect_producer_db
from .continuous_producer_v0_2 import LiveContinuousProducerV02
from .evidence_store_v0_1 import SourceEvidenceStore
from .ledger_repository_v0_1 import LedgerRepository
from .ledger_actions_v0_1 import utc_microseconds
from .ledger_evidence_codec_v0_1 import wallet_observation_from_json
from .operations_ownership_v0_1 import OperationsStore, RestartProfile
from .operations_degradation_v0_1 import DegradationStore
from .operations_startup_v0_1 import schema_fingerprint
from .runtime_dry_profile_v0_1 import read_reference, load_profile, _keys
from .runtime_dry_public_facts_v0_1 import ReviewedPublicFacts
from .runtime_dry_v0_1 import DryQualificationInterruption

SCHEMA = "MEME_LIVE_PUBLIC_DRY_QUALIFICATION_V1"
REQUEST_SCHEMA = "MEME_LIVE_PUBLIC_DRY_QUALIFICATION_REQUEST_V1"


def qualification_health(request, profile):
    """Bind the existing caller-supplied HealthProfile only for explicit C2."""
    from .t010_resource_envelope_v0_1 import observation_only, OBSERVATION_POLICY
    reference = profile.record["inputs"].get("resource_envelope")
    observing = reference is not None and observation_only(read_reference(reference))
    if not observing:
        require("health" not in request and "observation_policy" not in request,
            "PUBLIC_QUALIFICATION_OBSERVATION_SCOPE_CONFLICT")
        return None
    require(request.get("observation_policy") == OBSERVATION_POLICY,
        "PUBLIC_QUALIFICATION_OBSERVATION_POLICY_REQUIRED")
    health = request.get("health")
    _keys(health, ("startup_us", "progress_us", "termination_us"),
        "PUBLIC_QUALIFICATION_HEALTH_REQUIRED")
    from .operations_supervisor_v0_1 import HealthProfile
    return HealthProfile(**health)


def validate_request(request):
    _keys(request, {"schema", "profile", "facts", "controls", "review_reference"}
        | ({"health", "observation_policy"} & request.keys()),
        "PUBLIC_QUALIFICATION_EXPLICIT_REQUEST_REQUIRED")
    require(request["schema"] == REQUEST_SCHEMA and type(request["review_reference"]) is str
        and bool(request["review_reference"]), "PUBLIC_QUALIFICATION_REVIEW_REQUIRED")
    profile = load_profile(read_reference(request["profile"]))
    qualification_health(request, profile)
    require(profile.record["inputs"]["qualification_substitutions"] == {
        "scope":"PUBLIC_ENVIRONMENT_REVIEWED", "synthetic_baseline_and_rpc":False,
        "synthetic_source_binding":False,
        "source_read_path":profile.record["inputs"]["source_start"]["database_identity"]},
        "PUBLIC_QUALIFICATION_SYNTHETIC_SUBSTITUTIONS_DENIED")
    controls = read_reference(request["controls"])
    _keys(controls, ("schema", "profile_content_digest", "review_reference", "commands"),
        "PUBLIC_QUALIFICATION_REVIEWED_CONTROLS_REQUIRED")
    require(controls["schema"] == "MEME_LIVE_REVIEWED_DRY_CONTROLS_V1"
        and controls["profile_content_digest"] == profile.record["content_digest"]
        and bool(controls["review_reference"]), "PUBLIC_QUALIFICATION_CONTROL_BINDING_CONFLICT")
    commands = tuple(command_from_record(record) for record in controls["commands"])
    require(len(commands) == 2 and tuple(command.operation for command in commands) == ("INSTALL_POLICY", "ARM"),
        "PUBLIC_QUALIFICATION_EXPLICIT_INSTALL_AND_DRY_ARM_REQUIRED")
    domain = profile.configuration()["domain"]
    require(all(command.economic_domain_id == domain.economic_domain_id for command in commands)
        and commands[0].policy.economic_domain_id == domain.economic_domain_id
        and commands[1].grant.scope == "DRY" and commands[1].grant.policy_digest == commands[0].policy.content_digest,
        "PUBLIC_QUALIFICATION_DRY_CONTROL_DOMAIN_CONFLICT")
    binding = profile.configuration()["source_binding"]
    require(commands[0].policy.source_identity == binding.source_identity and
        commands[0].policy.source_profile_fingerprint == profile.configuration()["source_profile"].fingerprint,
        "PUBLIC_QUALIFICATION_CONTROL_SOURCE_CONFLICT")
    from .runtime_public_clock_v0_1 import RealPublicClock
    clock = RealPublicClock(read_reference(request["facts"])["clock_profile"],
        profile.configuration()["degradation_config"].host_identity_digest)
    require(clock.policy == commands[0].policy.clock, "PUBLIC_QUALIFICATION_REVIEWED_CLOCK_POLICY_CONFLICT")
    return profile, commands


def initialize_isolated(profile, commands, facts, resource_gate=None):
    """No repair, copying, reopening or initialization of canonical stores."""
    config = profile.configuration()
    paths = profile.record["inputs"]["store_paths"]
    parents = {Path(path).resolve().parent for path in paths.values()}
    require(len(parents) == 1 and not parents.intersection(Path(path).resolve().parent
        for path in (*config["dry_live_paths"], config["market_source"].db_path)),
        "PUBLIC_QUALIFICATION_SEPARATE_STORE_DIRECTORY_REQUIRED")
    require(all(not Path(path).exists() and not any(Path(path+suffix).exists()
        for suffix in ("-wal", "-shm", "-journal")) for path in paths.values()),
        "PUBLIC_QUALIFICATION_FRESH_ISOLATED_STORES_REQUIRED")
    facts.wallet()
    observed = (facts.source() if resource_gate is None else facts.source(
        max_raw_bytes=resource_gate.spec["source_unit_raw_bytes"], reserve_rows=resource_gate.reserve_capture,
        reserve_verdict=resource_gate.reserve_source_verdict))
    baseline = profile.record["inputs"]["baseline"]
    # The exact reviewed baseline is established by Ledger; the fresh public
    # observation above independently verifies that known balance still holds.
    original = wallet_observation_from_json(baseline["observation_json"])
    now = facts.clock().utc_upper_utc
    require(0 <= utc_microseconds(now)-utc_microseconds(original.observed_at_utc) <=
        original.profile.observation_freshness_seconds*1000000, "PUBLIC_REVIEWED_BASELINE_STALE")
    for path in paths.values():
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    with closing(LedgerRepository.initialize(paths["ledger"], config["domain"])) as repo:
        receipt = repo.ingest_wallet_observation(original, ingestion_key="public-dry-reviewed-baseline",
            evaluated_at_utc=baseline["evaluated_at_utc"], required_min_context_slot=baseline["required_min_context_slot"],
            fence=repo.write_fence())
        require(receipt.decision.disposition == "ESTABLISHED", "PUBLIC_DRY_ORIGINAL_BASELINE_DENIED")
        for command in commands:
            repo.record_authority_control(command, fence=repo.write_fence())
        with closing(connect_producer_db(Path(paths["producer"]))) as conn:
            producer = LiveContinuousProducerV02(conn, config["market_source"], profile=config["producer_profile"])
            CandidateHandoffV01(producer, repo, config["source_binding"],
                database_identity=config["database_identity"], initialize=True)
    with SourceEvidenceStore(paths["evidence_store"], config["source_binding"], config["source_profile"]) as store:
        store.append(observed, expected_previous_digest="0"*64)
    require(schema_fingerprint(paths["producer"]) == profile.record["inputs"]["producer_schema_digest"]
        and schema_fingerprint(paths["evidence_store"]) == profile.record["inputs"]["evidence_schema_digest"],
        "PUBLIC_DRY_INITIALIZED_SCHEMA_CONFLICT")
    accepted = read_reference(profile.record["inputs"]["accepted_profile"])
    OperationsStore.initialize(paths["operations"], config["domain"],
        RestartProfile(**accepted["constructor_configuration"]["restart_profile"]), now_us=utc_microseconds(now))
    DegradationStore.initialize(paths["monitor"], config["domain"], config["degradation_config"].policy,
        now_us=utc_microseconds(now))


def _terminal_evidence(repo, root, simulation_params):
    attempts = repo._root_attempts(root)
    require(len(attempts) == 1, "PUBLIC_DRY_EXACTLY_ONE_ORIGINAL_ATTEMPT_REQUIRED")
    attempt = attempts[0]
    require(attempt.primary_signature is None and attempt.signed_wire_digest is None,
        "PUBLIC_DRY_SIGNED_GRAPH_DENIED")
    stages = [json.loads(row[0]) for row in repo._conn.execute(
        "SELECT payload_json FROM ledger_attempt_stages WHERE attempt_id=? ORDER BY revision",
        (attempt.preparation.attempt_id,))]
    exact = [stage for stage in stages if stage["to_stage"] == "EXACT_SIMULATED"]
    require(len(exact) == 1, "PUBLIC_DRY_REAL_EXACT_SIMULATION_REQUIRED")
    stage = exact[0]["stage_input"]
    external = json.loads(stage["external_record_json"])
    require(content_fingerprint(external) == stage["external_record_digest"],
        "PUBLIC_DRY_SIMULATION_RECORD_HASH_CONFLICT")
    require(type(simulation_params) is list and len(simulation_params) == 2
        and content_fingerprint(simulation_params) == external["request_digest"],
        "PUBLIC_DRY_ORIGINAL_RPC_SIMULATION_REQUEST_REQUIRED")
    wire = base64.b64decode(simulation_params[0], validate=True)
    require(wire[:65] == b"\x01"+bytes(64) and wire[65:].hex() == attempt.preparation.message_hex,
        "PUBLIC_DRY_ORIGINAL_ZERO_SIGNATURE_WIRE_REQUIRED")
    simulation = repo._simulation_input_digest(attempt.preparation.attempt_id)
    reservation = repo.reservation(root)
    terminal = None if reservation.retired_sequence is None else repo._port_at_sequence(reservation.retired_sequence)
    require(terminal is not None and terminal.kind == "DRY_NON_SUBMITTED"
        and terminal.dry_terminal.simulation_input_digest == simulation,
        "PUBLIC_DRY_ORIGINAL_NON_SUBMITTED_RECEIPT_REQUIRED")
    return {"root_id":root, "preparation":attempt.preparation.to_record(),
        "preparation_digest":attempt.preparation.content_digest, "attempt_id":attempt.preparation.attempt_id,
        "message_sha256":attempt.preparation.message_sha256,
        "zero_signature_wire_base64":base64.b64encode(wire).decode("ascii"),
        "simulation_request_params":simulation_params,
        "stage":stage, "simulation_input_digest":simulation,
        "terminal_digest":terminal.content_digest, "terminal_record":terminal.to_record(),
        "terminal":asdict(terminal.dry_terminal)}


def qualify(request, *, test_transport=None, test_clock_observer=None):
    profile, commands = validate_request(request)
    if profile.record["inputs"].get("resource_envelope") is not None:
        from .t010_resource_envelope_v0_1 import enforce_current_process_rss, observation_only
        envelope = read_reference(profile.record["inputs"]["resource_envelope"])
        if not observation_only(envelope):
            enforce_current_process_rss(profile.record["inputs"]["monitor"]["resource_limits"]["HOST_RSS_BYTES"])
        guards = envelope["derivation"].get("measured_guards")
        if guards is not None:
            from .t010_resource_envelope_v0_1 import enforce_current_process_private_bytes
            enforce_current_process_private_bytes(guards["child_private_bytes"])
    facts = ReviewedPublicFacts(profile, read_reference(request["facts"]), test_transport=test_transport,
        test_clock_observer=test_clock_observer)
    require(facts.public_clock.policy == commands[0].policy.clock, "PUBLIC_QUALIFICATION_REVIEWED_CLOCK_POLICY_CONFLICT")
    resource_gate = None
    if profile.record["inputs"].get("resource_envelope") is not None:
        from .t010_resource_envelope_v0_1 import ResourceGate
        resource_record = read_reference(profile.record["inputs"]["resource_envelope"])
        if resource_record.get("physical_evidence") is not None:
            from .t010_resource_envelope_v0_1 import check_initial_capacity
            check_initial_capacity(resource_record)
        directory = Path(profile.record["inputs"]["store_paths"]["ledger"]).parent
        directory.mkdir(parents=True, exist_ok=True)
        resource_gate = ResourceGate(directory/
            "qualification_resource_work.json", read_reference(profile.record["inputs"]["resource_envelope"]),
            "qualification", create=True)
    initialize_isolated(profile, commands, facts, resource_gate)
    roots, terminals, startup_audits, steps = [], [], [], []
    simulation_requests = {}
    observed_monitor_maxima = {}
    observed_monitor_minima = {}
    empty_backlog_observations = 0
    interrupted = recovery = None
    started = None

    def start(generation=None):
        before = time.perf_counter_ns()
        if generation is not None:
            facts.begin_cold_reopen()
        value = profile.start(process_identity="public-dry-qualification", now_us=utc_microseconds(facts.clock().utc_upper_utc),
            replace_generation=generation)
        if resource_gate is not None:
            try:
                resource_gate.bind_runtime(value.runtime)
            except Exception:
                value.close()
                raise
        facts.startup_us = (time.perf_counter_ns()-before)//1000
        startup_audits.append({"identity":asdict(value.audit.identity), "owner":asdict(value.audit.owner_fence),
            "reconstruction":asdict(value.audit.reconstruction)})
        return value

    try:
        started = start()
        for _ in range(profile.record["inputs"]["driver"]["max_steps"]):
            if resource_gate is not None and not resource_gate.before_step(started):
                break
            driver = replace(profile.driver(facts, public_transport=test_transport),
                qualification_interrupt_after_simulation=len(terminals) == 1 and interrupted is None)
            try:
                with driver(started) as inputs:
                    try:
                        result = started.runtime.step(**inputs)
                    finally:
                        for raw in inputs["execution"].rpc.records:
                            observed = json.loads(raw)
                            if observed["method"] == "simulateTransaction":
                                require(bool(roots), "PUBLIC_DRY_SIMULATION_WITHOUT_ORIGINAL_ADMISSION")
                                simulation_requests[roots[-1]] = observed["params"]
            except DryQualificationInterruption:
                repo = started.runtime.ledger
                root = roots[-1]
                attempt = repo._root_attempts(root)[0]
                require(attempt.recorded_stage == "EXACT_SIMULATED" and repo.inbox_disposition(root) != "NON_SUBMITTED",
                    "PUBLIC_DRY_DURABLE_INTERRUPTION_CUT_REQUIRED")
                interrupted = {"root_id":root, "attempt_id":attempt.preparation.attempt_id,
                    "preparation_digest":attempt.preparation.content_digest,
                    "simulation_input_digest":repo._simulation_input_digest(attempt.preparation.attempt_id),
                    "ledger_audit":repo.audit()}
                generation = started.runtime._ownership.fence.generation
                started.close()
                started = start(generation)
                require(started.runtime._dry_recovery, "PUBLIC_DRY_COLD_RECOVERY_NOT_RECONSTRUCTED")
                continue
            steps.append(asdict(result))
            if resource_gate is not None:
                resource_gate.after_step(result, started)
            for key, value in started.runtime._operations_degradation.last_metrics:
                if value is not None:
                    prior = observed_monitor_maxima.get(key)
                    observed_monitor_maxima[key] = value if prior is None else max(prior, value)
                    prior = observed_monitor_minima.get(key)
                    observed_monitor_minima[key] = value if prior is None else min(prior, value)
                elif key == "OLDEST_UNCONSUMED_AGE_US":
                    require(started.runtime._operations_degradation.backlog_age_applicable is False,
                        "PUBLIC_DRY_BACKLOG_AGE_UNAVAILABLE")
                    empty_backlog_observations += 1
                    observed_monitor_maxima.setdefault(key, None)
                    observed_monitor_minima.setdefault(key, None)
            if result.work == "ENTRY_ADMITTED":
                roots.append(result.root_id)
            if result.work == "NON_SUBMITTED":
                terminal = _terminal_evidence(started.runtime.ledger, result.root_id, simulation_requests.get(result.root_id))
                terminals.append(terminal)
                if interrupted is not None:
                    require(terminal["attempt_id"] == interrupted["attempt_id"] and
                        terminal["preparation_digest"] == interrupted["preparation_digest"] and
                        terminal["simulation_input_digest"] == interrupted["simulation_input_digest"],
                        "PUBLIC_DRY_COLD_ORIGINAL_IDENTITY_CONFLICT")
                    recovery = {"before":interrupted, "after":terminal, "startup_index":len(startup_audits)-1}
                    break
            require(result.work != "OPERATIONS_HELD", "PUBLIC_DRY_ORIGINAL_OPERATIONS_HELD")
        if len(terminals) != 2 or recovery is None:
            from .ledger_domain_v0_1 import LedgerContractError
            error = LedgerContractError("PUBLIC_DRY_FINITE_BUDGET_NO_TWO_SIMULATIONS_AND_COLD_RECOVERY")
            error.qualification_diagnostics = {"admissions":len(roots), "terminals":len(terminals),
                "steps":steps, "observed_monitor_maxima":observed_monitor_maxima,
                "final_monitor":asdict(started.runtime._operations_degradation.last)}
            raise error
        final_monitor = started.runtime._operations_degradation
        validate_metric_evidence(observed_monitor_maxima, observed_monitor_minima, final_monitor.configuration.policy,
            empty_backlog_observations=empty_backlog_observations,
            observation_policy=final_monitor.configuration.observation_policy)
        require(final_monitor.last.unavailable_code is None and not final_monitor.last.entry_held,
            "PUBLIC_DRY_FINAL_REVIEWED_GUARD_OR_HEALTH_CONFLICT")
        audit = started.runtime.ledger.audit()
        require(all(audit[key] == 0 for key in ("chain_receipt_count", "application_receipt_count", "posting_count")),
            "PUBLIC_DRY_ECONOMIC_OR_BROADCAST_GRAPH_DENIED")
        source = started.runtime.source.latest_record()[1]
        lineage = started.runtime.producer.source_identity
    finally:
        if started is not None:
            started.close()
    stores = []
    for kind, path in profile.record["inputs"]["store_paths"].items():
        with closing(sqlite3.connect(Path(path).as_uri()+"?mode=ro", uri=True)) as conn:
            require(conn.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
                and not conn.execute("PRAGMA foreign_key_check").fetchall(), "PUBLIC_DRY_SQLITE_INTEGRITY_FAILURE")
        stores.append({"kind":kind, "path":path, "sha256":hashlib.sha256(Path(path).read_bytes()).hexdigest()})
    from .acceptance_dossier_v0_1 import source_freeze
    result = {"schema":SCHEMA, "request":request, "request_digest":content_fingerprint(request),
        "source_content_digest":source_freeze(Path(__file__).resolve().parents[2])["content_digest"],
        "profile_content_digest":profile.record["content_digest"], "startup_identity":profile.record["startup_identity"],
        "domain":profile.record["domain"], "source_identity":profile.record["source_identity"],
        "qualification_substitutions":profile.record["inputs"]["qualification_substitutions"],
        "same_root":profile.record["runtime_type"], "guard_amendment":profile.record["inputs"]["guard_amendment"],
        "original_source_mapping":{"source_binding_identity":source.binding.source_identity,
            "market_source_identity":profile.record["source_identity"], "producer_lineage":lineage},
        "scope":facts.scope, "observations":facts.observations, "startup_audits":startup_audits,
        "terminals":terminals, "cold_recovery":recovery, "steps":steps, "stores":stores, "ledger_audit":audit,
        "observed_monitor_maxima":observed_monitor_maxima, "observed_monitor_minima":observed_monitor_minima,
        "empty_backlog_observations":empty_backlog_observations,
        "final_monitor":asdict(final_monitor.last),
        "simulation_count":len(terminals), "cold_interruption_recovery_count":1,
        "signer_send_broadcast":False, "canonical_source_read_only":True,
        "canonical_live_store_access":False, "canonical_live_store_mutation":False, "T010_executed":False,
        "grants_permission":False, "capital_authority":False, "project_acceptance_claimed":False,
        "status":"IMPLEMENTED_PENDING_PROJECT_REVIEW"}
    result["content_digest"] = content_fingerprint(result)
    require(len(canonical_json(result).encode("utf-8")) <= 16777216, "PUBLIC_DRY_EVIDENCE_SIZE_BOUND_EXCEEDED")
    return result


def validate_metric_evidence(maxima, minima, policy, *, empty_backlog_observations=0, observation_policy=None):
    from .operations_degradation_v0_1 import resource_condition
    from .operations_degradation_monitor_v0_1 import observation_metrics, REQUIRED_METRICS
    observed = observation_metrics(policy, observation_policy)
    enforced = {item.metric for item in policy.resource_limits}
    expected = enforced | observed
    require(not observed or REQUIRED_METRICS - observed <= enforced,
        "PUBLIC_DRY_REVIEWED_GUARD_EVIDENCE_CONFLICT")
    require(type(empty_backlog_observations) is int and empty_backlog_observations >= 0,
        "PUBLIC_DRY_REVIEWED_GUARD_EVIDENCE_CONFLICT")
    require(set(maxima) == set(minima) == expected and all(
        (key == "OLDEST_UNCONSUMED_AGE_US" and maxima[key] is minima[key] is None and empty_backlog_observations > 0)
        or (type(minima[key]) is int and type(maxima[key]) is int and 0 <= minima[key] <= maxima[key]
        and (key in observed and maxima[key] < 2**63 or resource_condition(policy, key,
            minima[key] if key == "HOST_DISK_RESERVE_BYTES" else maxima[key],
            configuration_digest=policy.reviewed_configuration_digest) is None)) for key in expected),
        "PUBLIC_DRY_REVIEWED_GUARD_EVIDENCE_CONFLICT")


def validate_evidence(record, profile):
    """Hash-bound original receipts; booleans or synthetic labels cannot qualify."""
    original = dict(record)
    digest = original.pop("content_digest", None)
    require(digest == content_fingerprint(original) and record["schema"] == SCHEMA
        and record["scope"] == "PUBLIC_ENVIRONMENT_REVIEWED", "PUBLIC_DRY_EVIDENCE_SCOPE_OR_HASH_CONFLICT")
    require(record["profile_content_digest"] == profile.record["content_digest"]
        and canonical_json(record["startup_identity"]) == canonical_json(profile.record["startup_identity"])
        and record["request_digest"] == content_fingerprint(record["request"]), "PUBLIC_DRY_EVIDENCE_PROFILE_CONFLICT")
    rebuilt, _ = validate_request(record["request"])
    require(rebuilt.record == profile.record, "PUBLIC_DRY_EVIDENCE_REQUEST_CONFLICT")
    require(record["simulation_count"] == len(record["terminals"]) == 2
        and record["cold_interruption_recovery_count"] == 1 and len(record["startup_audits"]) == 2,
        "PUBLIC_DRY_EVIDENCE_REQUIRED_COUNTS")
    for terminal in record["terminals"]:
        from .ledger_actions_v0_1 import stage_from_record
        from .ledger_ports_v0_1 import port_receipt_from_record
        receipt = port_receipt_from_record(terminal["terminal_record"])
        require(receipt.kind == "DRY_NON_SUBMITTED" and receipt.content_digest == terminal["terminal_digest"]
            and canonical_json(asdict(receipt.dry_terminal)) == canonical_json(terminal["terminal"])
            and receipt.dry_terminal.root_id == terminal["root_id"]
            and receipt.dry_terminal.attempt_id == terminal["attempt_id"], "PUBLIC_DRY_TERMINAL_RECEIPT_CONFLICT")
        stage = stage_from_record(terminal["stage"])
        external = json.loads(stage.external_record_json)
        params = terminal["simulation_request_params"]
        wire = base64.b64decode(terminal["zero_signature_wire_base64"], validate=True)
        require(wire[:65] == b"\x01"+bytes(64) and wire[65:].hex() == terminal["preparation"]["message_hex"]
            and params[0] == terminal["zero_signature_wire_base64"]
            and content_fingerprint(params) == external["request_digest"]
            and params[1]["sigVerify"] is False and params[1]["replaceRecentBlockhash"] is False
            and stage.target_stage == "EXACT_SIMULATED" and stage.signed_wire_base64 is None
            and stage.attempt_id == terminal["attempt_id"] and stage.message_sha256 == terminal["message_sha256"]
            and external["preparation_digest"] == terminal["preparation_digest"]
            and hashlib.sha256(wire[65:]).hexdigest() == terminal["message_sha256"]
            and content_fingerprint(terminal["preparation"]) == terminal["preparation_digest"]
            and content_fingerprint(terminal["stage"]) == terminal["simulation_input_digest"]
            and terminal["terminal"]["simulation_input_digest"] == terminal["simulation_input_digest"],
            "PUBLIC_DRY_EXACT_ORIGINAL_EVIDENCE_CONFLICT")
    recovery = record["cold_recovery"]
    require(recovery["after"] == record["terminals"][1] and all(recovery["before"][key] == recovery["after"][key]
        for key in ("root_id", "attempt_id", "preparation_digest", "simulation_input_digest")),
        "PUBLIC_DRY_RECOVERY_EVIDENCE_CONFLICT")
    require(len({terminal["root_id"] for terminal in record["terminals"]}) == 2
        and all(canonical_json(audit["identity"]) == canonical_json(record["startup_identity"])
            for audit in record["startup_audits"])
        and record["startup_audits"][1]["owner"]["generation"] > record["startup_audits"][0]["owner"]["generation"],
        "PUBLIC_DRY_COLD_STARTUP_BINDING_CONFLICT")
    config = profile.configuration()
    require(record["observations"] and {item["kind"] for item in record["observations"]} == {"WALLET", "SOURCE", "CLOCK"},
        "PUBLIC_DRY_ACTUAL_PUBLIC_OBSERVATIONS_REQUIRED")
    for item in record["observations"]:
        require(item["scope"] == "PUBLIC_ENVIRONMENT_REVIEWED", "PUBLIC_DRY_FAKE_OBSERVATION_DENIED")
        if item["kind"] == "WALLET":
            observation = wallet_observation_from_json(item["observation_json"])
            require(observation.content_digest == item["content_digest"], "PUBLIC_DRY_WALLET_OBSERVATION_DIGEST_CONFLICT")
            if item["decision"] is not None:
                from .ledger_domain_v0_1 import adjudicate_opening_baseline
                decision = adjudicate_opening_baseline(config["domain"], observation,
                    evaluated_at_utc=item["evaluated_at_utc"], required_min_context_slot=item["required_min_context_slot"])
                require(decision.disposition == "ESTABLISHED" and canonical_json(asdict(decision)) == canonical_json(item["decision"]),
                    "PUBLIC_DRY_ORIGINAL_WALLET_OBSERVATION_CONFLICT")
            else:
                from .wallet_evidence_v0_1 import ledger_account_evidence
                domain = config["domain"]
                port = ledger_account_evidence(observation, expected_wallet=domain.wallet, expected_genesis=domain.genesis_hash,
                    expected_profile_fingerprint=domain.expected_profile_fingerprint,
                    required_min_context_slot=item["required_min_context_slot"], now_utc=item["evaluated_at_utc"])
                require(port.account_facts_usable, "PUBLIC_DRY_ORIGINAL_WALLET_SUPPORT_CONFLICT")
        elif item["kind"] == "SOURCE":
            from .source_health_v0_1 import verdict_from_json
            verdict = verdict_from_json(canonical_json(item["verdict"]))
            require(verdict.content_digest == item["content_digest"] and verdict.binding == config["source_binding"]
                and verdict.profile == config["source_profile"] and verdict.disposition == "HEALTHY",
                "PUBLIC_DRY_ORIGINAL_SOURCE_OBSERVATION_CONFLICT")
        else:
            from .runtime_public_clock_v0_1 import RealPublicClock, validate_sample_observation
            from .authority_controls_v0_1 import clock_from_record
            clock = RealPublicClock(item["clock_profile"], config["degradation_config"].host_identity_digest)
            sample = clock_from_record(item["sample"])
            observation = item["observation"]
            validate_sample_observation(observation, sample, clock)
            facts = read_reference(record["request"]["facts"])
            require(item["clock_profile"] == facts["clock_profile"]
                and sample.content_digest == item["content_digest"]
                and observation["scope"] == "PUBLIC_ENVIRONMENT_REVIEWED"
                and sample.provider_id == observation["provider_id"] == clock.provider.provider_id
                and sample.provider_fingerprint == observation["provider_fingerprint"] == clock.provider.fingerprint
                and sample.observation_record_digest == content_fingerprint({"review":item["clock_profile"],
                    "observation":observation, "sample_monotonic_ns":sample.monotonic_ns}),
                "PUBLIC_DRY_ORIGINAL_CLOCK_OBSERVATION_CONFLICT")
    expected_paths = profile.record["inputs"]["store_paths"]
    require(len(record["stores"]) == 5 and {item["kind"] for item in record["stores"]} == set(expected_paths),
        "PUBLIC_DRY_FIVE_ORIGINAL_STORES_REQUIRED")
    for item in record["stores"]:
        from .ledger_domain_v0_1 import digest_value
        digest_value(item["sha256"])
        require(item["path"] == expected_paths[item["kind"]]
            and Path(item["path"]).is_absolute(),
            "PUBLIC_DRY_ORIGINAL_STORE_HASH_CONFLICT")
    validate_metric_evidence(record["observed_monitor_maxima"], record["observed_monitor_minima"],
        config["degradation_config"].policy, empty_backlog_observations=record["empty_backlog_observations"],
        observation_policy=config["degradation_config"].observation_policy)
    require(record["final_monitor"]["unavailable_code"] is None and record["final_monitor"]["entry_held"] is False,
        "PUBLIC_DRY_REVIEWED_GUARD_EVIDENCE_CONFLICT")
    require(record["signer_send_broadcast"] is False and record["T010_executed"] is False
        and record["grants_permission"] is False and record["capital_authority"] is False
        and record["project_acceptance_claimed"] is False
        and record["canonical_live_store_access"] is False and record["canonical_live_store_mutation"] is False
        and record["canonical_source_read_only"] is True,
        "PUBLIC_DRY_EVIDENCE_SAFETY_CONFLICT")
    return record
