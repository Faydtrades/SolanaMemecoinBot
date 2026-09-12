"""Explicit supported DRY configuration, bound to original owners and public facts.

Construction is read-only. It neither initializes stores nor makes an unknown
baseline established. Original Ledger adjudication must establish the supplied
reviewed public observation. Every source/start/path change creates a new
profile requiring its own qualification and review.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from urllib.parse import urlsplit

from phase4.paper_continuous_market_source_v0_2 import ContinuousMarketSourceV02
from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from phase5.shadow_venue_route_quote_v0_1 import QuotePolicyV01
from phase5.shadow_unsigned_plan_simulation_v0_1 import TransactionPlanPolicyV01
from .authority_controls_v0_1 import require
from .ledger_domain_v0_1 import LedgerDomain, adjudicate_opening_baseline, ledger_utc, digest_value
from .ledger_evidence_codec_v0_1 import wallet_observation_from_json
from .continuous_producer_v0_2 import ContinuationProfileV02
from .source_health_v0_1 import SourceBinding, SourceProfile, CursorWitness
from .public_rpc_v0_1 import PublicRpcProfile
from .operations_ownership_v0_1 import RestartProfile
from .operations_degradation_v0_1 import DegradationPolicy, ResourceLimit
from .operations_degradation_monitor_v0_1 import MonitorConfiguration, monitor_configuration_digest
from .operations_startup_v0_1 import configured_identity, dry_monitor_fingerprint, start_dry, _distinct_dry_paths
from .runtime_dry_public_driver_v0_1 import DryPublicInputDriver, DryPublicFacts, drive_steps
from .execution_message_v0_1 import ComputeBudget

SCHEMA = "MEME_LIVE_SUPPORTED_DRY_PROFILE_V1"
SOURCE_START = "EXPLICIT_RETAINED_ANCHOR_ORIGINAL_CONTINUATION_NO_REPAIR"
FACTS_PROVIDER = "ORIGINAL_TYPED_PUBLIC_FACTS_NO_ECONOMIC_CALLBACK"
KEYS = {"accepted_profile", "accepted_extension", "accepted_monitor", "guard_amendment", "baseline", "source_start", "store_paths",
    "producer_schema_digest", "evidence_schema_digest", "monitor", "driver", "qualification_substitutions"}


def _keys(value, keys, reason):
    require(type(value) is dict and set(value) == set(keys), reason)


def _same(left, right):
    return canonical_json(left) == canonical_json(right)


def read_reference(reference):
    _keys(reference, ("path", "sha256"), "DRY_PROFILE_REFERENCE_REQUIRED")
    path = Path(reference["path"])
    digest_value(reference["sha256"])
    require(path.is_absolute() and path.suffix.lower() == ".json", "DRY_PROFILE_PUBLIC_JSON_REQUIRED")
    raw = path.read_bytes()
    require(len(raw) <= 16777216 and hashlib.sha256(raw).hexdigest() == reference["sha256"],
        "DRY_PROFILE_REFERENCE_HASH_CONFLICT")
    return json.loads(raw)


def _construct(inputs):
    _keys(inputs, KEYS, "DRY_PROFILE_EXPLICIT_INPUTS_REQUIRED")
    accepted, extension = read_reference(inputs["accepted_profile"]), read_reference(inputs["accepted_extension"])
    require(extension["parent_profile_sha256"] == inputs["accepted_profile"]["sha256"]
        and extension["parent_profile_content_digest"] == accepted["content_digest"],
        "DRY_PROFILE_ACCEPTED_CONSTRAINT_BINDING_CONFLICT")
    accepted_monitor = read_reference(inputs["accepted_monitor"])
    require(accepted_monitor["extension_content_digest"] == extension["content_digest"]
        and accepted_monitor["parent_profile_content_digest"] == accepted["content_digest"],
        "DRY_PROFILE_ACCEPTED_MONITOR_BINDING_CONFLICT")
    original_monitor = accepted_monitor["monitor"]
    target, original = accepted["target"], accepted["constructor_configuration"]
    baseline = inputs["baseline"]
    _keys(baseline, ("observation_json", "known_native_wallet_lamports", "evaluated_at_utc",
        "required_min_context_slot", "review_reference"), "DRY_PROFILE_REVIEWED_ORIGINAL_BASELINE_REQUIRED")
    require(type(baseline["review_reference"]) is str and bool(baseline["review_reference"]),
        "DRY_PROFILE_BASELINE_REVIEW_REQUIRED")
    require(type(baseline["known_native_wallet_lamports"]) is int,
        "DRY_PROFILE_NO_UNKNOWN_OR_DEFAULT_BASELINE")
    domain = LedgerDomain(**dict(target["domain"], mode="DRY", expected_empty_token_accounts=(),
        known_native_wallet_lamports=baseline["known_native_wallet_lamports"]))
    observation = wallet_observation_from_json(baseline["observation_json"])
    decision = adjudicate_opening_baseline(domain, observation,
        evaluated_at_utc=baseline["evaluated_at_utc"], required_min_context_slot=baseline["required_min_context_slot"])
    require(decision.disposition == "ESTABLISHED", "DRY_PROFILE_ORIGINAL_BASELINE_NOT_ESTABLISHED")
    start = inputs["source_start"]
    _keys(start, ("semantics", "binding", "profile", "start_after_p1_rowid", "database_identity",
        "read_path", "review_reference"), "DRY_PROFILE_EXPLICIT_SOURCE_START_REQUIRED")
    require(start["semantics"] == SOURCE_START and type(start["review_reference"]) is str
        and bool(start["review_reference"]), "DRY_PROFILE_SOURCE_START_REVIEW_REQUIRED")
    require(Path(start["database_identity"]).resolve() == Path(target["paths"]["market_source"]).resolve(),
        "DRY_PROFILE_ORIGINAL_DATABASE_IDENTITY_REQUIRED")
    binding = SourceBinding(**dict(start["binding"], anchors=tuple(CursorWitness(**r) for r in start["binding"]["anchors"])))
    require(type(start["start_after_p1_rowid"]) is int
        and start["start_after_p1_rowid"] == next(a.rowid for a in binding.anchors if a.table == "pump_events"),
        "DRY_PROFILE_RETAINED_ANCHOR_REQUIRED")
    require(_same(start["profile"], accepted["source_configuration"]["profile"]), "DRY_PROFILE_SOURCE_PROFILE_CHANGED")
    substitutions = inputs["qualification_substitutions"]
    _keys(substitutions, ("scope", "synthetic_baseline_and_rpc", "synthetic_source_binding", "source_read_path"),
        "DRY_PROFILE_EXPLICIT_QUALIFICATION_SUBSTITUTIONS_REQUIRED")
    require(substitutions["scope"] in ("DETERMINISTIC_QUALIFICATION_ONLY", "PUBLIC_ENVIRONMENT_REVIEWED"),
        "DRY_PROFILE_SCOPE_REQUIRED")
    synthetic = substitutions["scope"] == "DETERMINISTIC_QUALIFICATION_ONLY"
    require(type(substitutions["synthetic_baseline_and_rpc"]) is bool
        and type(substitutions["synthetic_source_binding"]) is bool
        and substitutions["synthetic_baseline_and_rpc"] == synthetic,
        "DRY_PROFILE_OBSERVATION_SUBSTITUTION_CONFLICT")
    require(substitutions["source_read_path"] == start["read_path"], "DRY_PROFILE_SOURCE_PATH_SUBSTITUTION_CONFLICT")
    require(Path(start["read_path"]).is_absolute(), "DRY_PROFILE_ABSOLUTE_SOURCE_PATH_REQUIRED")
    if synthetic:
        require(substitutions["synthetic_source_binding"]
            and Path(start["read_path"]).resolve() != Path(start["database_identity"]).resolve(),
            "DRY_PROFILE_SYNTHETIC_SOURCE_SUBSTITUTION_REQUIRED")
    if not synthetic:
        require(not substitutions["synthetic_source_binding"]
            and Path(start["read_path"]).resolve() == Path(start["database_identity"]).resolve(),
            "DRY_PROFILE_PRODUCTION_SOURCE_SUBSTITUTION_DENIED")
    paths = inputs["store_paths"]
    _keys(paths, ("ledger", "producer", "operations", "evidence_store", "monitor"), "DRY_PROFILE_FIVE_DISTINCT_STORES_REQUIRED")
    source_profile = SourceProfile(**start["profile"])
    market = ContinuousMarketSourceV02(start["read_path"], start_after_p1_rowid=start["start_after_p1_rowid"],
        database_identity=start["database_identity"])
    producer = ContinuationProfileV02(**original["continuation_profile"])
    restart = RestartProfile(**original["restart_profile"])
    monitor = inputs["monitor"]
    _keys(monitor, ("resource_limits", "persistent_unknown_alert_us", "recovery_evidence_max_age_us",
        "resource_max_age_us", "protective_qualification_digest"), "DRY_PROFILE_EXPLICIT_MONITOR_REQUIRED")
    amendment = read_reference(inputs["guard_amendment"])
    require(amendment["schema"] == "MEME_LIVE_STEP11C_DRY_GUARD_AMENDMENT_V1"
        and amendment["scope"] == "IR-C2 fixed DRY deterministic qualification only"
        and amendment["status"] == "IMPLEMENTED_PENDING_PROJECT_REVIEW"
        and not amendment["project_acceptance_claimed"] and not amendment["production_capacity_claimed"]
        and _same(amendment["proposed_guard_limits"], {"HISTORY_BYTES":16384,"TOMBSTONE_ROWS":2}),
        "DRY_PROFILE_EXACT_AUTHORIZED_GUARD_AMENDMENT_REQUIRED")
    limits = dict(extension["limits"], **amendment["proposed_guard_limits"])
    require(_same(monitor["resource_limits"], limits), "DRY_PROFILE_REVIEWED_GUARD_LIMIT_CONFLICT")
    require(all(monitor[key] == original_monitor["policy"][key] for key in
        ("persistent_unknown_alert_us", "recovery_evidence_max_age_us"))
        and all(monitor[key] == original_monitor[key] for key in
        ("resource_max_age_us", "protective_qualification_digest")), "DRY_PROFILE_MONITOR_TIMING_CONFLICT")
    policy = DegradationPolicy("0"*64, monitor["persistent_unknown_alert_us"], monitor["recovery_evidence_max_age_us"],
        tuple(ResourceLimit(k,v) for k,v in sorted(monitor["resource_limits"].items())))
    config = MonitorConfiguration(paths["monitor"], policy, extension["host_identity_digest"], "0"*64,
        monitor["resource_max_age_us"], monitor["protective_qualification_digest"])
    # Include the exact accepted monitor path; never invent an exclusion.
    live_paths = tuple(target["paths"][k] for k in ("ledger", "producer", "operations", "evidence_store"))
    live_paths += (original_monitor["path"],)
    _distinct_dry_paths(tuple(paths.values()), live_paths, start["read_path"])
    arguments = dict(operations_path=paths["operations"], ledger_path=paths["ledger"], domain=domain,
        producer_path=paths["producer"], market_source=market, producer_profile=producer,
        source_path=paths["evidence_store"], source_binding=binding, source_profile=source_profile,
        database_identity=start["database_identity"], dry_live_paths=live_paths,
        expected_baseline_observation_digest=observation.content_digest, **original["dispatch"])
    identity = configured_identity(**arguments, producer_schema_digest=inputs["producer_schema_digest"],
        evidence_schema_digest=inputs["evidence_schema_digest"], restart_profile=restart,
        dry_monitor_digest=dry_monitor_fingerprint(config))
    reviewed = monitor_configuration_digest(identity, path=config.path, host_identity_digest=config.host_identity_digest,
        resource_max_age_us=config.resource_max_age_us, protective_qualification_digest=config.protective_qualification_digest)
    config = replace(config, policy=replace(policy, reviewed_configuration_digest=reviewed),
        runtime_code_digest=identity.runtime_code_digest)
    arguments.update(expected_identity=identity, degradation_config=config)
    driver = inputs["driver"]
    _keys(driver, ("endpoint", "public_rpc_profile", "facts_provider", "max_steps", "quote_policy",
        "plan_policy", "compute", "source_cut_semantics"), "DRY_PROFILE_EXPLICIT_PUBLIC_DRIVER_REQUIRED")
    endpoint = urlsplit(driver["endpoint"])
    require(endpoint.scheme == "https" and endpoint.hostname and not endpoint.username and not endpoint.password
        and not endpoint.query and not endpoint.fragment, "DRY_PROFILE_PUBLIC_ENDPOINT_REQUIRED")
    require(_same(driver["public_rpc_profile"], original["public_rpc_profile"]), "DRY_PROFILE_PUBLIC_RPC_CONFIG_CONFLICT")
    rpc_profile = PublicRpcProfile(**driver["public_rpc_profile"])
    require(rpc_profile.fingerprint == domain.expected_profile_fingerprint, "DRY_PROFILE_PUBLIC_PROVIDER_CONFLICT")
    require(driver["facts_provider"] == FACTS_PROVIDER and type(driver["max_steps"]) is int
        and 1 <= driver["max_steps"] <= 1000000 and driver["source_cut_semantics"] == "EXPLICIT_CURRENT_PUBLIC_UTC_CUT",
        "DRY_PROFILE_FINITE_PUBLIC_DRIVER_REQUIRED")
    QuotePolicyV01(**driver["quote_policy"])
    TransactionPlanPolicyV01(**driver["plan_policy"])
    ComputeBudget(**driver["compute"])
    record = {"schema":SCHEMA, "inputs":inputs, "domain":asdict(domain), "baseline_decision":asdict(decision),
        "source_identity":market.source_identity, "startup_identity":asdict(identity),
        "monitor_configuration":asdict(config), "capability":"NO_BROADCAST", "runtime_type":identity.runtime_type,
        "store_initialization":False, "grants_permission":False, "capital_authority":False}
    record["content_digest"] = content_fingerprint(record)
    return record, arguments


@dataclass(frozen=True, slots=True)
class SupportedDryProfile:
    record_json: str

    @property
    def record(self):
        return json.loads(self.record_json)

    def configuration(self):
        value = self.record
        rebuilt, arguments = _construct(value["inputs"])
        require(_same(value, rebuilt), "DRY_PROFILE_STALE_CODE_CONFIG_DOMAIN_OR_PATH")
        return arguments

    def start(self, *, process_identity, now_us, replace_generation=None):
        return start_dry(**self.configuration(), process_identity=process_identity, now_us=now_us,
            replace_generation=replace_generation)

    def driver(self, facts, *, public_transport=None):
        self.configuration()
        settings = self.record["inputs"]["driver"]
        return DryPublicInputDriver(settings["endpoint"], PublicRpcProfile(**settings["public_rpc_profile"]),
            BoundPublicFacts(self.record_json, facts), public_transport)

    def drive(self, started, facts, *, public_transport=None):
        return drive_steps(started, self.driver(facts, public_transport=public_transport),
            max_steps=self.record["inputs"]["driver"]["max_steps"])


@dataclass(frozen=True, slots=True)
class BoundPublicFacts:
    profile_json: str
    public_facts: object

    def __call__(self, started):
        profile = SupportedDryProfile(self.profile_json)
        configuration = profile.configuration()
        require(_same(asdict(started.audit.identity), asdict(configuration["expected_identity"])),
            "DRY_PROFILE_DIFFERENT_STARTED_RUNTIME")
        value = self.public_facts(started)
        require(type(value) is DryPublicFacts, "DRY_PROFILE_ORIGINAL_TYPED_FACTS_REQUIRED")
        driver = profile.record["inputs"]["driver"]
        require(_same(value.quote_policy.payload(), QuotePolicyV01(**driver["quote_policy"]).payload())
            and _same(value.plan_policy.payload(), TransactionPlanPolicyV01(**driver["plan_policy"]).payload())
            and _same(asdict(value.compute), driver["compute"]), "DRY_PROFILE_EXECUTION_CONFIG_CHANGED")
        ledger_utc(value.source_cut_utc)
        if value.wallet is not None:
            domain = configuration["domain"]
            require(value.wallet.observation.request.wallet == domain.wallet
                and value.wallet.observation.request.genesis_hash == domain.genesis_hash
                and value.wallet.observation.profile.fingerprint == domain.expected_profile_fingerprint,
                "DRY_PROFILE_PUBLIC_WALLET_BINDING_CONFLICT")
        return value


def build_profile(inputs):
    # Detach all mutable caller dictionaries before any identity is returned.
    record, _ = _construct(json.loads(canonical_json(inputs)))
    return SupportedDryProfile(canonical_json(record))


def load_profile(record):
    profile = SupportedDryProfile(canonical_json(record))
    profile.configuration()
    return profile
