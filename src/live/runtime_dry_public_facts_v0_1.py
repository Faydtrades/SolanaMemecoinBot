"""Current public observations for the reviewed DRY qualification boundary.

The clock uses a separately reviewed real public provider and exact policy.
OS wall time never becomes a qualified clock. No RPC endpoint is evidence.
"""
from __future__ import annotations

import json
import shutil
from collections import deque
from dataclasses import asdict
from pathlib import Path

from phase5.shadow_domain_v0_1 import content_fingerprint
from phase5.shadow_venue_route_quote_v0_1 import QuotePolicyV01, TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID, WSOL_MINT
from phase5.shadow_unsigned_plan_simulation_v0_1 import TransactionPlanPolicyV01, derive_associated_token_address
from .authority_controls_v0_1 import require, utc_from_us
from .execution_message_v0_1 import ComputeBudget
from .ledger_actions_v0_1 import utc_microseconds
from .ledger_domain_v0_1 import adjudicate_opening_baseline
from .ledger_evidence_codec_v0_1 import wallet_observation_to_json
from .ledger_settlement_v0_1 import WalletSupportInput
from .public_rpc_v0_1 import PublicReadOnlyRpc, PublicRpcProfile
from .wallet_evidence_v0_1 import WalletEvidenceAdapter, WalletEvidenceRequest, ExpectedTokenAccount, ledger_account_evidence
from .runtime_composition_v0_1 import EntryFacts, capture_source_at_completion
from .runtime_dry_public_driver_v0_1 import DryPublicFacts
from .runtime_dry_profile_v0_1 import read_reference, _keys
from .source_health_v0_1 import CollectorSourceAdapter
from .operations_degradation_monitor_v0_1 import HostMetric, HostObservations, ERRORS as HOST_OBSERVATION_ERRORS

from .runtime_public_clock_v0_1 import RealPublicClock
from .pump_protocol_compatibility_v0_1 import PumpProtocolObservation
from phase5.shadow_venue_route_quote_v0_1 import derive_bonding_curve_pda, derive_pumpswap_pool_pda


def windows_working_set_bytes():
    """Current process RSS via the native read-only Win32 process API."""
    import ctypes
    from ctypes import wintypes as w
    import os
    require(os.name == "nt", "PUBLIC_NATIVE_WINDOWS_RSS_REQUIRED")
    class Counters(ctypes.Structure):
        _fields_ = [("cb", w.DWORD), ("PageFaultCount", w.DWORD)] + [
            (key, ctypes.c_size_t) for key in ("PeakWorkingSetSize", "WorkingSetSize",
             "QuotaPeakPagedPoolUsage", "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage",
             "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")]
    api = ctypes.WinDLL("kernel32", use_last_error=True)
    api.GetCurrentProcess.restype = w.HANDLE
    api.K32GetProcessMemoryInfo.argtypes = [w.HANDLE, ctypes.POINTER(Counters), w.DWORD]
    api.K32GetProcessMemoryInfo.restype = w.BOOL
    counters = Counters()
    counters.cb = ctypes.sizeof(counters)
    require(api.K32GetProcessMemoryInfo(api.GetCurrentProcess(), ctypes.byref(counters), counters.cb),
        "PUBLIC_NATIVE_WINDOWS_RSS_UNAVAILABLE")
    require(counters.WorkingSetSize > 0, "PUBLIC_NATIVE_WINDOWS_RSS_INVALID")
    return int(counters.WorkingSetSize)


class ReviewedPublicFacts:
    """Only external facts; original Runtime chooses candidates and actions.

    Supplying a fake HTTP boundary explicitly marks the observations TEST_ONLY.
    Public CLI construction has no transport/callback substitution option.
    """
    def __init__(self, profile, settings, *, test_transport=None, test_clock_observer=None):
        self.profile = profile
        self.config = profile.configuration()
        _keys(settings, ("clock_profile", "protective_timing", "venue", "token_program",
            "minimum_context_slot", "source_lag_us"), "PUBLIC_FACTS_EXPLICIT_SETTINGS_REQUIRED")
        require(settings["venue"] in ("PUMP", "PUMPSWAP") and settings["token_program"] in
            (TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID), "PUBLIC_FACTS_ROUTE_REQUIRED")
        require(type(settings["minimum_context_slot"]) is int and settings["minimum_context_slot"] >=
            self.config["domain"].minimum_context_slot and type(settings["source_lag_us"]) is int
            and 0 <= settings["source_lag_us"] <= 1000000, "PUBLIC_FACTS_CURRENT_CUT_REQUIRED")
        self.settings = json.loads(json.dumps(settings))
        self.transport = test_transport
        self.scope = "TEST_ONLY_FAKE_EXTERNAL_TRANSPORT" if test_transport is not None or test_clock_observer is not None else "PUBLIC_ENVIRONMENT_REVIEWED"
        self._baseline_observation = None
        self._recent_wallet_observations = deque(maxlen=4)
        self._recent_source_observations = deque(maxlen=4)
        self._recent_clock_observations = deque(maxlen=4)
        self.startup_us = None
        self.public_clock = RealPublicClock(settings["clock_profile"],
            self.config["degradation_config"].host_identity_digest, test_observer=test_clock_observer)

    def begin_cold_reopen(self):
        """New clock instance for an explicitly closed/reopened Runtime only.

        Keep the reviewed provider/policy and all retained observations. The
        first sample against the reopened Ledger must reconcile its exact
        durable checkpoint; ordinary steps never renew their clock epoch.
        """
        self.public_clock = RealPublicClock(self.settings["clock_profile"],
            self.config["degradation_config"].host_identity_digest,
            test_observer=self.public_clock.test_observer)
        self.startup_us = None

    def clock(self, started=None):
        sample = self.public_clock.sample(started)
        self._recent_clock_observations.append({"kind":"CLOCK", "scope":self.scope,
            "clock_profile":self.settings["clock_profile"], "sample":asdict(sample),
            "observation":dict(self.public_clock.last_observation), "content_digest":sample.content_digest})
        return sample

    @property
    def observations(self):
        return ([] if self._baseline_observation is None else [self._baseline_observation]) + list(
            self._recent_source_observations) + list(self._recent_clock_observations) + list(self._recent_wallet_observations)

    def wallet(self, started=None, *, minimum_context_slot=None):
        domain = self.config["domain"]
        settings = self.profile.record["inputs"]["driver"]
        expected = ()
        candidate_only = False
        floor = self.settings["minimum_context_slot"]
        if minimum_context_slot is not None:
            require(type(minimum_context_slot) is int and minimum_context_slot >= 0,
                "PUBLIC_WALLET_EXPLICIT_VENUE_FLOOR_REQUIRED")
            floor = max(floor, minimum_context_slot)
        if started is not None:
            runtime = started.runtime
            floor = max(floor, runtime.ledger.baseline().observation.anchor.slot)
            action = None if runtime._entry_action_id is None else runtime.ledger.action(runtime._entry_action_id)
            candidate = None if not runtime.queued_roots else runtime.ledger.candidate(runtime.queued_roots[0])
            mint = action.mint if action is not None else None if candidate is None else candidate.mint
            if mint is not None:
                program = self.settings["token_program"] if action is None else action.token_program
                candidate_only = action is None
                if candidate_only:
                    with PublicReadOnlyRpc(settings["endpoint"], PublicRpcProfile(**settings["public_rpc_profile"]),
                            transport=self.transport) as rpc:
                        current = rpc.get_multiple_accounts((mint,), min_context_slot=floor).accounts[0]
                    require(current is not None, "PUBLIC_CANDIDATE_MINT_UNAVAILABLE")
                    # This only locates the expected ATA. The complete original
                    # wallet and protocol observations independently validate it.
                    if current.account.owner in (TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID):
                        program = current.account.owner
                expected = (ExpectedTokenAccount(derive_associated_token_address(domain.wallet, mint, program), mint, program),
                    ExpectedTokenAccount(derive_associated_token_address(domain.wallet, WSOL_MINT, TOKEN_PROGRAM_ID), WSOL_MINT, TOKEN_PROGRAM_ID))
        request = WalletEvidenceRequest(domain.wallet, domain.genesis_hash, floor, expected)
        with PublicReadOnlyRpc(settings["endpoint"], PublicRpcProfile(**settings["public_rpc_profile"]),
                transport=self.transport) as rpc:
            observation = WalletEvidenceAdapter(rpc, request, clock=lambda:self.clock().utc_upper_utc,
                concurrent_cohort=True, retry_min_context=True).observe()
        now = self.clock().utc_upper_utc
        decision = None
        if started is None:
            decision = adjudicate_opening_baseline(domain, observation, evaluated_at_utc=now,
                required_min_context_slot=floor)
            require(decision.disposition == "ESTABLISHED", "PUBLIC_CURRENT_WALLET_BASELINE_CONFLICT")
        else:
            support = ledger_account_evidence(observation, expected_wallet=domain.wallet,
                expected_genesis=domain.genesis_hash, expected_profile_fingerprint=domain.expected_profile_fingerprint,
                required_min_context_slot=floor, now_utc=now)
            require(support.account_facts_usable or candidate_only,
                "PUBLIC_CURRENT_WALLET_SUPPORT_UNAVAILABLE")
        record = {"kind":"WALLET", "scope":self.scope,
            "observation_json":wallet_observation_to_json(observation), "decision":None if decision is None else asdict(decision),
            "evaluated_at_utc":now, "required_min_context_slot":floor,
            "content_digest":observation.content_digest}
        if started is None:
            self._baseline_observation = record
        elif support.account_facts_usable:
            self._recent_wallet_observations.append(record)
        return WalletSupportInput(observation, now, floor)

    def protocol(self, started, wallet):
        root = started.runtime.queued_roots[0]
        mint = started.runtime.ledger.candidate(root).mint
        domain = self.config["domain"]
        settings = self.profile.record["inputs"]["driver"]
        require(wallet.observation.anchor is not None, "PUBLIC_CANDIDATE_FINALIZED_CONTEXT_REQUIRED")
        with PublicReadOnlyRpc(settings["endpoint"], PublicRpcProfile(**settings["public_rpc_profile"]),
                transport=self.transport) as rpc:
            genesis_start = rpc.get_genesis_hash()
            read = rpc.get_multiple_accounts((mint, derive_bonding_curve_pda(mint), derive_pumpswap_pool_pda(mint)),
                min_context_slot=wallet.observation.anchor.slot)
            anchor = rpc.get_finalized_block_anchor(read.context.slot)
            genesis_end = rpc.get_genesis_hash()
        return PumpProtocolObservation(mint, wallet.digest, genesis_start, genesis_end,
            domain.expected_profile_fingerprint, read, anchor, self.clock(started).utc_lower_utc)

    def source(self, *, max_raw_bytes=None, reserve_rows=None, reserve_verdict=None):
        sample = self.clock()
        cut = utc_from_us(utc_microseconds(sample.utc_lower_utc)-self.settings["source_lag_us"])
        _, verdict = capture_source_at_completion(CollectorSourceAdapter(self.config["market_source"].db_path),
            self.config["source_binding"], self.config["source_profile"], sample, self.clock, cut=cut,
            max_raw_bytes=max_raw_bytes, reserve_rows=reserve_rows)
        require(reserve_verdict is None or reserve_verdict(verdict), "PUBLIC_SOURCE_RECORD_CAPACITY_HELD")
        self._recent_source_observations.append({"kind":"SOURCE", "scope":self.scope, "verdict":asdict(verdict),
            "content_digest":verdict.content_digest})
        require(verdict.disposition == "HEALTHY", "PUBLIC_RETAINED_SOURCE_NOT_HEALTHY_NO_REPAIR")
        return verdict

    def resources(self, started):
        config = self.config["degradation_config"]
        stamp = utc_microseconds(self.clock(started).utc_upper_utc)
        timing = read_reference(self.settings["protective_timing"])
        require(timing["schema"] == "MEME_LIVE_REVIEWED_PROTECTIVE_TIMING_V1"
            and timing["host_identity_digest"] == config.host_identity_digest
            and timing["runtime_code_digest"] == config.runtime_code_digest
            and timing["qualification_digest"] == config.protective_qualification_digest
            and timing["configuration_digest"] == self.profile.record["startup_identity"]["configuration_digest"],
            "PUBLIC_PROTECTIVE_TIMING_BINDING_CONFLICT")
        values = {"HOST_RSS_BYTES":windows_working_set_bytes(),
            "HOST_DISK_RESERVE_BYTES":shutil.disk_usage(Path(config.path).parent).free,
            "STARTUP_US":self.startup_us, "PROTECTIVE_STEP_US":timing["protective_step_us"]}
        require(self.startup_us is not None, "PUBLIC_ACTUAL_STARTUP_MEASUREMENT_REQUIRED")
        metrics = tuple(HostMetric(key, value, stamp, config.protective_qualification_digest if key == "PROTECTIVE_STEP_US"
            else content_fingerprint({"kind":key, "value":value, "observed_us":stamp})) for key,value in values.items())
        return HostObservations(config.host_identity_digest, config.policy.reviewed_configuration_digest,
            content_fingerprint(asdict(started.runtime._ownership.fence)), metrics)

    def __call__(self, started):
        self.clock(started)
        wallet = None if started.runtime._dry_recovery else self.wallet(started)
        protocol = None if wallet is None or not started.runtime.queued_roots else self.protocol(started, wallet)
        driver = self.profile.record["inputs"]["driver"]
        sample = self.clock(started)
        # The monitor compares host evidence with the clock sample supplied to
        # that observation. Capture before publishing that sample; sampling a
        # new clock inside its resources callback would be future evidence.
        # Each Runtime clock call refreshes the snapshot. The original monitor
        # still rejects a stale, future, missing or differently owned snapshot.
        resource_snapshot, resource_error = [None], [None]
        def runtime_clock():
            resource_snapshot[0], resource_error[0] = None, None
            try:
                resource_snapshot[0] = self.resources(started)
            except HOST_OBSERVATION_ERRORS as error:
                # Preserve the monitor's UNKNOWN handling and original recovery
                # priority. A resource failure must never replace clock failure.
                resource_error[0] = error
            return self.clock(started)
        def runtime_resources():
            if resource_error[0] is not None:
                raise resource_error[0]
            require(resource_snapshot[0] is not None, "PUBLIC_RESOURCE_CLOCK_ORDER_REQUIRED")
            return resource_snapshot[0]
        return DryPublicFacts(runtime_clock, None if wallet is None or not started.runtime.queued_roots else
            EntryFacts(self.settings["venue"], self.settings["token_program"], wallet, protocol), wallet,
            QuotePolicyV01(**driver["quote_policy"]), TransactionPlanPolicyV01(**driver["plan_policy"]),
            ComputeBudget(**driver["compute"]), lambda:utc_microseconds(self.clock(started).utc_upper_utc),
            runtime_resources,
            utc_from_us(utc_microseconds(sample.utc_lower_utc)-self.settings["source_lag_us"]),
            lambda floor:self.wallet(started, minimum_context_slot=floor))
