"""Finite public-input driver for the original owned NO_BROADCAST Runtime.

The host supplies reviewed external wallet/clock/route facts and resource
observations. This driver does not install policy, pick candidates or actions,
construct economic decisions, recover stores, or grant capital authority.
Each step owns a fresh bounded public read session, closed even on interruption.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass

from .authority_controls_v0_1 import require
from .execution_readonly_v0_1 import ExecutionReadOnlyRpc
from .runtime_composition_v0_1 import DryExecutionPorts, EntryFacts
from .operations_startup_v0_1 import StartedRuntime, DRY_CAPABILITY

VERSION = "live_runtime_dry_public_driver_v0.1"


@dataclass(frozen=True, slots=True)
class DryPublicFacts:
    clock: object
    entry: EntryFacts | None
    wallet: object
    quote_policy: object
    plan_policy: object
    compute: object
    now_us: object
    operations_resources: object
    source_cut_utc: str | None = None

    def __post_init__(self):
        require(callable(self.clock) and callable(self.now_us)
            and (self.entry is None or type(self.entry) is EntryFacts),
            "DRY_DRIVER_ORIGINAL_PUBLIC_FACTS_REQUIRED")


@dataclass(frozen=True, slots=True)
class DryPublicInputDriver:
    endpoint: str
    profile: object
    facts: object
    # Tests may supply a mock public HTTP boundary; never an execution port.
    public_transport: object = None

    def __post_init__(self):
        require(callable(self.facts), "DRY_DRIVER_PUBLIC_FACTS_PROVIDER_REQUIRED")

    @contextmanager
    def __call__(self, started):
        require(type(started) is StartedRuntime
            and started.audit.identity.capability == DRY_CAPABILITY
            and started.runtime.capability == "NO_BROADCAST"
            and self.profile.fingerprint == started.runtime.ledger.domain.expected_profile_fingerprint,
            "DRY_DRIVER_FIXED_OWNED_CAPABILITY_REQUIRED")
        facts = self.facts(started)
        require(type(facts) is DryPublicFacts, "DRY_DRIVER_ORIGINAL_PUBLIC_FACTS_REQUIRED")
        with ExecutionReadOnlyRpc(self.endpoint, self.profile, now_us=facts.now_us,
                transport=self.public_transport) as rpc:
            yield dict(clock=facts.clock, entry=facts.entry,
                execution=DryExecutionPorts(rpc, facts.wallet, facts.quote_policy,
                    facts.plan_policy, facts.compute, facts.now_us),
                operations_resources=facts.operations_resources, source_cut_utc=facts.source_cut_utc)


def drive_steps(started, driver, *, max_steps):
    """Explicit finite host budget; original Runtime alone selects every unit."""
    require(type(driver) is DryPublicInputDriver and type(max_steps) is int
        and 1 <= max_steps <= 1000000, "DRY_DRIVER_FINITE_BUDGET_REQUIRED")
    results = []
    for _ in range(max_steps):
        with driver(started) as inputs:
            result = started.runtime.step(**inputs)
        results.append(result)
        if result.work == "OPERATIONS_HELD":
            break
    return tuple(results)
