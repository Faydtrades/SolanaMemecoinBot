"""One Step11-C affected regression; disposable stores, no signing or network.

Reuses the existing shared LIVE admission/timer/reopen/ownership/startup and
finite supervisor checks. The new DRY qualification is included at freeze.
This is deliberately scoped to the changed Runtime/Operations interfaces.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "scripts")]

import httpx
from live.execution_signer_v0_1 import AutonomousLocalSigner
from live.execution_send_v0_1 import SolanaSendTransport
import live_runtime_composition_selftest_v0_1 as composition
import live_runtime_reconstruction_selftest_v0_1 as reconstruction
import live_operations_ownership_selftest_v0_1 as ownership
import live_operations_startup_selftest_v0_1 as startup
import live_operations_supervisor_selftest_v0_1 as supervisor
import live_runtime_owned_dry_selftest_v0_1 as dry
import live_runtime_dry_profile_selftest_v0_1 as profile


def forbidden(*args, **kwargs):
    raise AssertionError("STEP11C_REGRESSION_SIGN_SEND_OR_NETWORK_FORBIDDEN")


def install_guards():
    # Also installed when Windows spawn loads this file as __mp_main__.
    AutonomousLocalSigner.__init__ = forbidden
    AutonomousLocalSigner.sign_exact = forbidden
    SolanaSendTransport.__init__ = forbidden
    SolanaSendTransport._send_claimed = forbidden
    httpx.HTTPTransport.handle_request = forbidden


def main():
    selected = (
        ("composition", composition, composition.timer_fences),
        ("reconstruction", reconstruction, reconstruction.pending_and_consumed),
        ("ownership", ownership, ownership.controls),
        ("startup", startup, startup.normal),
        ("supervisor_observation", supervisor, supervisor.starting_observation),
        ("supervisor_recovery", supervisor, supervisor.recovery),
        ("owned_dry", dry, dry.qualification),
        ("dry_profile", profile, profile.qualification),
    )
    checks = {}
    with tempfile.TemporaryDirectory(prefix="step11c-affected-regression-") as tmp:
        for label, module, test in selected:
            directory = Path(tmp) / label
            directory.mkdir()
            module.CHECKS.clear()
            test(directory)
            checks.update({label + ":" + name: value for name, value in module.CHECKS.items()})
    print(json.dumps({"schema": "MEME_LIVE_STEP11C_AFFECTED_REGRESSION_V1",
        "checks": checks, "check_count": len(checks), "all_checks_true": all(checks.values()),
        "scope": "shared Runtime/Operations directly affected LIVE paths and continuous owned DRY",
        "signer_send_network_traps_installed_in_parent_and_spawned_children": True,
        "canonical_stores_used": False, "T010_executed": False}, sort_keys=True))


if __name__ in ("__main__", "__mp_main__"):
    install_guards()
if __name__ == "__main__":
    main()
