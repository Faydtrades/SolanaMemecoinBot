"""Read-only bounded clock discovery. Never writes an approved profile or policy."""
from __future__ import annotations
import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from live.runtime_public_clock_v0_1 import NtpProvider, observe_provider
from live.acceptance_dossier_v0_1 import canonical_bytes, read_json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=int, required=True)
    parser.add_argument("--interval-seconds", type=int, required=True)
    args = parser.parse_args()
    if not 1 <= args.samples <= 6 or not 1 <= args.interval_seconds <= 10:
        raise ValueError("CLOCK_DISCOVERY_FINITE_BOUND_REQUIRED")
    # Reserve output first: no network side effect if an existing path is given.
    with args.output.open("xb") as stream:
        provider = NtpProvider(**read_json(args.provider))
        observations = []
        for index in range(args.samples):
            try:
                observation = observe_provider(provider)
                # OS UTC is a diagnostic offset comparator, never clock truth.
                observation["diagnostic_local_wall_us"] = time.time_ns() // 1000
                observations.append(observation)
            except ValueError as exc:
                observations.append({"unavailable": str(exc)})
            if index + 1 < args.samples:
                time.sleep(args.interval_seconds)
        result = {"schema": "MEME_LIVE_PUBLIC_CLOCK_DISCOVERY_V1",
            "status": "PROVIDER_CANDIDATE_NOT_APPROVED", "provider": asdict(provider),
            "provider_fingerprint": provider.fingerprint, "observations": observations,
            "clock_policy_approved": False, "sets_system_time": False, "T010_executed": False}
        stream.write(canonical_bytes(result))
    print(json.dumps({"output": str(args.output.resolve()), "observations": len(observations),
        "successful": sum("unavailable" not in row for row in observations), "clock_policy_approved": False}))


if __name__ == "__main__":
    main()
