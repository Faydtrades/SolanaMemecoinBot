"""Offline M57 structural report. Exit 0 is structural readiness, never permission."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from live.acceptance_dossier_v0_1 import canonical_bytes, digest, read_json
from live.t010_preflight_v0_1 import preflight


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dossier", type=Path, required=True)
    parser.add_argument("--package", type=Path, help="Explicit public config/policy/grant/review package; absence denies")
    parser.add_argument("--at-utc", required=True, help="Explicit original UTC evaluation time; no implicit current-clock authority")
    parser.add_argument("--output", type=Path, required=True, help="New report path; exclusive create")
    args = parser.parse_args()
    try:
        report = preflight(ROOT, read_json(args.dossier),
                           None if args.package is None else read_json(args.package), evaluated_at_utc=args.at_utc)
        with args.output.open("xb") as stream:
            stream.write(canonical_bytes(report))
        print(json.dumps({"readiness": report["readiness"], "sha256": digest(report),
                          "structural_validation": report["structural_validation"]["state"],
                          "grants_permission": False, "report": str(args.output)}, sort_keys=True))
        return 0 if report["ready"] else 2
    except (ValueError, TypeError, KeyError, OSError) as exc:
        print(json.dumps({"readiness": "DENIED", "reason": str(exc), "grants_permission": False}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
