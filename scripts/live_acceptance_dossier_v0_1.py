"""Create or verify the read-only M56 freeze; does not execute a runtime."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from live.acceptance_dossier_v0_1 import (DossierError, build_dossier, canonical_bytes,
                                        digest, read_json, verify_dossier)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--output", type=Path, help="New manifest path; existing files are never overwritten")
    action.add_argument("--verify", type=Path, help="Verify existing manifest against exact source and retained proof")
    parser.add_argument("--dry-profile", type=Path, help="Exact supported DRY profile JSON; requires qualification")
    parser.add_argument("--dry-qualification", type=Path, help="Exact same-profile qualification JSON")
    args = parser.parse_args()
    try:
        if args.verify:
            dossier = verify_dossier(ROOT, read_json(args.verify))
        else:
            import hashlib
            def reference(path):
                return None if path is None else {"path":str(path.resolve()),"sha256":hashlib.sha256(path.read_bytes()).hexdigest()}
            dossier = build_dossier(ROOT, dry_profile=reference(args.dry_profile), dry_qualification=reference(args.dry_qualification))
            with args.output.open("xb") as stream:
                stream.write(canonical_bytes(dossier))
        print(json.dumps({"result": "CONSISTENT_PENDING_PROJECT_REVIEW", "sha256": digest(dossier),
                          "evidence_files": len(dossier["evidence"]),
                          "source_files": len(dossier["source"]["files"]),
                          "t010_executed": False, "grants_permission": False}, sort_keys=True))
        return 0
    except (DossierError, OSError, KeyError, TypeError) as exc:
        print(json.dumps({"result": "DENIED", "reason": str(exc), "grants_permission": False}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
