"""Focused M56 deterministic/read-only validation with disposable tiny fixtures."""
from __future__ import annotations

import copy
import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from live import acceptance_dossier_v0_1 as d

CHECKS = []


def check(name, value):
    if not value:
        raise AssertionError(name)
    CHECKS.append(name)


def denied(name, fn, reason):
    try:
        fn()
    except d.DossierError as exc:
        check(name, reason in str(exc))
    else:
        raise AssertionError(name + ": accepted")


def main():
    with tempfile.TemporaryDirectory(prefix="m56-selftest-") as directory:
        root = Path(directory)
        (root / "src/live").mkdir(parents=True)
        (root / "scripts").mkdir()
        original = "src/live/original.py"
        (root / original).write_bytes(b"# accepted\n")
        overlay = "src/live/acceptance_dossier_v0_1.py"
        (root / overlay).write_bytes(b"# new\n")
        git = lambda _, command, *args: (d.BRANCH + "\n").encode() if command == "branch" else b""
        with patch.object(d, "_base_files", return_value={original: b"# accepted\n"}), patch.object(d, "_git", side_effect=git):
            first = d.source_freeze(root)
            check("deterministic_source", first == d.source_freeze(root))
            check("new_gate_overlay_bound", first["files"][overlay]["provenance"] == "STEP11C_OVERLAY")
            (root / overlay).write_bytes(b"# edited\n")
            check("overlay_change_changes_digest", first["content_digest"] != d.source_freeze(root)["content_digest"])
            (root / original).write_bytes(b"# modified accepted\n")
            denied("accepted_source_stale", lambda: d.source_freeze(root), "ACCEPTED_SOURCE_CHANGED")
            (root / original).write_bytes(b"# accepted\n")
            (root / "src/live/alternate.py").write_bytes(b"# alternate\n")
            denied("alternate_source_denied", lambda: d.source_freeze(root), "UNREVIEWED_ADDITIONAL_SOURCE")
            (root / "src/live/alternate.py").unlink()
            (root / original).unlink()
            denied("missing_production_source", lambda: d.source_freeze(root), "MISSING_ACCEPTED_SOURCE")
        child = root / "result.json"
        child.write_bytes(d.canonical_bytes({"result": "immutable proof"}))
        parent = root / "index.json"
        parent.write_bytes(d.canonical_bytes({"artifact": {"path": str(child), "sha256": d.sha256(child.read_bytes())}}))
        expected = d.sha256(parent.read_bytes())
        graph = d.EvidenceGraph(root)
        graph.add(parent, expected, parent="accepted")
        check("recursive_proof", len(graph.nodes) == 2)
        denied("conflicting_reference", lambda: graph.add(child, "f" * 64, parent="conflict"), "CONFLICTING_ARTIFACT_HASH")
        child.write_text("{}", encoding="utf-8")
        denied("stale_nested_proof", lambda: d.EvidenceGraph(root).add(parent, expected, parent="accepted"), "ARTIFACT_HASH_MISMATCH")
        child.unlink()
        denied("missing_nested_proof", lambda: d.EvidenceGraph(root).add(parent, expected, parent="accepted"), "MISSING_ARTIFACT")
        denied("runtime_store_never_opened", lambda: d.EvidenceGraph(root).add(root / "live.sqlite3", "a" * 64, parent="x"), "DATABASE_IS_NOT_A_DOSSIER_ARTIFACT")
        parent.write_bytes(b'{"x":1,"x":2}')
        denied("ambiguous_json_denied", lambda: d.read_json(parent), "DUPLICATE_JSON_KEY")
        denied("unsupported_schema", lambda: d.verify_dossier(root, {"schema": "v999"}), "UNSUPPORTED_DOSSIER")
    matrix = (ROOT / d.MATRIX_PATH).read_text(encoding="utf-8-sig")
    disposition = d.lifecycle_disposition(matrix)
    check("six_explicit_human_rows", disposition["human_external"] == list(d.HUMAN_ROWS))
    check("engineering_stays_pending", disposition["project_review_pending"] == ["M56", "M57"])
    changed = matrix.replace("| M56 | Full production composition and gate tooling | Frozen deterministic acceptance dossier | OWNED_NOT_BUILT |",
                             "| M56 | Full production composition and gate tooling | Frozen deterministic acceptance dossier | HUMAN_EXTERNAL |")
    denied("static_work_not_human", lambda: d.lifecycle_disposition(changed), "UNEXPECTED_LIFECYCLE_DISPOSITION:M56")
    snapshot = d.build_dossier(ROOT)
    check("real_frozen_revision", snapshot["source"]["accepted_revision"] == d.BASE_REVISION)
    check("all_accepted_transitions", len(snapshot["transition_coverage"]) == 17)
    check("no_capability_granted", not any(snapshot["capabilities"].values()))
    check("no_evidence_payload_duplication", all(set(r) == {"path", "sha256", "parents"} for r in snapshot["evidence"]))
    with patch.object(d, "build_dossier", return_value=snapshot):
        check("exact_freeze_verifies", d.verify_dossier(ROOT, copy.deepcopy(snapshot)) == snapshot)
        for field in ("source", "evidence", "deployment", "contracts", "lifecycle", "runtime_components"):
            bad = copy.deepcopy(snapshot)
            bad.pop(field)
            denied("missing_" + field, lambda: d.verify_dossier(ROOT, bad), "STALE_OR_CONFLICTING_DOSSIER")
        bad = copy.deepcopy(snapshot)
        bad["source"]["accepted_revision"] = "0" * 40
        denied("wrong_revision", lambda: d.verify_dossier(ROOT, bad), "STALE_OR_CONFLICTING_DOSSIER")
    print(json.dumps({"checks": CHECKS, "check_count": len(CHECKS),
                      "source_files": len(snapshot["source"]["files"]), "evidence_files": len(snapshot["evidence"]),
                      "status": "IMPLEMENTED_PENDING_PROJECT_REVIEW", "t010_executed": False,
                      "network_or_database_activity": False}, sort_keys=True))


if __name__ == "__main__":
    main()
