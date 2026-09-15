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
        old_source = d.ACCEPTED_DEVELOPMENT_CHECKOUT / original
        parent.write_bytes(d.canonical_bytes({"source": {"path": str(old_source), "sha256": "a" * 64}}))
        with patch.object(d, "_base_files", return_value={original: b"# accepted\n"}):
            relocated = d.EvidenceGraph(root)
            relocated.add(parent, d.sha256(parent.read_bytes()), parent="accepted")
            check("historical_checkout_source_is_provenance_not_current_file",
                  relocated.historical_sources == [{"parent": str(parent), "path": str(root / original),
                      "original_reference_path": str(old_source), "sha256": "a" * 64}]
                  and len(relocated.nodes) == 1)
        with patch.object(d, "_base_files", return_value={}):
            denied("unknown_historical_source_not_relocated",
                   lambda: d.EvidenceGraph(root).add(parent, d.sha256(parent.read_bytes()), parent="accepted"),
                   "UNKNOWN_HISTORICAL_SOURCE_REFERENCE")
        denied("unsupported_schema", lambda: d.verify_dossier(root, {"schema": "v999"}), "UNSUPPORTED_DOSSIER")
    matrix = (ROOT / d.MATRIX_PATH).read_text(encoding="utf-8-sig")
    disposition = d.lifecycle_disposition(matrix)
    check("six_explicit_human_rows", disposition["human_external"] == list(d.HUMAN_ROWS))
    check("current_matrix_exact_verified_rows", {row for row, state in disposition["authoritative_rows"].items()
        if state == "VERIFIED"} == {f"M{n:02}" for n in range(1, 62)} - set(d.HUMAN_ROWS)
        - set(disposition["reopened_engineering_rows"]))
    check("exact_dossier_review_still_required", disposition["exact_dossier_project_review_required"] == ["M56", "M57"])
    check("tooling_does_not_promote", disposition["tooling_promotes_rows"] is False)

    def replace_state(row, state):
        lines = matrix.splitlines()
        position = next(i for i, line in enumerate(lines) if line.startswith("| " + row + " |"))
        cells = lines[position].split("|")
        cells[4] = " " + state + " "
        lines[position] = "|".join(cells)
        return "\n".join(lines)

    for row in ("M56", "M57"):
        reopened = d.lifecycle_disposition(replace_state(row, "OWNED_NOT_BUILT"))
        check(row + "_truthful_reopened_state", row in reopened["reopened_engineering_rows"]
              and reopened["authoritative_rows"][row] == "OWNED_NOT_BUILT"
              and not reopened["tooling_promotes_rows"])
        for state in ("HUMAN_EXTERNAL", "BLOCKED"):
            denied(row + "_rejects_" + state, lambda: d.lifecycle_disposition(replace_state(row, state)),
                "UNEXPECTED_LIFECYCLE_DISPOSITION:" + row)
    for row in d.HUMAN_ROWS:
        denied(row + "_cannot_be_promoted", lambda: d.lifecycle_disposition(replace_state(row, "VERIFIED")),
            "UNEXPECTED_LIFECYCLE_DISPOSITION:" + row)
    denied("other_engineering_cannot_be_deferred", lambda: d.lifecycle_disposition(replace_state("M01", "HUMAN_EXTERNAL")),
        "UNEXPECTED_LIFECYCLE_DISPOSITION:M01")
    row = next(line for line in matrix.splitlines() if line.startswith("| M56 |"))
    denied("missing_lifecycle_row", lambda: d.lifecycle_disposition(matrix.replace(row, "")), "INCOMPLETE_LIFECYCLE")
    denied("duplicate_lifecycle_row", lambda: d.lifecycle_disposition(matrix + "\n" + row), "DUPLICATE_LIFECYCLE_ROW")
    step11b = d.read_json(ROOT / d.INDEX_PATHS[2])
    check("historical_step11b_remainder_preserved", step11b["final_project_acceptance"]["remaining_owned_not_built"]
        == list(d.STEP11B_REMAINING_OWNED_ROWS) == ["M56", "M57"])
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
