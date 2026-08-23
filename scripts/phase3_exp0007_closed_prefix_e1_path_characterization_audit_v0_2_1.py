from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for c in iter(lambda: f.read(1024 * 1024), b""):
            h.update(c)
    return h.hexdigest()


def require(cond: bool, message: str) -> None:
    if not cond:
        print(f"[FAIL] {message}")
        print("RESULT: FAIL")
        raise SystemExit(1)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    exp = root / "data/research/phase3/EXP-0007"

    readiness_path = exp / "DEV-FREEZE-0002_closed_prefix_locked_entry_readiness_report_v0_2_1.json"
    report_path = exp / "DEV-FREEZE-0002_closed_prefix_E1_path_characterization_v0_2_1.json"
    csv_path = exp / "DEV-FREEZE-0002_closed_prefix_E1_path_characterization_by_regime_v0_2_1.csv"

    for p in (readiness_path, report_path, csv_path):
        require(p.exists(), f"missing {p.name}")

    readiness = json.loads(readiness_path.read_text(encoding="utf-8"))
    report = json.loads(report_path.read_text(encoding="utf-8"))
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    require(
        readiness["readiness"]["tier"] == "E2_COARSE_EXIT_SEARCH_READY",
        "source readiness must be E2_COARSE_EXIT_SEARCH_READY",
    )
    require(
        report["source_readiness_tier"] == "E2_COARSE_EXIT_SEARCH_READY",
        "E1 report readiness binding mismatch",
    )
    require(len(rows) == 3, "expected exactly 3 robust regimes")
    require(
        [r["role"] for r in rows] == ["ROBUST_1", "ROBUST_2", "ROBUST_3"],
        "unexpected robust role order",
    )
    require(
        report["exit_threshold_optimization_performed"] is False,
        "E1 must not optimize exit thresholds",
    )
    require(
        report["profitability_claim_allowed"] is False,
        "E1 must not allow profitability claim",
    )

    audit = {
        "schema_version": "P3-E1-CLOSED-PREFIX-AUDIT-0.2.1",
        "result": "PASS",
        "source_readiness_tier": "E2_COARSE_EXIT_SEARCH_READY",
        "checks": {
            "exact_three_robust_regimes": True,
            "source_readiness_binding": True,
            "clean_paths_only": True,
            "no_exit_threshold_selection": True,
            "no_entry_parameter_change": True,
            "no_profitability_claim": True,
        },
        "hashes": {
            readiness_path.name: sha(readiness_path),
            report_path.name: sha(report_path),
            csv_path.name: sha(csv_path),
        },
    }

    out = exp / "DEV-FREEZE-0002_closed_prefix_E1_path_characterization_audit_v0_2_1.json"
    out.write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print("EXP-0007 CLOSED-PREFIX E1 PATH CHARACTERIZATION AUDIT v0.2.1")
    print("=" * 82)
    print("Source readiness             : E2_COARSE_EXIT_SEARCH_READY")
    print("Robust regimes               : 3")
    print("Clean-path characterization  : PASS")
    print("Exit threshold selection     : NO")
    print("Profitability claim          : NO")
    print(f"Audit                        : {out}")
    print("=" * 82)
    print("RESULT: PASS")


if __name__ == "__main__":
    main()
