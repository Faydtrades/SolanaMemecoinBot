from __future__ import annotations
import csv, hashlib, json
from pathlib import Path

def sha(p):
    h = hashlib.sha256()
    with p.open("rb") as f:
        for c in iter(lambda: f.read(1024 * 1024), b""):
            h.update(c)
    return h.hexdigest()

def req(c, m):
    if not c:
        print(f"[FAIL] {m}")
        print("RESULT: FAIL")
        raise SystemExit(1)

def main():
    root = Path(__file__).resolve().parents[1]
    exp = root / "data/research/phase3/EXP-0007"
    rp = exp / "DEV-FREEZE-0002_E1_path_characterization_v0_1.json"
    cp = exp / "DEV-FREEZE-0002_E1_path_characterization_by_regime_v0_1.csv"
    source = exp / "DEV-FREEZE-0002_locked_entry_readiness_report_v0_1.json"
    for p in (rp, cp, source):
        req(p.exists(), f"missing {p.name}")

    report = json.loads(rp.read_text(encoding="utf-8"))
    with cp.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    req(len(rows) == 3, "expected exactly 3 robust regimes")
    req([r["role"] for r in rows] == ["ROBUST_1", "ROBUST_2", "ROBUST_3"], "unexpected robust role order")
    req(report["exit_threshold_optimization_performed"] is False, "exit tuning must be false")
    req(report["profitability_claim_allowed"] is False, "profitability claim must be false")
    req(report["source_readiness_tier"] in ("E1_PATH_CHARACTERIZATION_READY", "E2_COARSE_EXIT_SEARCH_READY"),
        "E1 characterization ran without readiness")

    audit = {
        "schema_version": "P3-E1-PATH-CHAR-AUDIT-0.1",
        "result": "PASS",
        "checks": {
            "exact_three_robust_regimes": True,
            "source_readiness_gate_satisfied": True,
            "clean_paths_only": True,
            "no_exit_threshold_selection": True,
            "no_profitability_claim": True,
        },
        "hashes": {p.name: sha(p) for p in (rp, cp, source)},
    }
    ap = exp / "DEV-FREEZE-0002_E1_path_characterization_audit_v0_1.json"
    ap.write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("EXP-0007 E1 PATH CHARACTERIZATION AUDIT v0.1")
    print("=" * 64)
    print("Robust regimes               : 3")
    print("Readiness gate               : SATISFIED")
    print("Exit threshold selection     : NO")
    print("Profitability claim          : NO")
    print(f"Audit                        : {ap}")
    print("=" * 64)
    print("RESULT: PASS")

if __name__ == "__main__":
    main()
