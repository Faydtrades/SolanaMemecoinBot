from pathlib import Path
import zipfile

def main():
    root = Path(__file__).resolve().parents[1]
    exp = root / "data/research/phase3/EXP-0007"
    freeze = root / "data/research/phase3/dev_freezes/DEV-FREEZE-0002"
    required = [
        freeze / "DEV-FREEZE-0002_postfreeze_audit_v0_1.json",
        exp / "DEV-FREEZE-0002_locked_entry_readiness_report_v0_1.json",
        exp / "DEV-FREEZE-0002_locked_entry_readiness_by_regime_v0_1.csv",
    ]
    optional = [
        exp / "DEV-FREEZE-0002_E1_path_characterization_v0_1.json",
        exp / "DEV-FREEZE-0002_E1_path_characterization_by_regime_v0_1.csv",
        exp / "DEV-FREEZE-0002_E1_path_characterization_audit_v0_1.json",
    ]
    for p in required:
        if not p.exists():
            raise FileNotFoundError(p)
    include = required + [p for p in optional if p.exists()]
    out = root / "EXP-0007_DEV-FREEZE-0002_readiness_results_for_review_v0_1.zip"
    if out.exists():
        out.unlink()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        for p in include:
            z.write(p, p.name)
    print(f"Created: {out}")
    print("RESULT: PASS")

if __name__ == "__main__":
    main()
