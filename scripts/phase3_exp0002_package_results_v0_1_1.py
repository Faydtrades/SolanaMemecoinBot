from pathlib import Path
import zipfile

def main():
    root=Path(__file__).resolve().parents[1]
    exp=root/"data/research/phase3/EXP-0002"
    names=[
        "EXP-0002_A_impulse_summary_v0_1_1.csv",
        "EXP-0002_A_impulse_frontier_v0_1_1.csv",
        "EXP-0002_A_impulse_candidate_outcomes_v0_1_1.csv",
        "EXP-0002_A_impulse_report_v0_1_1.json",
        "EXP-0002_manifest_v0_1_1.json",
        "EXP-0002_A_neighborhood_diagnostics_v0_1_1.csv",
        "EXP-0002_A_postrun_audit_v0_1_1.json",
    ]
    for n in names:
        if not (exp/n).exists():
            raise FileNotFoundError(exp/n)
    out=root/"EXP-0002_A_results_for_review_v0_1_1.zip"
    if out.exists(): out.unlink()
    with zipfile.ZipFile(out,"w",zipfile.ZIP_DEFLATED) as z:
        for n in names: z.write(exp/n,n)
    print(f"Created: {out}")
    print("RESULT: PASS")
if __name__=="__main__": main()
