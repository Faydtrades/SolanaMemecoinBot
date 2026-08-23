from pathlib import Path
import zipfile
def main():
    root=Path(__file__).resolve().parents[1];exp=root/"data/research/phase3/EXP-0003"
    names=[
        "EXP-0003_B_pullback_summary_v0_1.csv","EXP-0003_B_pullback_frontier_v0_1.csv",
        "EXP-0003_B_pullback_candidate_outcomes_v0_1.csv","EXP-0003_B_pullback_report_v0_1.json",
        "EXP-0003_manifest_v0_1.json","EXP-0003_B_neighborhood_diagnostics_v0_1.csv",
        "EXP-0003_B_crossA_diagnostics_v0_1.csv","EXP-0003_B_postrun_audit_v0_1.json",
    ]
    for n in names:
        if not (exp/n).exists():raise FileNotFoundError(exp/n)
    out=root/"EXP-0003_B_results_for_review_v0_1.zip"
    if out.exists():out.unlink()
    with zipfile.ZipFile(out,"w",zipfile.ZIP_DEFLATED) as z:
        for n in names:z.write(exp/n,n)
    print(f"Created: {out}");print("RESULT: PASS")
if __name__=="__main__":main()
