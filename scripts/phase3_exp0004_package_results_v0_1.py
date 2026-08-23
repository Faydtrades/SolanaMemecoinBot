from pathlib import Path
import zipfile
def main():
    root=Path(__file__).resolve().parents[1];exp=root/"data/research/phase3/EXP-0004"
    names=[
        "EXP-0004_C_buyer_response_summary_v0_1.csv",
        "EXP-0004_C_buyer_response_frontier_v0_1.csv",
        "EXP-0004_C_buyer_response_candidate_outcomes_v0_1.csv",
        "EXP-0004_C_buyer_response_report_v0_1.json",
        "EXP-0004_manifest_v0_1.json",
        "EXP-0004_C_neighborhood_diagnostics_v0_1.csv",
        "EXP-0004_C_crossAB_diagnostics_v0_1.csv",
        "EXP-0004_C_postrun_audit_v0_1.json",
    ]
    for n in names:
        if not (exp/n).exists():raise FileNotFoundError(exp/n)
    out=root/"EXP-0004_C_results_for_review_v0_1.zip"
    if out.exists():out.unlink()
    with zipfile.ZipFile(out,"w",zipfile.ZIP_DEFLATED) as z:
        for n in names:z.write(exp/n,n)
    print(f"Created: {out}");print("RESULT: PASS")
if __name__=="__main__":main()
