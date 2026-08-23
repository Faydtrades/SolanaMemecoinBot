from pathlib import Path
import sys
PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))
from phase3.locked_entry_readiness_v0_1 import run_locked_entry_readiness
EXPECTED={"CONTROL":230,"ROBUST_1":19,"ROBUST_2":22,"ROBUST_3":21}
def check(c,l):
    if not c:print(f"[FAIL] {l}");print("RESULT: FAIL");raise SystemExit(1)
    print(f"[PASS] {l}")
def main():
    freeze=PROJECT_ROOT/"data/research/phase2_research_freeze_v0_1";p3=PROJECT_ROOT/"data/research/phase3"
    result=run_locked_entry_readiness(
        freeze_db=freeze/"phase2_development_dataset_v0_1.sqlite3",
        freeze_manifest=freeze/"phase2_development_dataset_manifest_v0_1.json",
        coverage_snapshot=p3/"collector_coverage_DEV-FREEZE-0001_v0_1.json",
        selection_path=p3/"EXP-0005/EXP-0005_D_selection_v0_1.json")
    print("EXP-0007 DEV-FREEZE-0001 LOCKED-ENTRY READINESS REGRESSION v0.1");print("="*74)
    by={r["role"]:r for r in result["regimes"]}
    for role,n in EXPECTED.items():check(by[role]["candidate_count"]==n,f"{role} candidates reproduce {n}")
    robust=[by[r]["clean_5m_count"] for r in ("ROBUST_1","ROBUST_2","ROBUST_3")]
    check(robust==[0,0,0],"robust clean 5m reproduces EXP-0006 [0,0,0]")
    check(result["readiness"]["tier"]=="NOT_READY","DEV-FREEZE-0001 readiness remains NOT_READY")
    print(f"Readiness tier         : {result['readiness']['tier']}")
    print(f"Robust clean 5m        : {robust}")
    print("Production DB touched  : NO");print("Profitability claim    : NO")
    print("="*74);print("RESULT: PASS")
if __name__=="__main__":main()
