from __future__ import annotations

import csv
import hashlib
import json
import statistics
from pathlib import Path
from typing import Any

RETURNS = [500, 1500, 2000, 3000, 5000, 5500, 12500]
TRADES = [2, 3, 5, 7, 24]
BUYERS = [1, 2, 3, 5, 9]

FOUNDATION_DOWNSTREAM = {
    "pullback": {"min_depth_bps": 1000, "max_depth_bps": 9000},
    "buyer_response": {
        "window_id": "3s",
        "min_rebound_bps": 200,
        "min_buys": 1,
        "min_net_flow_reserve_ppm": 0,
    },
    "reclaim": {"min_extension_from_response_bps": 200},
    "runaway": {"max_extension_from_response_bps": 10000},
}

def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def read_csv(path: Path):
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))

def require(cond: bool, msg: str):
    if not cond:
        print(f"[FAIL] {msg}")
        print("RESULT: FAIL")
        raise SystemExit(1)

def as_int(v):
    return None if v in (None, "") else int(v)

def as_bool(v):
    return str(v).lower() == "true"

def med(vals):
    return int(statistics.median(vals)) if vals else None

def neighbor_keys(r, t, b):
    ri, ti, bi = RETURNS.index(r), TRADES.index(t), BUYERS.index(b)
    out = []
    for di,dj,dk in ((1,0,0),(-1,0,0),(0,1,0),(0,-1,0),(0,0,1),(0,0,-1)):
        a,c,d = ri+di, ti+dj, bi+dk
        if 0 <= a < len(RETURNS) and 0 <= c < len(TRADES) and 0 <= d < len(BUYERS):
            out.append((RETURNS[a],TRADES[c],BUYERS[d]))
    return out

def main():
    root = Path(__file__).resolve().parents[1]
    exp = root / "data/research/phase3/EXP-0002"
    summary_p = exp / "EXP-0002_A_impulse_summary_v0_1_1.csv"
    frontier_p = exp / "EXP-0002_A_impulse_frontier_v0_1_1.csv"
    long_p = exp / "EXP-0002_A_impulse_candidate_outcomes_v0_1_1.csv"
    report_p = exp / "EXP-0002_A_impulse_report_v0_1_1.json"
    manifest_p = exp / "EXP-0002_manifest_v0_1_1.json"

    print("EXP-0002 BLOCK A POST-RUN AUDIT v0.1.1")
    print("=" * 62)
    for p in (summary_p,frontier_p,long_p,report_p,manifest_p):
        require(p.exists(), f"missing {p}")

    summary = read_csv(summary_p)
    frontier = read_csv(frontier_p)
    longs = read_csv(long_p)
    report = json.loads(report_p.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_p.read_text(encoding="utf-8"))

    require(report.get("experiment_id") == "EXP-0002", "experiment id")
    require(report.get("block") == "A_IMPULSE", "block id")
    require(report.get("dataset",{}).get("freeze_id") == "DEV-FREEZE-0001", "freeze binding")
    require(report.get("dataset",{}).get("untouched_oos") is False, "OOS label")
    design = report.get("experiment_design",{})
    require(design.get("combination_count") == 175, "175 combinations")
    require(design.get("full_cartesian_13_23m_run") is False, "Cartesian prohibition")
    require(design.get("single_winner_selected") is False, "winner preselection")
    require(design.get("fixed_downstream_foundation_anchors") == FOUNDATION_DOWNSTREAM, "B/C/D changed")
    corr = report.get("harness_correction",{})
    require(corr.get("reject_state_counted") is True, "REJECT correction absent")
    require(corr.get("per_combination_atomic_checkpointing") is True, "checkpointing absent")
    require(report.get("determinism",{}).get("pass") is True, "determinism")

    claims = report.get("performance_claims",{})
    require(claims.get("exit_optimization_performed") is False, "exit optimization")
    require(claims.get("realistic_net_pnl_available") is False, "net pnl")
    require(claims.get("profitability_claim_allowed") is False, "profitability claim")

    require(len(summary) == 175, f"summary rows {len(summary)}")
    require(len({r["parameter_set_id"] for r in summary}) == 175, "duplicate parameter sets")
    require(sorted(int(r["combo_index"]) for r in summary) == list(range(1,176)), "combo indices")
    grid = {(int(r["min_return_bps"]),int(r["min_trades_since_t0"]),int(r["min_unique_buyers_since_t0"])) for r in summary}
    require(grid == {(a,b,c) for a in RETURNS for b in TRADES for c in BUYERS}, "locked grid mismatch")

    by_id = {}
    for r in summary:
        run = int(r["run_count"])
        cand = int(r["candidate_count"])
        state = int(r["candidate_state_count"])
        accounted = (
            int(r["expired_count"])
            + int(r["invalidated_count"])
            + int(r["rejected_count"])
            + int(r["trade_count"])
            + state
        )
        require(run == 3901, f"{r['parameter_set_id']} run_count")
        require(cand == state, f"{r['parameter_set_id']} candidate lifecycle")
        require(int(r["accounted_run_count"]) == run == accounted, f"{r['parameter_set_id']} terminal accounting")
        by_id[r["parameter_set_id"]] = r

    long_counts = {}
    for r in longs:
        pid = r["parameter_set_id"]
        require(pid in by_id, f"unknown long-row pid {pid}")
        long_counts[pid] = long_counts.get(pid,0) + 1
    for pid,r in by_id.items():
        require(long_counts.get(pid,0) == int(r["candidate_count"]), f"{pid} long-row count")

    require(len(longs) == int(report["result_counts"]["candidate_outcome_rows"]), "long total")
    require(len(frontier) == int(report["result_counts"]["diagnostic_frontier_rows"]), "frontier total")
    flagged = {r["parameter_set_id"] for r in summary if as_bool(r.get("diagnostic_pareto_frontier"))}
    require(flagged == {r["parameter_set_id"] for r in frontier}, "frontier consistency")

    by_key = {(int(r["min_return_bps"]),int(r["min_trades_since_t0"]),int(r["min_unique_buyers_since_t0"])):r for r in summary}
    diag = []
    for key,row in sorted(by_key.items()):
        ns = [by_key[k] for k in neighbor_keys(*key)]
        n15 = [as_int(x.get("h15_p50_bps")) for x in ns]
        n30 = [as_int(x.get("h30_p50_bps")) for x in ns]
        n15 = [x for x in n15 if x is not None]
        n30 = [x for x in n30 if x is not None]
        diag.append({
            "parameter_set_id": row["parameter_set_id"],
            "min_return_bps": key[0],
            "min_trades_since_t0": key[1],
            "min_unique_buyers_since_t0": key[2],
            "candidate_count": int(row["candidate_count"]),
            "rejected_count": int(row["rejected_count"]),
            "h15_observable_n": int(row["h15_observable_n"]),
            "h15_p50_bps": as_int(row.get("h15_p50_bps")),
            "h30_observable_n": int(row["h30_observable_n"]),
            "h30_p50_bps": as_int(row.get("h30_p50_bps")),
            "diagnostic_pareto_frontier": as_bool(row.get("diagnostic_pareto_frontier")),
            "neighbor_count": len(ns),
            "neighbors_with_15s_median": len(n15),
            "neighbor_15s_p50_median_bps": med(n15),
            "neighbor_15s_p50_min_bps": min(n15) if n15 else None,
            "neighbor_15s_p50_max_bps": max(n15) if n15 else None,
            "neighbors_with_30s_median": len(n30),
            "neighbor_30s_p50_median_bps": med(n30),
            "neighbor_30s_p50_min_bps": min(n30) if n30 else None,
            "neighbor_30s_p50_max_bps": max(n30) if n30 else None,
        })

    diag_p = exp / "EXP-0002_A_neighborhood_diagnostics_v0_1_1.csv"
    with diag_p.open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(diag[0].keys())); w.writeheader(); w.writerows(diag)

    audit = {
        "schema_version":"P3-EXP0002-POSTAUDIT-0.1.1",
        "experiment_id":"EXP-0002",
        "result":"PASS",
        "checks":{
            "exact_175_grid":True,
            "terminal_reject_accounting":True,
            "terminal_lifecycle_complete":True,
            "candidate_rows_match":True,
            "B_C_D_unchanged":True,
            "frontier_consistent":True,
            "determinism":True,
            "no_winner_selected":True,
            "no_exit_optimization":True,
            "no_profitability_claim":True,
        },
        "counts":{
            "summary_rows":len(summary),
            "candidate_outcome_rows":len(longs),
            "frontier_rows":len(frontier),
        },
        "hashes":{p.name:file_sha256(p) for p in (summary_p,frontier_p,long_p,report_p,manifest_p,diag_p)},
        "note":"Read-only result audit except creation of diagnostics/audit artifacts; no strategy/dataset/registry mutation.",
    }
    audit_p = exp / "EXP-0002_A_postrun_audit_v0_1_1.json"
    audit_p.write_text(json.dumps(audit,indent=2,sort_keys=True)+"\n",encoding="utf-8")

    print(f"Summary rows             : {len(summary)}")
    print(f"Candidate outcome rows   : {len(longs)}")
    print(f"Frontier rows            : {len(frontier)}")
    print("REJECT accounting        : PASS")
    print("Terminal lifecycle       : PASS")
    print("B/C/D unchanged          : PASS")
    print("Winner selected          : NO")
    print("Profitability claim      : NO")
    print(f"Diagnostics              : {diag_p}")
    print(f"Audit                    : {audit_p}")
    print("=" * 62)
    print("RESULT: PASS")

if __name__ == "__main__":
    main()
