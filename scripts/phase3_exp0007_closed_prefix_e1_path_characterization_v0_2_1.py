from __future__ import annotations

import csv
import json
import os
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from phase3.e1_path_characterization_v0_1 import characterize_readiness_report


def atomic(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def main() -> None:
    exp = PROJECT_ROOT / "data/research/phase3/EXP-0007"
    source = exp / "DEV-FREEZE-0002_closed_prefix_locked_entry_readiness_report_v0_2_1.json"
    if not source.exists():
        raise FileNotFoundError(source)

    readiness = json.loads(source.read_text(encoding="utf-8"))
    result = characterize_readiness_report(readiness)

    rp = exp / "DEV-FREEZE-0002_closed_prefix_E1_path_characterization_v0_2_1.json"
    cp = exp / "DEV-FREEZE-0002_closed_prefix_E1_path_characterization_by_regime_v0_2_1.csv"
    atomic(rp, result)

    rows = []
    for r in result["robust_regimes"]:
        row = {
            "role": r["role"],
            "parameter_set_id": r["parameter_set_id"],
            "candidate_count": r["candidate_count"],
            "clean_5m_count": r["clean_5m_count"],
            "clean_5m_rate": r["clean_5m_rate"],
        }
        for name in (
            "clean_path_fresh_point_count",
            "clean_path_future_observation_count",
            "clean_path_peak_bps",
            "clean_path_trough_bps",
        ):
            for k, v in r[name].items():
                row[f"{name}_{k}"] = v
        rows.append(row)

    with cp.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print("EXP-0007 / DEV-FREEZE-0002 CLOSED-PREFIX E1 PATH CHARACTERIZATION v0.2.1")
    print("=" * 88)
    print(f"Source readiness tier       : {result['source_readiness_tier']}")
    for r in result["robust_regimes"]:
        print(
            f"{r['role']:8s} "
            f"clean5m={r['clean_5m_count']:4d} "
            f"peak_p50={r['clean_path_peak_bps']['p50']} bps "
            f"trough_p50={r['clean_path_trough_bps']['p50']} bps "
            f"fresh_points_p50={r['clean_path_fresh_point_count']['p50']}"
        )
    print("Exit thresholds selected    : NO")
    print("Entry parameters changed    : NO")
    print("Profitability claim         : NO")
    print(f"Report                      : {rp}")
    print(f"Summary                     : {cp}")
    print("=" * 88)
    print("RESULT: PASS")


if __name__ == "__main__":
    main()
