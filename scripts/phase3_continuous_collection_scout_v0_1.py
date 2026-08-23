from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase2.data_coverage_root_cause_v0_1_1 import (
    nearest_observation_before_us,read_control_events,reconstruct_collector_coverage,us_to_iso
)
from phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01

def fmt(seconds):
    h=seconds/3600
    return f"{h:.2f} h"

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--production-db",default=str(PROJECT_ROOT/"data/db/tradingbot.sqlite3"))
    args=ap.parse_args()
    db=Path(args.production_db).resolve()
    if not db.exists():raise FileNotFoundError(db)

    conn=Phase1ReadOnlyAdapterV01.open_readonly(db)
    try:
        controls=read_control_events(conn)
        intervals,sessions,gaps,summary=reconstruct_collector_coverage(
            controls,
            nearest_before=lambda cutoff_us,after_us:nearest_observation_before_us(
                conn,cutoff_us,after_us=after_us
            )
        )
    finally:
        conn.close()

    usable=[i for i in intervals if i.end_us>i.start_us]
    latest=max(usable,key=lambda i:i.end_us) if usable else None
    longest=max(usable,key=lambda i:i.end_us-i.start_us) if usable else None

    print("PHASE 3 CONTINUOUS COLLECTION SCOUT v0.1")
    print("="*64)
    print(f"Collector control events       : {len(controls)}")
    print(f"Reconstructed sessions         : {len(sessions)}")
    print(f"Active intervals               : {len(intervals)}")
    print(f"Explicit gaps                  : {len(gaps)}")
    if latest:
        sec=(latest.end_us-latest.start_us)/1_000_000
        print(f"Latest active segment          : {us_to_iso(latest.start_us)} -> {us_to_iso(latest.end_us)}")
        print(f"Latest segment duration        : {fmt(sec)}")
        print(f"Latest close provisional       : {'YES' if latest.uncertain_close else 'NO'}")
    if longest:
        sec=(longest.end_us-longest.start_us)/1_000_000
        print(f"Longest observed active segment: {fmt(sec)}")
        print(f"6h milestone                   : {'YES' if sec>=6*3600 else 'NO'}")
        print(f"12h milestone                  : {'YES' if sec>=12*3600 else 'NO'}")
        print(f"24h milestone                  : {'YES' if sec>=24*3600 else 'NO'}")
    print("Milestones are informational only; they do NOT declare exit-data readiness.")
    print("Production DB opened           : READ-ONLY")
    print("Canonical freeze created       : NO")
    print("="*64)
    print("RESULT: PASS")

if __name__=="__main__":main()
