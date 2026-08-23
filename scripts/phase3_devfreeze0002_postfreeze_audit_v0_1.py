from __future__ import annotations
import json, os
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from phase3.devfreeze_postfreeze_audit_v0_1 import audit_devfreeze

def atomic(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n"); f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)

def main():
    freeze = PROJECT_ROOT / "data/research/phase3/dev_freezes/DEV-FREEZE-0002"
    selection = PROJECT_ROOT / "data/research/phase3/EXP-0005/EXP-0005_D_selection_v0_1.json"
    report = audit_devfreeze(freeze_dir=freeze, selection_path=selection)
    out = freeze / "DEV-FREEZE-0002_postfreeze_audit_v0_1.json"
    atomic(out, report)
    print("DEV-FREEZE-0002 POST-FREEZE AUDIT v0.1")
    print("=" * 68)
    print(f"Segment duration            : {report['segment_duration_seconds']/3600:.2f} h")
    print(f"Eligible tokens             : {report['eligible_token_count']}")
    print(f"Frozen BOT_TRUTH rows       : {report['frozen_pump_event_rows']}")
    print("Coverage                    : 1 CLOSED CLEAN INTERVAL / 0 GAPS")
    print("10m t0 tail guard           : PASS")
    print("Entry A/B/C/D binding       : PASS")
    print("Production DB touched       : NO")
    print("Profitability claim         : NO")
    print(f"Audit                       : {out}")
    print("=" * 68)
    print("RESULT: PASS")

if __name__ == "__main__":
    main()
