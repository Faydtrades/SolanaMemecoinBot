"""Native allocation containment only in a fresh disposable child process."""
from __future__ import annotations
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]


def child():
    from live.t010_resource_envelope_v0_1 import enforce_current_process_private_bytes
    from live_t010_joint_hot_measure_v0_3 import native_memory
    before = native_memory()
    limit = ((before['PrivateUsage']+8*1024*1024+4095)//4096)*4096
    verified = enforce_current_process_private_bytes(limit)
    within = bytearray(4*1024*1024)
    within[-1] = 1
    denied = False
    try:
        excess = bytearray(16*1024*1024)
    except MemoryError:
        denied = True
    assert denied and within[-1] == 1
    after = native_memory()
    assert after['PrivateUsage'] <= limit
    assert enforce_current_process_private_bytes(limit) == verified
    changed_denied = False
    try:
        enforce_current_process_private_bytes(limit+4096)
    except ValueError:
        changed_denied = True
    assert changed_denied
    print(json.dumps({'checks': 5, 'verified': verified, 'before': before, 'after': after,
        'above_limit_allocation': 'MemoryError', 'production_data': False}))


if __name__ == '__main__':
    if sys.argv[1:] == ['child']:
        child()
    else:
        result = subprocess.run([sys.executable, '-B', str(Path(__file__).resolve()), 'child'],
            cwd=ROOT, capture_output=True, text=True, timeout=60)
        print(result.stdout, end='')
        print(result.stderr, end='', file=sys.stderr)
        raise SystemExit(result.returncode)
