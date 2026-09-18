"""T20 core acceptance harness: sequential, network-blocked, short output.

Runs the ~14-script core set (A01-A09 + S01/S02, S05, S06 BUY, E02) in
artifact-dependency order, one child process per script, using the codex
runtime interpreter with the accepted evidence environment's site-packages
(.codex_venv, in which all P1/P2 evidence was produced; the retained
evidence venv was lost to Temp cleanup) on PYTHONPATH. Fixes nothing;
records everything.

Parent:  python t20_core_harness_v0_1.py [--only S01_S02,A07] [--timeout 300]
Child:   python t20_core_harness_v0_1.py --child <script.py> [args...]

Never sets tempfile.tempdir (S04/S08, S05, S06 resolve retained evidence
through tempfile.gettempdir()). Never signs, never broadcasts, never
touches src/live.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / 'scripts'
CODEX_PYTHON = Path(r'C:\Users\Mari1\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe')
VENV_SITE = Path(r'D:\Tradingbot\solana_memecoin_bot_phase1_v0_1\.codex_venv\Lib\site-packages')
DEFAULT_EVIDENCE_ROOT = ROOT.parent / 'rescued_evidence' / 't20-core-harness'
DEFAULT_TIMEOUT = 300

# (label, script, extra argv, notes). Order is the artifact-dependency order:
# S01/S02 must precede S04/S08 (positive evidence), which precedes S05/S06
# (helper pins). Everything else is independent.
CORE = (
    ('A01', 'live_source_health_selftest_v0_1.py', (), ''),
    ('A02', 'live_wallet_evidence_selftest_v0_1.py', (), ''),
    ('A04', 'live_pump_protocol_compatibility_selftest_v0_1.py', (), ''),
    ('A05', 'live_runtime_composition_selftest_v0_1.py', (), 'shared c2 helper'),
    ('A03', 'live_runtime_acquisition_handoff_selftest_v0_1.py', (), 'untracked WIP'),
    ('A06', 'live_runtime_owned_dry_selftest_v0_1.py', (), ''),
    ('A07', 'live_operations_readiness_selftest_v0_1.py', (), 'B02 oracle at :141'),
    ('A08', 'live_operations_degradation_integration_selftest_v0_1.py', (), ''),
    ('S01_S02', 'live_step10_s01_s02_selftest_v0_1.py', (), 'B01; qualified helper'),
    ('A09_S04_S08', 'live_step10_s04_s08_selftest_v0_1.py', (), 'B01/B03; --positive-evidence chained from S01_S02 when it passes'),
    ('S05', 'live_step10_s05_selftest_v0_1.py', (), 'B03 pin'),
    ('S06_BUY', 'live_step10_s06_buy_recovery_selftest_v0_1.py', (), 'B03 pins S01/S04/S05'),
    ('E02', 'live_step10_e02_partial_selftest_v0_1.py', (), 'full run is a superset of --default-call-compat'),
)
# A10 (T19 integration) is not a repository script; it lives in the retained
# evidence tree and is intentionally out of scope here.


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def lf(data: bytes) -> bytes:
    return data.replace(b'\r\n', b'\n')


# ---------------------------------------------------------------- child ----
def child(argv):
    script, args = argv[0], argv[1:]
    sys.path[:0] = [str(ROOT / 'src'), str(SCRIPTS)]
    sys.dont_write_bytecode = True
    import runpy
    import socket
    from unittest.mock import patch
    import httpx

    def forbidden(*a, **k):
        raise AssertionError('T20_HARNESS_PUBLIC_NETWORK_FORBIDDEN')

    sys.argv = [script, *args]
    with patch.object(httpx.HTTPTransport, 'handle_request', forbidden), \
            patch.object(socket.socket, 'connect', forbidden):
        runpy.run_path(script, run_name='__main__')


# --------------------------------------------------------------- parent ----
def parse_checks(stdout: bytes):
    """Best-effort check count from per-line {"check":..} JSON or a final
    object carrying "checks" (dict or int) / "check_count"."""
    line_checks, final = {}, None
    for raw in stdout.splitlines():
        raw = raw.strip()
        if not raw.startswith(b'{'):
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        if 'check' in obj and 'result' in obj:
            line_checks[str(obj['check'])] = bool(obj['result'])
        if 'checks' in obj or 'check_count' in obj:
            final = obj
    count, all_true = None, None
    if final is not None:
        checks = final.get('checks')
        if isinstance(checks, dict):
            count, all_true = len(checks), all(bool(v) for v in checks.values())
        elif isinstance(checks, int):
            count = checks
            all_true = final.get('all_checks', final.get('all_checks_true'))
        if isinstance(final.get('check_count'), int):
            count = final['check_count']
            all_true = final.get('all_checks_true', all_true)
    if count is None and line_checks:
        count, all_true = len(line_checks), all(line_checks.values())
    return count, all_true


def whole_stdout_json(stdout: bytes):
    try:
        obj = json.loads(stdout.decode('utf-8-sig'))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def git(*args):
    try:
        return subprocess.run(['git', '-C', str(ROOT), *args], capture_output=True, timeout=30).stdout.decode(errors='replace').strip()
    except Exception as exc:  # git absent or slow: record, do not fail the run
        return f'<git unavailable: {exc}>'


def interpreter_identity(env):
    probe = 'import sys,sqlite3,json;print(json.dumps({"python":sys.version,"sqlite":sqlite3.sqlite_version,"executable":sys.executable}))'
    out = subprocess.run([str(CODEX_PYTHON), '-B', '-c', probe], capture_output=True, env=env, timeout=60)
    try:
        ident = json.loads(out.stdout.decode())
    except Exception:
        ident = {'error': out.stderr.decode(errors='replace')[-500:]}
    ident['python_sha256'] = sha256(CODEX_PYTHON.read_bytes())
    return ident


def run_one(label, script, extra, env, evidence, timeout):
    path = SCRIPTS / script
    cmd = [str(CODEX_PYTHON), '-B', str(Path(__file__).resolve()), '--child', str(path), *extra]
    out_path, err_path = evidence / f'{label}.stdout.txt', evidence / f'{label}.stderr.txt'
    started = time.perf_counter()
    timed_out = False
    with out_path.open('wb') as out, err_path.open('wb') as err:
        try:
            completed = subprocess.run(cmd, cwd=ROOT, stdout=out, stderr=err, env=env, timeout=timeout)
            code = completed.returncode
        except subprocess.TimeoutExpired:
            code, timed_out = 124, True
    seconds = time.perf_counter() - started
    stdout, stderr = out_path.read_bytes(), err_path.read_bytes()
    count, all_true = parse_checks(stdout)
    status = 'TIMEOUT' if timed_out else ('PASS' if code == 0 else 'FAIL')
    raw = path.read_bytes()
    return {
        'label': label, 'script': script, 'argv': cmd[3:], 'status': status, 'exit_code': code,
        'seconds': round(seconds, 3), 'timeout_seconds': timeout, 'timed_out': timed_out,
        'check_count': count, 'all_checks_true': all_true,
        'script_sha256_raw': sha256(raw), 'script_sha256_lf': sha256(lf(raw)),
        'stdout': str(out_path), 'stderr': str(err_path),
        'stdout_bytes': len(stdout), 'stderr_bytes': len(stderr),
        'stderr_tail': stderr.decode('utf-8', errors='replace').splitlines()[-15:],
    }


def main():
    if len(sys.argv) > 1 and sys.argv[1] == '--child':
        child(sys.argv[2:])
        return
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--evidence-root', type=Path, default=DEFAULT_EVIDENCE_ROOT)
    parser.add_argument('--run-id', default=None, help='default: UTC timestamp')
    parser.add_argument('--timeout', type=int, default=DEFAULT_TIMEOUT, help='seconds per script')
    parser.add_argument('--only', default=None, help='comma-separated labels, order preserved from CORE')
    parser.add_argument('--no-chain', action='store_true',
        help='do not pass this run\'s S01_S02 stdout to S04/S08 --positive-evidence even when it passes')
    parser.add_argument('--site-packages', type=Path, default=VENV_SITE,
        help='site-packages placed on PYTHONPATH; default is the accepted evidence environment')
    args = parser.parse_args()

    if not CODEX_PYTHON.is_file():
        sys.exit(f'interpreter missing: {CODEX_PYTHON}')
    site = args.site_packages.resolve()
    # A directory without __init__.py imports silently as a namespace package,
    # so check for real sources, not just the directory.
    for marker in ('httpx/__init__.py', 'solana/__init__.py', 'solders/solders.pyd', 'websockets/__init__.py'):
        if not (site / marker).is_file():
            sys.exit(f'site-packages unusable, missing {marker}: {site}')

    run_id = args.run_id or datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    evidence = args.evidence_root / run_id
    evidence.mkdir(parents=True, exist_ok=False)

    env = dict(os.environ)
    env['PYTHONPATH'] = str(site)
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    env.pop('PYTHONHOME', None)

    selected = CORE if not args.only else tuple(row for row in CORE if row[0] in set(args.only.split(',')))
    unknown = set(args.only.split(',')) - {row[0] for row in CORE} if args.only else set()
    if unknown:
        sys.exit(f'unknown labels: {sorted(unknown)}')

    summary = {
        'schema': 'T20_CORE_HARNESS_V0_1', 'run_id': run_id, 'evidence_dir': str(evidence),
        'started_utc': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'root': str(ROOT), 'head': git('rev-parse', 'HEAD'), 'branch': git('branch', '--show-current'),
        'git_status_lines': [l for l in git('status', '--porcelain').splitlines() if l],
        'interpreter': interpreter_identity(env), 'pythonpath': str(site),
        'site_packages_is_accepted_environment': site == VENV_SITE.resolve(),
        'network': 'httpx.HTTPTransport.handle_request and socket.socket.connect patched to raise',
        'tempfile_tempdir_overridden': False, 'timeout_seconds': args.timeout,
        'results': [], 'chained_positive_evidence': None,
    }
    print(f'T20 core harness  run={run_id}  head={summary["head"][:12]}  branch={summary["branch"]}  '
          f'wip={len(summary["git_status_lines"])}  evidence={evidence}')
    if not summary['site_packages_is_accepted_environment']:
        print(f'!! DEVIATION: site-packages substitute in use: {site}')
    print(f'{"label":12s} {"status":7s} {"secs":>7s} {"checks":>9s}  script')

    positive = None
    total = time.perf_counter()
    for label, script, extra, note in selected:
        extra = list(extra)
        if label == 'A09_S04_S08' and positive is not None:
            extra += ['--positive-evidence', str(positive)]
        result = run_one(label, script, extra, env, evidence, args.timeout)
        result['note'] = note
        if label == 'S01_S02' and result['status'] == 'PASS' and not args.no_chain:
            doc = whole_stdout_json(Path(result['stdout']).read_bytes())
            if doc and isinstance(doc.get('checks'), dict) and all(doc['checks'].values()):
                positive = evidence / 'S01_S02.stdout.json'
                positive.write_bytes(Path(result['stdout']).read_bytes())
                summary['chained_positive_evidence'] = str(positive)
        if label == 'A09_S04_S08':
            result['positive_evidence'] = str(positive) if positive else 'default (retained Temp evidence)'
        summary['results'].append(result)
        checks = '-' if result['check_count'] is None else (
            f'{result["check_count"]}{"" if result["all_checks_true"] else "!"}')
        print(f'{label:12s} {result["status"]:7s} {result["seconds"]:7.1f} {checks:>9s}  {script}', flush=True)
        if result['status'] != 'PASS':
            for line in result['stderr_tail']:
                print('    | ' + line[:200])
    summary['total_seconds'] = round(time.perf_counter() - total, 1)
    summary['finished_utc'] = datetime.now(timezone.utc).isoformat(timespec='seconds')
    counts = {s: sum(r['status'] == s for r in summary['results']) for s in ('PASS', 'FAIL', 'TIMEOUT')}
    summary['counts'] = counts
    (evidence / 'summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True), encoding='utf-8')
    print(f'-- {counts["PASS"]} PASS / {counts["FAIL"]} FAIL / {counts["TIMEOUT"]} TIMEOUT  '
          f'in {summary["total_seconds"]}s  -> {evidence / "summary.json"}')
    sys.exit(0 if counts['FAIL'] == 0 and counts['TIMEOUT'] == 0 else 1)


if __name__ == '__main__':
    main()
