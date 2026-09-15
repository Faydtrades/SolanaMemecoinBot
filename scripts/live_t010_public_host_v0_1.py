"""Explicit public DRY host CLI. No launch is performed by inspect/status/stop."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]


def main(argv=None):
    from live import t010_public_host_v0_1 as host
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('inspect', 'launch', 'run'):
        item = sub.add_parser(name)
        item.add_argument('--request', required=True, type=Path)
        item.add_argument('--sha256', required=True)
        if name in ('launch', 'run'):
            item.add_argument('--execute-reviewed-t010', action='store_true', required=True)
        if name == 'run':
            item.add_argument('--launch-id', required=True)
    for name in ('status', 'stop', 'reconcile'):
        sub.add_parser(name).add_argument('--journal-root', required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command in ('status', 'stop', 'reconcile'):
            value = getattr(host, args.command)(args.journal_root)
        else:
            reference = {'path':str(args.request.resolve(strict=True)), 'sha256':args.sha256}
            request = host.read_reference(reference)
            if args.command == 'inspect':
                host.validate_request(request, ROOT, utc_us=time.time_ns()//1000)
                value = {'state':'STRUCTURAL_PREFLIGHT_ONLY', 'grants_permission':False,
                    'T010_executed':False, 'capability':'NO_BROADCAST'}
            elif args.command == 'launch':
                value = host.launch(reference, ROOT)
            else:
                return host.run(request, ROOT, launch_id=args.launch_id)
        print(json.dumps(value, sort_keys=True, allow_nan=False), flush=True)
        return 0
    except Exception as error:
        # No exception payloads, URLs or arbitrary provider data in health.
        code = str(error)
        if not code.startswith('T010_HOST_') or not code.replace('_', '').isalnum():
            code = 'T010_HOST_DENIED_' + type(error).__name__
        print(json.dumps({'state':'DENIED', 'reason':code, 'grants_permission':False}), flush=True)
        return 2


if __name__ == '__main__':
    result = main()
    sys.stdout.flush()
    sys.stderr.flush()
    # Named job/mutex handles intentionally survive until OS process exit.
    os._exit(result)
