"""Build/verify an explicit supported DRY profile offline; never start a runtime."""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from live.acceptance_dossier_v0_1 import read_json,canonical_bytes
from live.runtime_dry_profile_v0_1 import build_profile,load_profile


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    group=parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--inputs',type=Path)
    group.add_argument('--verify',type=Path)
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    try:
        if args.verify:profile=load_profile(read_json(args.verify))
        else:
            if args.output is None:raise ValueError('EXPLICIT_NEW_PROFILE_OUTPUT_REQUIRED')
            profile=build_profile(read_json(args.inputs))
            with args.output.open('xb') as stream:stream.write(canonical_bytes(profile.record))
        print(json.dumps({'content_digest':profile.record['content_digest'],'grants_permission':False,
            'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','runtime_started':False},sort_keys=True))
        return 0
    except (ValueError,TypeError,KeyError,OSError) as exc:
        print(json.dumps({'result':'DENIED','reason':str(exc),'grants_permission':False},sort_keys=True))
        return 2


if __name__=='__main__':raise SystemExit(main())
