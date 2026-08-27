from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_continuous_firstpullback_multihour_run_v0_5 as accepted_v05  # noqa: E402


accepted = accepted_v05.accepted
accepted_v04 = accepted_v05.accepted_v04
ContinuousFirstPullbackBindingV05 = accepted_v05.ContinuousFirstPullbackBindingV05
RUNTIME_DIR = accepted_v05.RUNTIME_DIR
PRODUCTION_DB = accepted_v05.PRODUCTION_DB

MODEL_ID = "P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0006"
ACCEPTED_V05_HARNESS_SHA256 = (
    "22d4c0fa40e53e62fb305c75a832f6a633b496a8f10382d75353444e1392db37"
)
ACCEPTED_V05_MODEL_FINGERPRINT = (
    "0308899e0a2306c921603f3beff4a957e01832ce180a397b88841ba498fab5e4"
)

DEFAULT_DURATION_SECONDS = accepted_v05.DEFAULT_DURATION_SECONDS
MIN_DURATION_SECONDS = 3_600
MAX_DURATION_SECONDS = 604_800
REFERENCE_DURATION_SECONDS = (43_200, 86_400, 172_800)

MULTIHOUR_SPEC = json.loads(json.dumps(accepted_v05.MULTIHOUR_SPEC))
duration_policy = dict(MULTIHOUR_SPEC.get("duration_policy", {}))
duration_policy.update({
    "default_seconds": DEFAULT_DURATION_SECONDS,
    "minimum_seconds": MIN_DURATION_SECONDS,
    "maximum_seconds": MAX_DURATION_SECONDS,
    "reference_extended_seconds": list(REFERENCE_DURATION_SECONDS),
    "mode": "BOUNDED_EXTENDED_DURATION",
    "continuous_unbounded": False,
    "trading_runtime_semantics": "INHERIT_ACCEPTED_V05_UNCHANGED",
})
MULTIHOUR_SPEC.update({
    "model_id": MODEL_ID,
    "accepted_multihour_v0_5_sha256": ACCEPTED_V05_HARNESS_SHA256,
    "accepted_multihour_v0_5_fingerprint": ACCEPTED_V05_MODEL_FINGERPRINT,
    "duration_policy": duration_policy,
    "extended_duration_only": True,
})
MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(MULTIHOUR_SPEC, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()

run_control_reason = accepted_v05.run_control_reason


def parse_duration_seconds(value: str | int) -> int:
    try:
        duration = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("duration-seconds must be an integer") from exc

    if not MIN_DURATION_SECONDS <= duration <= MAX_DURATION_SECONDS:
        raise argparse.ArgumentTypeError(
            f"duration-seconds must be in {MIN_DURATION_SECONDS}..{MAX_DURATION_SECONDS}"
        )

    return duration


def parse_duration_hours(value: str | int) -> int:
    try:
        hours = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("duration-hours must be an integer") from exc

    try:
        return parse_duration_seconds(hours * 3600)
    except argparse.ArgumentTypeError as exc:
        raise argparse.ArgumentTypeError(
            f"duration-hours must be in {MIN_DURATION_SECONDS // 3600}.."
            f"{MAX_DURATION_SECONDS // 3600}"
        ) from exc


def resolve_duration_seconds(
    duration_seconds: int | None,
    duration_hours_seconds: int | None,
) -> int:
    if duration_seconds is not None and duration_hours_seconds is not None:
        raise ValueError("use only one of --duration-seconds or --duration-hours")

    if duration_hours_seconds is not None:
        return parse_duration_seconds(duration_hours_seconds)

    if duration_seconds is not None:
        return parse_duration_seconds(duration_seconds)

    return DEFAULT_DURATION_SECONDS


def _validate_locked_contracts_v06() -> None:
    accepted_v05._validate_locked_contracts_v05()

    actual_sha = accepted.file_sha256(Path(accepted_v05.__file__).resolve())

    if actual_sha != ACCEPTED_V05_HARNESS_SHA256:
        raise RuntimeError(
            f"accepted V0.5 harness changed: {actual_sha} != "
            f"{ACCEPTED_V05_HARNESS_SHA256}"
        )

    if accepted_v05.MODEL_FINGERPRINT != ACCEPTED_V05_MODEL_FINGERPRINT:
        raise RuntimeError("accepted V0.5 model fingerprint drift")


def run_multihour(*, duration_seconds: int, launch_collector: bool):
    duration_seconds = parse_duration_seconds(duration_seconds)
    _validate_locked_contracts_v06()

    base = accepted_v05.accepted

    v05_originals = {
        "MODEL_ID": accepted_v05.MODEL_ID,
        "MODEL_FINGERPRINT": accepted_v05.MODEL_FINGERPRINT,
        "MULTIHOUR_SPEC": accepted_v05.MULTIHOUR_SPEC,
        "DEFAULT_DURATION_SECONDS": accepted_v05.DEFAULT_DURATION_SECONDS,
        "MIN_DURATION_SECONDS": accepted_v05.MIN_DURATION_SECONDS,
        "MAX_DURATION_SECONDS": accepted_v05.MAX_DURATION_SECONDS,
        "ContinuousFirstPullbackBindingV05": (
            accepted_v05.ContinuousFirstPullbackBindingV05
        ),
    }

    base_originals = {
        "DEFAULT_DURATION_SECONDS": base.DEFAULT_DURATION_SECONDS,
        "MIN_DURATION_SECONDS": base.MIN_DURATION_SECONDS,
        "MAX_DURATION_SECONDS": base.MAX_DURATION_SECONDS,
    }

    try:
        accepted_v05.MODEL_ID = MODEL_ID
        accepted_v05.MODEL_FINGERPRINT = MODEL_FINGERPRINT
        accepted_v05.MULTIHOUR_SPEC = MULTIHOUR_SPEC
        accepted_v05.DEFAULT_DURATION_SECONDS = DEFAULT_DURATION_SECONDS
        accepted_v05.MIN_DURATION_SECONDS = MIN_DURATION_SECONDS
        accepted_v05.MAX_DURATION_SECONDS = MAX_DURATION_SECONDS
        accepted_v05.ContinuousFirstPullbackBindingV05 = (
            ContinuousFirstPullbackBindingV05
        )

        base.DEFAULT_DURATION_SECONDS = DEFAULT_DURATION_SECONDS
        base.MIN_DURATION_SECONDS = MIN_DURATION_SECONDS
        base.MAX_DURATION_SECONDS = MAX_DURATION_SECONDS

        return accepted_v05.run_multihour(
            duration_seconds=duration_seconds,
            launch_collector=launch_collector,
        )

    finally:
        base.DEFAULT_DURATION_SECONDS = base_originals["DEFAULT_DURATION_SECONDS"]
        base.MIN_DURATION_SECONDS = base_originals["MIN_DURATION_SECONDS"]
        base.MAX_DURATION_SECONDS = base_originals["MAX_DURATION_SECONDS"]

        accepted_v05.ContinuousFirstPullbackBindingV05 = v05_originals[
            "ContinuousFirstPullbackBindingV05"
        ]
        accepted_v05.MAX_DURATION_SECONDS = v05_originals["MAX_DURATION_SECONDS"]
        accepted_v05.MIN_DURATION_SECONDS = v05_originals["MIN_DURATION_SECONDS"]
        accepted_v05.DEFAULT_DURATION_SECONDS = v05_originals[
            "DEFAULT_DURATION_SECONDS"
        ]
        accepted_v05.MULTIHOUR_SPEC = v05_originals["MULTIHOUR_SPEC"]
        accepted_v05.MODEL_FINGERPRINT = v05_originals["MODEL_FINGERPRINT"]
        accepted_v05.MODEL_ID = v05_originals["MODEL_ID"]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = accepted_v05.build_arg_parser()

    duration_action = next(
        action
        for action in parser._actions
        if action.dest == "duration_seconds"
    )

    duration_action.type = parse_duration_seconds
    duration_action.default = None
    duration_action.help = (
        f"Extended bounded duration in seconds "
        f"({MIN_DURATION_SECONDS}..{MAX_DURATION_SECONDS})."
    )

    parser.add_argument(
        "--duration-hours",
        type=parse_duration_hours,
        default=None,
        help=(
            f"Convenience form for integer hours "
            f"({MIN_DURATION_SECONDS // 3600}..{MAX_DURATION_SECONDS // 3600}); "
            "mutually exclusive with --duration-seconds."
        ),
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.collector_child:
        return accepted.run_collector_child()

    if args.collector_child_probe:
        return accepted.run_collector_child_probe(
            args.collector_child_probe_ready
        )

    if not args.use_existing_collector and not args.launch_managed_collector:
        parser.error("one collector mode is required")

    try:
        duration_seconds = resolve_duration_seconds(
            args.duration_seconds,
            args.duration_hours,
        )
    except ValueError as exc:
        parser.error(str(exc))

    code, _summary = run_multihour(
        duration_seconds=duration_seconds,
        launch_collector=bool(args.launch_managed_collector),
    )

    return code


if __name__ == "__main__":
    raise SystemExit(main())
