from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_continuous_firstpullback_external_observability_run_v0_1 as accepted_obs  # noqa: E402
import phase4_continuous_firstpullback_multihour_run_v0_6 as accepted_ext  # noqa: E402


MODEL_ID = "P4-CONTINUOUS-FIRSTPULLBACK-EXTENDED-OBSERVABILITY-0001"

ACCEPTED_EXTENDED_RUNTIME_SHA256 = "8bf11f86f333220dd8e8617021123d99566186f9ba90b9afaaa7b3fd731caf61"

ACCEPTED_T021_WRAPPER_SHA256 = (
    "6f517b63220ae19b2edde282aea580ae42a18794c73291115a92a4e0b850518a"
)

ACCEPTED_T021_INTEGRATION_FINGERPRINT = (
    "6ac56972dd192e58f1c042768303fecb7a7e5d20d146ee3b7271190459d8fda4"
)

DEFAULT_REGISTRY_PATH = accepted_obs.DEFAULT_REGISTRY_PATH

INTEGRATION_SPEC = {
    "model_id": MODEL_ID,
    "accepted_extended_runtime_model_id": accepted_ext.MODEL_ID,
    "accepted_extended_runtime_fingerprint": accepted_ext.MODEL_FINGERPRINT,
    "accepted_extended_runtime_sha256": ACCEPTED_EXTENDED_RUNTIME_SHA256,
    "accepted_t021_model_id": accepted_obs.MODEL_ID,
    "accepted_t021_integration_fingerprint": (
        ACCEPTED_T021_INTEGRATION_FINGERPRINT
    ),
    "accepted_t021_wrapper_sha256": ACCEPTED_T021_WRAPPER_SHA256,
    "integration": (
        "T021_PASSIVE_OBSERVABILITY_OVER_EXTENDED_BOUNDED_RUNTIME"
    ),
    "terminal_finalization": (
        "INHERIT_ACCEPTED_T021_EXACT_RUN_ID_FINALIZER"
    ),
    "trading_runtime_semantics": "INHERIT_ACCEPTED_V05_UNCHANGED",
    "duration_control": "EXTENDED_BOUNDED_ONLY",
    "paper_only": True,
}

MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(
        INTEGRATION_SPEC,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()


def _validate_locked_contracts() -> None:
    accepted_ext._validate_locked_contracts_v06()

    file_sha256 = accepted_ext.accepted.file_sha256

    runtime_sha = file_sha256(Path(accepted_ext.__file__).resolve())
    if runtime_sha != ACCEPTED_EXTENDED_RUNTIME_SHA256:
        raise RuntimeError("accepted extended runtime harness changed")

    obs_sha = file_sha256(Path(accepted_obs.__file__).resolve())
    if obs_sha != ACCEPTED_T021_WRAPPER_SHA256:
        raise RuntimeError("accepted T021 observability wrapper changed")

    if (
        accepted_obs.MODEL_FINGERPRINT
        != ACCEPTED_T021_INTEGRATION_FINGERPRINT
    ):
        raise RuntimeError("accepted T021 integration fingerprint drift")


def run_multihour(
    *,
    duration_seconds: int,
    launch_collector: bool,
    registry_path: str | Path = DEFAULT_REGISTRY_PATH,
    **kwargs: Any,
):
    duration_seconds = accepted_ext.parse_duration_seconds(
        duration_seconds
    )

    _validate_locked_contracts()

    originals = {
        "accepted_v05": accepted_obs.accepted_v05,
        "_validate_locked_contracts": (
            accepted_obs._validate_locked_contracts
        ),
        "MODEL_ID": accepted_obs.MODEL_ID,
        "MODEL_FINGERPRINT": accepted_obs.MODEL_FINGERPRINT,
        "INTEGRATION_SPEC": accepted_obs.INTEGRATION_SPEC,
    }

    try:
        accepted_obs.accepted_v05 = accepted_ext
        accepted_obs._validate_locked_contracts = lambda: None
        accepted_obs.MODEL_ID = MODEL_ID
        accepted_obs.MODEL_FINGERPRINT = MODEL_FINGERPRINT
        accepted_obs.INTEGRATION_SPEC = INTEGRATION_SPEC

        return accepted_obs.run_multihour(
            duration_seconds=duration_seconds,
            launch_collector=launch_collector,
            registry_path=registry_path,
            **kwargs,
        )

    finally:
        accepted_obs.INTEGRATION_SPEC = originals[
            "INTEGRATION_SPEC"
        ]
        accepted_obs.MODEL_FINGERPRINT = originals[
            "MODEL_FINGERPRINT"
        ]
        accepted_obs.MODEL_ID = originals["MODEL_ID"]
        accepted_obs._validate_locked_contracts = originals[
            "_validate_locked_contracts"
        ]
        accepted_obs.accepted_v05 = originals[
            "accepted_v05"
        ]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = accepted_ext.build_arg_parser()

    parser.add_argument(
        "--observability-registry",
        type=Path,
        default=DEFAULT_REGISTRY_PATH,
        help=(
            "fixed producer-owned SQLite registry "
            "for read-only observers"
        ),
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.collector_child:
        return accepted_ext.accepted.run_collector_child()

    if args.collector_child_probe:
        return accepted_ext.accepted.run_collector_child_probe(
            args.collector_child_probe_ready
        )

    if (
        not args.use_existing_collector
        and not args.launch_managed_collector
    ):
        parser.error("one collector mode is required")

    try:
        duration_seconds = accepted_ext.resolve_duration_seconds(
            args.duration_seconds,
            args.duration_hours,
        )
    except ValueError as exc:
        parser.error(str(exc))

    code, summary = run_multihour(
        duration_seconds=duration_seconds,
        launch_collector=bool(
            args.launch_managed_collector
        ),
        registry_path=args.observability_registry,
    )

    external = summary.get(
        "external_observability",
        {},
    )

    if external.get("publication_errors"):
        print(
            "external observability errors: "
            + " | ".join(
                str(error)
                for error in external["publication_errors"]
            ),
            file=sys.stderr,
        )

    return code


if __name__ == "__main__":
    raise SystemExit(main())
