from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_continuous_firstpullback_extended_observability_run_v0_1 as ext_obs  # noqa: E402
import phase4_continuous_firstpullback_external_observability_run_v0_1 as t021  # noqa: E402
import phase4_continuous_firstpullback_multihour_run_v0_5 as v05  # noqa: E402
import phase4_continuous_firstpullback_multihour_run_v0_6 as v06  # noqa: E402


EXPECTED_V05_SHA256 = (
    "22d4c0fa40e53e62fb305c75a832f6a633b496a8f10382d75353444e1392db37"
)

EXPECTED_T021_SHA256 = (
    "6f517b63220ae19b2edde282aea580ae42a18794c73291115a92a4e0b850518a"
)

EXPECTED_V06_SHA256 = "8bf11f86f333220dd8e8617021123d99566186f9ba90b9afaaa7b3fd731caf61"
EXPECTED_EXT_OBS_SHA256 = "5f7bcfa1de4216f1e939a59295ec26d93fda968632ee1cec1de95232ece595d2"


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def expect_raises(fn, exc_type) -> bool:
    try:
        fn()
    except exc_type:
        return True
    return False


def main() -> int:
    checks = []

    protected_before = {
        "v05": sha256_file(
            Path(v05.__file__).resolve()
        ),
        "t021": sha256_file(
            Path(t021.__file__).resolve()
        ),
    }

    checks.append((
        "1_ACCEPTED_BASELINES_EXACT",
        protected_before["v05"] == EXPECTED_V05_SHA256
        and protected_before["t021"] == EXPECTED_T021_SHA256,
    ))

    checks.append((
        "2_EXTENDED_FILES_EXACT",
        sha256_file(
            Path(v06.__file__).resolve()
        ) == EXPECTED_V06_SHA256
        and sha256_file(
            Path(ext_obs.__file__).resolve()
        ) == EXPECTED_EXT_OBS_SHA256,
    ))

    accepted_seconds = (
        3600,
        21600,
        43200,
        86400,
        172800,
        604800,
    )

    checks.append((
        "3_DURATION_SECONDS_ACCEPT_1H_6H_12H_24H_48H_7D",
        all(
            v06.parse_duration_seconds(value) == value
            for value in accepted_seconds
        ),
    ))

    checks.append((
        "4_DURATION_HOURS_ACCEPT_12_24_48",
        v06.parse_duration_hours(12) == 43200
        and v06.parse_duration_hours(24) == 86400
        and v06.parse_duration_hours(48) == 172800,
    ))

    checks.append((
        "5_DURATION_BOUNDS_FAIL_CLOSED",
        expect_raises(
            lambda: v06.parse_duration_seconds(3599),
            argparse.ArgumentTypeError,
        )
        and expect_raises(
            lambda: v06.parse_duration_seconds(604801),
            argparse.ArgumentTypeError,
        )
        and expect_raises(
            lambda: v06.parse_duration_hours(0),
            argparse.ArgumentTypeError,
        )
        and expect_raises(
            lambda: v06.parse_duration_hours(169),
            argparse.ArgumentTypeError,
        ),
    ))

    parser = ext_obs.build_arg_parser()

    args = parser.parse_args([
        "--duration-hours",
        "24",
        "--launch-managed-collector",
    ])

    checks.append((
        "6_CLI_DURATION_HOURS_24_RESOLVES_86400",
        v06.resolve_duration_seconds(
            args.duration_seconds,
            args.duration_hours,
        ) == 86400,
    ))

    checks.append((
        "7_CLI_REJECTS_DOUBLE_DURATION_SELECTION",
        expect_raises(
            lambda: v06.resolve_duration_seconds(
                86400,
                86400,
            ),
            ValueError,
        ),
    ))

    expected_runtime_fp = hashlib.sha256(
        json.dumps(
            v06.MULTIHOUR_SPEC,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

    checks.append((
        "8_EXTENDED_RUNTIME_SPEC_AND_FINGERPRINT_BIND_DURATION_POLICY",
        v06.MODEL_FINGERPRINT == expected_runtime_fp
        and (
            v06.MULTIHOUR_SPEC[
                "duration_policy"
            ]["maximum_seconds"]
            == 604800
        )
        and (
            v06.MULTIHOUR_SPEC[
                "duration_policy"
            ]["reference_extended_seconds"]
            == [43200, 86400, 172800]
        )
        and (
            v06.MULTIHOUR_SPEC[
                "duration_policy"
            ]["continuous_unbounded"]
            is False
        ),
    ))

    old_v05_model = v05.MODEL_ID
    old_v05_fp = v05.MODEL_FINGERPRINT
    old_v05_spec = v05.MULTIHOUR_SPEC
    old_v05_max = v05.MAX_DURATION_SECONDS
    old_base_max = v05.accepted.MAX_DURATION_SECONDS
    old_v05_binding = (
        v05.ContinuousFirstPullbackBindingV05
    )
    old_v05_run = v05.run_multihour

    captured = {}

    def fake_v05_run(
        *,
        duration_seconds: int,
        launch_collector: bool,
    ):
        captured.update({
            "duration": duration_seconds,
            "launch_collector": launch_collector,
            "model_id": v05.MODEL_ID,
            "model_fp": v05.MODEL_FINGERPRINT,
            "spec": v05.MULTIHOUR_SPEC,
            "v05_max": v05.MAX_DURATION_SECONDS,
            "base_max": (
                v05.accepted.MAX_DURATION_SECONDS
            ),
            "base_parser_value": (
                v05.accepted.parse_duration_seconds(
                    duration_seconds
                )
            ),
            "binding_is_forwarded": (
                v05.ContinuousFirstPullbackBindingV05
                is v06.ContinuousFirstPullbackBindingV05
            ),
        })

        return 0, {
            "result_class": "PASS_MULTI_HOUR",
            "requested_duration_seconds": (
                duration_seconds
            ),
        }

    try:
        v05.run_multihour = fake_v05_run

        code, summary = v06.run_multihour(
            duration_seconds=86400,
            launch_collector=True,
        )

    finally:
        v05.run_multihour = old_v05_run

    checks.append((
        "9_CORE_DELEGATION_EXTENDS_ONLY_DURATION_CONTRACT",
        code == 0
        and summary[
            "requested_duration_seconds"
        ] == 86400
        and captured.get("duration") == 86400
        and captured.get("launch_collector") is True
        and captured.get("model_id") == v06.MODEL_ID
        and (
            captured.get("model_fp")
            == v06.MODEL_FINGERPRINT
        )
        and captured.get("spec") is v06.MULTIHOUR_SPEC
        and captured.get("v05_max") == 604800
        and captured.get("base_max") == 604800
        and (
            captured.get("base_parser_value")
            == 86400
        )
        and (
            captured.get("binding_is_forwarded")
            is True
        ),
    ))

    checks.append((
        "10_CORE_DELEGATION_RESTORES_ACCEPTED_MODULES",
        v05.MODEL_ID == old_v05_model
        and v05.MODEL_FINGERPRINT == old_v05_fp
        and v05.MULTIHOUR_SPEC is old_v05_spec
        and v05.MAX_DURATION_SECONDS == old_v05_max
        and (
            v05.accepted.MAX_DURATION_SECONDS
            == old_base_max
        )
        and (
            v05.ContinuousFirstPullbackBindingV05
            is old_v05_binding
        ),
    ))

    old_obs_run = t021.run_multihour
    old_obs_runtime = t021.accepted_v05
    old_obs_model = t021.MODEL_ID
    old_obs_fp = t021.MODEL_FINGERPRINT
    old_obs_spec = t021.INTEGRATION_SPEC

    obs_capture = {}

    def fake_t021_run(
        *,
        duration_seconds: int,
        launch_collector: bool,
        registry_path,
        **kwargs,
    ):
        obs_capture.update({
            "duration": duration_seconds,
            "launch_collector": launch_collector,
            "registry_path": str(registry_path),
            "runtime_is_v06": (
                t021.accepted_v05 is v06
            ),
            "model_id": t021.MODEL_ID,
            "model_fp": t021.MODEL_FINGERPRINT,
            "spec": t021.INTEGRATION_SPEC,
        })

        return 0, {
            "result_class": "PASS_MULTI_HOUR",
            "external_observability": {
                "terminal_publication_outcome": (
                    "SUCCEEDED"
                ),
            },
        }

    try:
        t021.run_multihour = fake_t021_run

        code, summary = ext_obs.run_multihour(
            duration_seconds=172800,
            launch_collector=False,
            registry_path=Path(
                "dummy.sqlite3"
            ),
        )

    finally:
        t021.run_multihour = old_obs_run

    checks.append((
        "11_T021_WRAPPER_DELEGATION_BINDS_EXTENDED_RUNTIME",
        code == 0
        and (
            summary["result_class"]
            == "PASS_MULTI_HOUR"
        )
        and obs_capture.get("duration") == 172800
        and (
            obs_capture.get("runtime_is_v06")
            is True
        )
        and (
            obs_capture.get("model_id")
            == ext_obs.MODEL_ID
        )
        and (
            obs_capture.get("model_fp")
            == ext_obs.MODEL_FINGERPRINT
        )
        and (
            obs_capture.get("spec")
            is ext_obs.INTEGRATION_SPEC
        ),
    ))

    checks.append((
        "12_T021_MODULE_GLOBALS_RESTORED",
        t021.accepted_v05 is old_obs_runtime
        and t021.MODEL_ID == old_obs_model
        and (
            t021.MODEL_FINGERPRINT
            == old_obs_fp
        )
        and (
            t021.INTEGRATION_SPEC
            is old_obs_spec
        ),
    ))

    checks.append((
        "13_RUN_CONTROL_REMAINS_EXACT_BOUNDARY_ONLY",
        v06.run_control_reason(
            86399.999,
            86400,
        ) is None
        and v06.run_control_reason(
            86400.0,
            86400,
        ) == "DURATION_REACHED",
    ))

    protected_after = {
        "v05": sha256_file(
            Path(v05.__file__).resolve()
        ),
        "t021": sha256_file(
            Path(t021.__file__).resolve()
        ),
    }

    checks.append((
        "14_ACCEPTED_V05_AND_T021_FILES_UNCHANGED",
        protected_before == protected_after,
    ))

    print("=" * 112)
    print(
        "EXTENDED BOUNDED PAPER RUN "
        "T022 v0.1 SELF-TEST"
    )
    print("=" * 112)

    for name, passed in checks:
        print(
            f"{name}: "
            f"{'PASS' if passed else 'FAIL'}"
        )

    print(
        "EVIDENCE:",
        f"v06_model={v06.MODEL_ID}",
        f"v06_fingerprint={v06.MODEL_FINGERPRINT}",
        (
            "extended_observability_fingerprint="
            f"{ext_obs.MODEL_FINGERPRINT}"
        ),
        "reference_hours=12,24,48",
        (
            "max_hours="
            f"{v06.MAX_DURATION_SECONDS // 3600}"
        ),
    )

    print(
        "CHECKS:",
        f"{sum(1 for _, passed in checks if passed)}"
        f"/{len(checks)}",
    )

    ok = all(
        passed
        for _, passed in checks
    )

    print(
        f"RESULT: "
        f"{'PASS' if ok else 'FAIL'}"
    )

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
