from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_continuous_firstpullback_multihour_run_v0_5 as accepted_v05  # noqa: E402
from phase2.quote_aware_price_v0_1 import (  # noqa: E402
    Phase1QuoteAwareAdapterV01,
    SCHEMA_VERSION as PRICE_SCHEMA_VERSION,
)
from phase4.paper_continuous_firstpullback_binding_v0_5 import (  # noqa: E402
    ContinuousFirstPullbackBindingV05,
)
from phase4.paper_continuous_market_source_v0_2 import (  # noqa: E402
    MODEL_FINGERPRINT as SOURCE_FINGERPRINT,
    MODEL_ID as SOURCE_MODEL_ID,
    SCHEMA_VERSION as SOURCE_SCHEMA_VERSION,
    ContinuousMarketSourceV02,
)
from phase4.paper_external_live_observability_v0_1 import (  # noqa: E402
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_HEARTBEAT_TTL_SECONDS,
    MODEL_FINGERPRINT as OBSERVABILITY_FINGERPRINT,
    MODEL_ID as OBSERVABILITY_MODEL_ID,
    SCHEMA_VERSION as OBSERVABILITY_SCHEMA_VERSION,
    LiveRunPublisherV01,
    RunState,
    canonical_run_id,
)


MODEL_ID = "P4-CONTINUOUS-FIRSTPULLBACK-EXTERNAL-OBSERVABILITY-0001"
ACCEPTED_V05_HARNESS_SHA256 = (
    "22d4c0fa40e53e62fb305c75a832f6a633b496a8f10382d75353444e1392db37"
)
DEFAULT_REGISTRY_PATH = (
    accepted_v05.RUNTIME_DIR / "phase4_external_live_observability_v0_1.sqlite3"
)
INTEGRATION_SPEC = {
    "model_id": MODEL_ID,
    "accepted_runtime_model_id": accepted_v05.MODEL_ID,
    "accepted_runtime_fingerprint": accepted_v05.MODEL_FINGERPRINT,
    "accepted_v05_harness_sha256": ACCEPTED_V05_HARNESS_SHA256,
    "observability_model_id": OBSERVABILITY_MODEL_ID,
    "observability_schema_version": OBSERVABILITY_SCHEMA_VERSION,
    "observability_fingerprint": OBSERVABILITY_FINGERPRINT,
    "integration": "PASSIVE_WRAPPER_AROUND_ACCEPTED_V05_EXECUTION_PATH",
    "heartbeat": "LATEST_SOURCE_PROBE_AND_VERSIONED_BINDING_BOUNDARIES",
    "publication_failure": "DISABLE_OBSERVABILITY_RUNTIME_CONTINUES_STALE_EXPIRES",
    "strategy_or_execution_input": False,
    "accepted_runtime_modified": False,
    "paper_only": True,
}
MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(INTEGRATION_SPEC, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
).hexdigest()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _validate_locked_contracts() -> None:
    accepted_v05._validate_locked_contracts_v05()
    if accepted_v05.accepted.file_sha256(Path(accepted_v05.__file__).resolve()) != (
        ACCEPTED_V05_HARNESS_SHA256
    ):
        raise RuntimeError("accepted multi-hour v0.5 harness changed")
    expected = (
        accepted_v05.MODEL_FINGERPRINT,
        SOURCE_FINGERPRINT,
        OBSERVABILITY_FINGERPRINT,
    )
    actual = (
        "0308899e0a2306c921603f3beff4a957e01832ce180a397b88841ba498fab5e4",
        "9cb094f52bf4b4fe28dc4828b1d9a52a3350cde664c7da5a489fa84e9a4085a9",
        OBSERVABILITY_FINGERPRINT,
    )
    if expected != actual:
        raise RuntimeError("external observability integration fingerprint drift")


def _paper_database_path(binding: ContinuousFirstPullbackBindingV05) -> Path:
    rows = binding.conn.execute("PRAGMA database_list").fetchall()
    for row in rows:
        if str(row[1]) == "main" and str(row[2]).strip():
            return Path(str(row[2])).resolve()
    raise RuntimeError("paper runtime main SQLite path is unavailable")


def run_multihour(
    *,
    duration_seconds: int,
    launch_collector: bool,
    registry_path: str | Path = DEFAULT_REGISTRY_PATH,
    heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    heartbeat_ttl_seconds: float = DEFAULT_HEARTBEAT_TTL_SECONDS,
    wall_clock: Callable[[], datetime] = _utc_now,
    monotonic_clock: Callable[[], float] = time.monotonic,
    run_id_factory: Callable[[], str] = lambda: str(uuid.uuid4()),
) -> tuple[int, dict[str, Any]]:
    """Run the frozen V0.5 path with downstream passive publication only."""

    _validate_locked_contracts()
    run_id = canonical_run_id(str(run_id_factory()))
    run_started_at_utc = wall_clock()
    registry = Path(registry_path).resolve()
    context: dict[str, Any] = {
        "run_id": run_id,
        "latest_p1_rowid": None,
        "runtime_session_identity": None,
        "publisher": None,
        "binding": None,
        "last_cursor": 0,
        "last_watermark": 0,
        "last_watermark_kind": "CURRENT",
        "publication_errors": [],
        "publisher_disabled": False,
        "publication_metrics": None,
    }

    source_owner = accepted_v05.accepted_v04.accepted_v03
    accepted_base = accepted_v05.accepted
    original_source_class = source_owner.ContinuousMarketSourceV02
    original_binding_class = accepted_v05.ContinuousFirstPullbackBindingV05
    original_session_identity = accepted_base.session_identity

    def record_publication_error(stage: str, exc: BaseException) -> None:
        context["publication_errors"].append(
            f"{stage}:{type(exc).__name__}:{exc}"
        )
        context["publisher_disabled"] = True
        publisher = context.get("publisher")
        if publisher is not None:
            context["publication_metrics"] = {
                "publication_count": publisher.publication_count,
                "heartbeat_publication_count": (
                    publisher.heartbeat_publication_count
                ),
                "terminal_publication_count": publisher.terminal_publication_count,
            }
            try:
                publisher.close()
            except Exception:
                pass
            context["publisher"] = None

    def current_source_position() -> tuple[int, int, str]:
        binding = context.get("binding")
        if binding is None:
            return (
                int(context["last_cursor"]),
                int(context["last_watermark"]),
                str(context["last_watermark_kind"]),
            )
        try:
            cursor = int(binding.durable_p1_rowid)
            fence = binding.active_timer_fence_p1_rowid
        except sqlite3.ProgrammingError:
            return (
                int(context["last_cursor"]),
                int(context["last_watermark"]),
                str(context["last_watermark_kind"]),
            )
        if fence is not None:
            position = (cursor, max(cursor, int(fence)), "FROZEN")
        else:
            latest = context.get("latest_p1_rowid")
            position = (
                cursor,
                max(cursor, cursor if latest is None else int(latest)),
                "CURRENT",
            )
        context["last_cursor"], context["last_watermark"], context[
            "last_watermark_kind"
        ] = position
        return position

    def publish_heartbeat(*, force: bool, stage: str) -> None:
        if context["publisher_disabled"]:
            return
        publisher = context.get("publisher")
        if publisher is None:
            return
        try:
            cursor, watermark, kind = current_source_position()
            publisher.publish_heartbeat(
                durable_source_cursor_p1_rowid=cursor,
                source_watermark_p1_rowid=watermark,
                source_watermark_kind=kind,
                force=force,
            )
        except Exception as exc:
            record_publication_error(stage, exc)

    class ObservableMarketSourceV02(original_source_class):
        def latest_p1_rowid(self) -> int:
            latest = super().latest_p1_rowid()
            context["latest_p1_rowid"] = int(latest)
            publish_heartbeat(force=False, stage="LATEST_SOURCE_HEARTBEAT")
            return latest

    class ObservableBindingV05(original_binding_class):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            context["binding"] = self
            context["latest_p1_rowid"] = max(
                int(self.market_source.start_after_p1_rowid),
                int(context.get("latest_p1_rowid") or 0),
            )
            context["last_cursor"] = int(self.durable_p1_rowid)
            context["last_watermark"] = max(
                int(self.durable_p1_rowid), int(context["latest_p1_rowid"])
            )
            try:
                publisher = LiveRunPublisherV01.start(
                    registry,
                    runtime_model_id=accepted_v05.MODEL_ID,
                    runtime_fingerprint=accepted_v05.MODEL_FINGERPRINT,
                    runtime_session_identity=str(
                        context.get("runtime_session_identity") or run_id
                    ),
                    paper_runtime_sqlite_path=_paper_database_path(self),
                    source_model_id=SOURCE_MODEL_ID,
                    source_model_fingerprint=SOURCE_FINGERPRINT,
                    source_contract_version=SOURCE_SCHEMA_VERSION,
                    source_identity=self.market_source.source_identity,
                    source_sqlite_path=self.market_source.db_path,
                    source_database_identity=self.market_source.database_identity,
                    source_anchor_p1_rowid=self.market_source.start_after_p1_rowid,
                    initial_durable_source_cursor_p1_rowid=self.durable_p1_rowid,
                    initial_source_watermark_p1_rowid=max(
                        self.durable_p1_rowid,
                        int(context["latest_p1_rowid"]),
                    ),
                    price_representation_version=(
                        f"{PRICE_SCHEMA_VERSION}/{Phase1QuoteAwareAdapterV01.schema_version}"
                    ),
                    run_id=run_id,
                    heartbeat_interval_seconds=heartbeat_interval_seconds,
                    heartbeat_ttl_seconds=heartbeat_ttl_seconds,
                    started_at_utc=run_started_at_utc,
                    wall_clock=wall_clock,
                    monotonic_clock=monotonic_clock,
                )
                context["publisher"] = publisher
            except Exception as exc:
                record_publication_error("START", exc)

        def process_next_batch(self, *args, **kwargs):
            result = super().process_next_batch(*args, **kwargs)
            publish_heartbeat(force=False, stage="SOURCE_BATCH_HEARTBEAT")
            return result

        def prepare_clock_tick(self, *args, **kwargs):
            key = super().prepare_clock_tick(*args, **kwargs)
            publish_heartbeat(force=True, stage="TIMER_PREPARE_HEARTBEAT")
            return key

        def execute_prepared_timer(self, *args, **kwargs):
            result = super().execute_prepared_timer(*args, **kwargs)
            publish_heartbeat(force=False, stage="TIMER_EXECUTE_HEARTBEAT")
            return result

    def capture_session_identity(*args, **kwargs):
        identity = original_session_identity(*args, **kwargs)
        context["runtime_session_identity"] = identity
        return identity

    code: int
    summary: dict[str, Any]
    terminal_state: RunState | None = None
    terminal_exception: BaseException | None = None
    try:
        source_owner.ContinuousMarketSourceV02 = ObservableMarketSourceV02
        accepted_v05.ContinuousFirstPullbackBindingV05 = ObservableBindingV05
        accepted_base.session_identity = capture_session_identity
        code, summary = accepted_v05.run_multihour(
            duration_seconds=duration_seconds,
            launch_collector=launch_collector,
        )
        terminal_state = (
            RunState.COMPLETED
            if code == 0 and summary.get("result_class") == "PASS_MULTI_HOUR"
            else RunState.FAILED
        )
    except (KeyboardInterrupt, SystemExit) as exc:
        terminal_state = RunState.ABORTED
        terminal_exception = exc
        raise
    except BaseException as exc:
        terminal_state = RunState.FAILED
        terminal_exception = exc
        raise
    finally:
        try:
            publisher = context.get("publisher")
            binding = context.get("binding")
            if publisher is not None and terminal_state is not None and binding is not None:
                try:
                    cursor, watermark, kind = current_source_position()
                    if terminal_exception is None and "summary" in locals():
                        source_summary = summary.get("production_source", {})
                        cursor = int(
                            source_summary.get("final_durable_p1_rowid")
                            if source_summary.get("final_durable_p1_rowid") is not None
                            else cursor
                        )
                        watermark = max(
                            cursor,
                            int(source_summary.get("end_watermark_p1_rowid", watermark)),
                        )
                        if summary.get("duration_bound_reached"):
                            kind = "FROZEN"
                    publisher.publish_terminal(
                        terminal_state,
                        durable_source_cursor_p1_rowid=cursor,
                        source_watermark_p1_rowid=watermark,
                        source_watermark_kind=kind,
                        ended_at_utc=wall_clock(),
                    )
                    context["publication_metrics"] = {
                        "publication_count": publisher.publication_count,
                        "heartbeat_publication_count": (
                            publisher.heartbeat_publication_count
                        ),
                        "terminal_publication_count": (
                            publisher.terminal_publication_count
                        ),
                    }
                except Exception as exc:
                    record_publication_error("TERMINAL", exc)
                finally:
                    try:
                        publisher.close()
                    except Exception:
                        pass
                    context["publisher"] = None
        finally:
            accepted_base.session_identity = original_session_identity
            accepted_v05.ContinuousFirstPullbackBindingV05 = original_binding_class
            source_owner.ContinuousMarketSourceV02 = original_source_class

    summary["external_observability"] = {
        "integration_model_id": MODEL_ID,
        "integration_fingerprint": MODEL_FINGERPRINT,
        "contract_model_id": OBSERVABILITY_MODEL_ID,
        "contract_version": OBSERVABILITY_SCHEMA_VERSION,
        "contract_fingerprint": OBSERVABILITY_FINGERPRINT,
        "registry_path": str(registry),
        "run_id": run_id,
        "terminal_state": terminal_state.value if terminal_state is not None else None,
        "publication_errors": list(context["publication_errors"]),
        "publisher_disabled": bool(context["publisher_disabled"]),
        "publication_metrics": context["publication_metrics"],
        "passive_only": True,
    }
    return code, summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = accepted_v05.build_arg_parser()
    parser.add_argument(
        "--observability-registry",
        type=Path,
        default=DEFAULT_REGISTRY_PATH,
        help="fixed producer-owned SQLite registry for read-only observers",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if args.collector_child:
        return accepted_v05.accepted.run_collector_child()
    if args.collector_child_probe:
        return accepted_v05.accepted.run_collector_child_probe(
            args.collector_child_probe_ready
        )
    if not args.use_existing_collector and not args.launch_managed_collector:
        parser.error("one collector mode is required")
    code, _summary = run_multihour(
        duration_seconds=args.duration_seconds,
        launch_collector=bool(args.launch_managed_collector),
        registry_path=args.observability_registry,
    )
    return code


if __name__ == "__main__":
    raise SystemExit(main())
