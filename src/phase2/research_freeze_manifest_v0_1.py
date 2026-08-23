from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import csv
import json
import re
import sqlite3
from pathlib import Path
from typing import Iterable, Mapping, Sequence

FREEZE_SCHEMA_VERSION = "P2FREEZE-0.1"
EXPERIMENT_MANIFEST_SCHEMA_VERSION = "EXPM-0.1"
EXPERIMENT_REGISTRY_SCHEMA_VERSION = "EXPR-0.1"
FREEZE_ID = "DEV-FREEZE-0001"
LOCK_DECISION_ID = "PHASE-2-PARAM-SEARCH-001"

# Exact values explicitly locked after the successful Phase 2.13 v0.1.1 run.
LOCKED_PARAM_SEARCH_001: dict[str, object] = {
    "decision_id": LOCK_DECISION_ID,
    "status": "LOCKED_FOR_FIRST_RESEARCH_CYCLE",
    "strategy_name": "FirstPullback",
    "strategy_version_target": "v1.1",
    "max_entry_age_ms": [300_000],
    "impulse": {
        "min_return_bps": [500, 1500, 2000, 3000, 5000, 5500, 12500],
        "min_trades_since_t0": [2, 3, 5, 7, 24],
        "min_unique_buyers_since_t0": [1, 2, 3, 5, 9],
    },
    "pullback": {
        "min_depth_bps": [1000, 2500, 3000, 4000, 6000],
        "max_depth_bps": [3500, 5500, 6000, 6500, 9000],
        "valid_min_max_pair_count": 21,
    },
    "buyer_response": {
        "window_id": ["3s"],
        "min_rebound_bps": [200, 500, 800, 2300, 7300, 20700],
        "min_buys": [1, 2, 3, 4, 8],
        "min_net_flow_reserve_ppm": [0, 6000, 30000, 93000, 185000],
    },
    "reclaim": {
        "min_extension_from_response_bps": [200, 500, 700, 800, 2500, 7100, 19500],
    },
    "runaway": {
        "max_extension_from_response_bps": [2000, 2500, 3000, 7000, 10000],
        "valid_reclaim_runaway_pair_count": 24,
    },
    "search_blocks": {
        "A_IMPULSE": [
            "min_return_bps",
            "min_trades_since_t0",
            "min_unique_buyers_since_t0",
        ],
        "B_PULLBACK": ["min_depth_bps", "max_depth_bps"],
        "C_BUYER_RESPONSE_3S": [
            "min_rebound_bps",
            "min_buys",
            "min_net_flow_reserve_ppm",
        ],
        "D_RECLAIM_RUNAWAY": [
            "min_extension_from_response_bps",
            "max_extension_from_response_bps",
        ],
    },
    "block_combination_counts": {
        "A_IMPULSE": 175,
        "B_PULLBACK": 21,
        "C_BUYER_RESPONSE_3S": 150,
        "D_RECLAIM_RUNAWAY": 24,
    },
    "naive_full_cartesian_count": 13_230_000,
    "full_cartesian_recommended": False,
    "dataset_policy": {
        "development_discovery_tokens": 3901,
        "development_dataset_freeze_id": FREEZE_ID,
        "do_not_treat_as_untouched_oos": True,
        "future_range_changes_require_new_version": True,
    },
}


def _jsonable(value: object) -> object:
    if isinstance(value, bytes):
        return {"__bytes_hex__": value.hex()}
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return value


def canonical_json(value: object) -> str:
    return json.dumps(
        _jsonable(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def stable_sha256(value: object) -> str:
    return sha256(canonical_json(value).encode("utf-8")).hexdigest()


def file_sha256(path: str | Path) -> str:
    h = sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def iso_to_us(value: str) -> int:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        raise ValueError(f"Timestamp lacks timezone: {value!r}")
    return int(round(dt.astimezone(timezone.utc).timestamp() * 1_000_000))


def us_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc).isoformat()


def validate_locked_search_space(space: Mapping[str, object]) -> None:
    if space.get("decision_id") != LOCK_DECISION_ID:
        raise ValueError("Wrong parameter-search decision id")
    if space.get("status") != "LOCKED_FOR_FIRST_RESEARCH_CYCLE":
        raise ValueError("Search space is not explicitly locked")
    if space.get("strategy_version_target") != "v1.1":
        raise ValueError("Locked search space must target FirstPullback v1.1")
    if space.get("max_entry_age_ms") != [300_000]:
        raise ValueError("First-cycle entry window must remain locked at 5m")
    response = space.get("buyer_response")
    if not isinstance(response, Mapping) or response.get("window_id") != ["3s"]:
        raise ValueError("First-cycle buyer-response window must remain fixed at 3s")
    if space.get("full_cartesian_recommended") is not False:
        raise ValueError("Naive full Cartesian search must remain rejected")
    if int(space.get("naive_full_cartesian_count", -1)) != 13_230_000:
        raise ValueError("Unexpected locked full-Cartesian count")


def proposal_parameter_projection(proposal: Mapping[str, object]) -> dict[str, object]:
    """Project the 2.13 proposal onto the fields that became PARAM-SEARCH-001."""
    return {
        "strategy_version_target": proposal.get("strategy_version_target"),
        "max_entry_age_ms": proposal.get("max_entry_age_ms"),
        "impulse": proposal.get("impulse"),
        "pullback": proposal.get("pullback"),
        "buyer_response": proposal.get("buyer_response"),
        "reclaim": proposal.get("reclaim"),
        "runaway": proposal.get("runaway"),
        "search_blocks": proposal.get("search_blocks"),
        "block_combination_counts": proposal.get("block_combination_counts"),
        "naive_full_cartesian_count": proposal.get("naive_full_cartesian_count"),
        "full_cartesian_recommended": proposal.get("full_cartesian_recommended"),
    }


def locked_parameter_projection() -> dict[str, object]:
    return proposal_parameter_projection(LOCKED_PARAM_SEARCH_001)


def validate_proposal_matches_lock(proposal: Mapping[str, object]) -> None:
    if proposal_parameter_projection(proposal) != locked_parameter_projection():
        raise ValueError(
            "The Phase 2.13 proposal does not exactly match PHASE-2-PARAM-SEARCH-001"
        )


def load_dev_mints_from_csv(path: str | Path) -> tuple[str, ...]:
    mints: list[str] = []
    with Path(path).open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if "mint" not in (reader.fieldnames or []):
            raise ValueError("Token metrics CSV has no mint column")
        for row in reader:
            mint = str(row.get("mint") or "").strip()
            if mint:
                mints.append(mint)
    if not mints:
        raise ValueError("No development mints found")
    if len(mints) != len(set(mints)):
        raise ValueError("Development mint list contains duplicates")
    return tuple(sorted(mints))


def _row_value(row: sqlite3.Row | Mapping[str, object], key: str) -> object:
    if isinstance(row, sqlite3.Row):
        return row[key]
    return row[key]


def eligible_mints_from_normalized(events: Sequence[object]) -> tuple[str, ...]:
    by_mint: dict[str, bool] = {}
    for item in events:
        base = item.base if hasattr(item, "base") else item
        mint = str(base.mint)
        is_trade = getattr(base.event_type, "value", base.event_type) in ("BUY", "SELL")
        by_mint[mint] = by_mint.get(mint, False) or bool(is_trade)
    return tuple(sorted(m for m, has_trade in by_mint.items() if has_trade))


def pump_event_columns(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    rows = conn.execute("PRAGMA table_info(pump_events)").fetchall()
    if not rows:
        raise ValueError("pump_events table missing")
    out = []
    for row in rows:
        name = str(row["name"])
        type_name = str(row["type"] or "")
        out.append((name, type_name))
    return out


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def selected_rows_for_dev_mints(
    raw_rows: Sequence[sqlite3.Row],
    dev_mints: set[str],
) -> list[sqlite3.Row]:
    rows = [row for row in raw_rows if str(row["mint"]) in dev_mints]
    return sorted(rows, key=lambda r: int(r["p1_rowid"]))


def frozen_rows_sha256(
    rows: Sequence[sqlite3.Row | Mapping[str, object]],
    column_names: Sequence[str],
) -> str:
    h = sha256()
    h.update(canonical_json(list(column_names)).encode("utf-8"))
    h.update(b"\n")
    for row in rows:
        payload = {
            "source_p1_rowid": int(_row_value(row, "p1_rowid")),
            "values": [_jsonable(_row_value(row, name)) for name in column_names],
        }
        h.update(canonical_json(payload).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def event_type_counts(rows: Sequence[sqlite3.Row]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        key = str(row["event_type"])
        out[key] = out.get(key, 0) + 1
    return dict(sorted(out.items()))


def source_counts(rows: Sequence[sqlite3.Row]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        key = str(row["source_decoded_file"])
        out[key] = out.get(key, 0) + 1
    return dict(sorted(out.items()))


@dataclass(frozen=True, slots=True)
class FreezeArtifacts:
    snapshot_db: Path
    manifest_json: Path
    token_csv: Path
    locked_search_json: Path
    experiment_template_json: Path
    experiment_registry_json: Path


def write_snapshot_database(
    source_conn: sqlite3.Connection,
    rows: Sequence[sqlite3.Row],
    dev_mints: Sequence[str],
    snapshot_path: str | Path,
    *,
    freeze_core: Mapping[str, object],
) -> None:
    path = Path(snapshot_path)
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite frozen dataset: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()

    columns = pump_event_columns(source_conn)
    names = [name for name, _ in columns]
    defs = ", ".join(
        f"{quote_ident(name)} {type_name}".strip()
        for name, type_name in columns
    )

    dst = sqlite3.connect(tmp)
    try:
        dst.execute("PRAGMA journal_mode=DELETE;")
        dst.execute("PRAGMA synchronous=FULL;")
        dst.execute(f"CREATE TABLE pump_events ({defs})")
        dst.execute(
            """
            CREATE TABLE freeze_event_index (
                source_p1_rowid INTEGER PRIMARY KEY,
                event_key TEXT,
                mint TEXT NOT NULL,
                event_type TEXT NOT NULL,
                decoded_at_utc TEXT NOT NULL
            )
            """
        )
        dst.execute(
            """
            CREATE TABLE freeze_tokens (
                mint TEXT PRIMARY KEY
            )
            """
        )
        dst.execute(
            """
            CREATE TABLE freeze_metadata (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL
            )
            """
        )

        placeholders = ",".join("?" for _ in range(len(names) + 1))
        insert_sql = (
            f"INSERT INTO pump_events(rowid,{','.join(quote_ident(n) for n in names)}) "
            f"VALUES ({placeholders})"
        )
        for row in rows:
            source_rowid = int(row["p1_rowid"])
            values = [row[name] for name in names]
            dst.execute(insert_sql, [source_rowid, *values])
            dst.execute(
                """
                INSERT INTO freeze_event_index(
                    source_p1_rowid,event_key,mint,event_type,decoded_at_utc
                ) VALUES (?,?,?,?,?)
                """,
                (
                    source_rowid,
                    str(row["event_key"]),
                    str(row["mint"]),
                    str(row["event_type"]),
                    str(row["decoded_at_utc"]),
                ),
            )

        dst.executemany(
            "INSERT INTO freeze_tokens(mint) VALUES (?)",
            [(m,) for m in sorted(dev_mints)],
        )
        for key, value in sorted(freeze_core.items()):
            dst.execute(
                "INSERT INTO freeze_metadata(key,value_json) VALUES (?,?)",
                (str(key), canonical_json(value)),
            )
        dst.commit()
        quick = dst.execute("PRAGMA quick_check").fetchone()[0]
        if quick != "ok":
            raise RuntimeError(f"Frozen snapshot quick_check failed: {quick}")
    finally:
        dst.close()

    tmp.replace(path)


def build_experiment_manifest_template(
    dataset_manifest: Mapping[str, object],
) -> dict[str, object]:
    return {
        "manifest_schema_version": EXPERIMENT_MANIFEST_SCHEMA_VERSION,
        "experiment_id": "EXP-XXXX",
        "status": "TEMPLATE_NOT_RUN",
        "hypothesis": "REQUIRED_BEFORE_RUN",
        "dataset": {
            "freeze_id": dataset_manifest["freeze_id"],
            "dataset_role": "DEVELOPMENT_DISCOVERY",
            "dataset_manifest_sha256": dataset_manifest[
                "deterministic_manifest_sha256"
            ],
            "token_count": dataset_manifest["dataset"]["eligible_token_count"],
            "date_time_range": dataset_manifest["dataset"]["t0_observed_range_utc"],
            "source_cutoff": dataset_manifest["dataset"]["source_cutoff"],
        },
        "strategy": {
            "name": "FirstPullback",
            "version": "v1.1",
            "market_state_schema": "MS-0.1",
            "feature_stack": [
                "FeatureEngineV02/BOT_TRUTH_CLOCK",
                "QuoteAwarePriceV01",
                "DenominationAwareFeatureEngineV01",
            ],
            "strategy_clock": "SCLOCK-0.1",
        },
        "search_space": {
            "decision_id": LOCK_DECISION_ID,
            "locked_search_space_sha256": dataset_manifest[
                "parameter_search"
            ]["locked_search_space_sha256"],
            "experiment_block": "REQUIRED:A_IMPULSE|B_PULLBACK|C_BUYER_RESPONSE_3S|D_RECLAIM_RUNAWAY",
        },
        "parameters": {},
        "filters": [],
        "execution_model": {
            "version": "NOT_DEFINED_YET",
            "reference_position_size_sol": 0.10,
            "reference_size_note": (
                "Locked research/comparison size from the Phase-1 master checkpoint; "
                "not a promise of initial live-trading size."
            ),
            "fee_model": "NOT_DEFINED_YET",
            "slippage_model": "NOT_DEFINED_YET",
            "latency_model": "NOT_DEFINED_YET",
            "failed_execution_behavior": "NOT_DEFINED_YET",
        },
        "result_metrics": {
            "gross_raw": {},
            "realistic_net": {},
            "execution_quality": {},
        },
        "notes": "",
        "decision": "TBD",
        "reproducibility": {
            "dataset_freeze_id": dataset_manifest["freeze_id"],
            "frozen_rows_sha256": dataset_manifest["dataset"][
                "frozen_rows_sha256"
            ],
            "code_versions_must_be_recorded": True,
            "random_seed_if_applicable": None,
        },
    }


def build_experiment_registry(
    dataset_manifest: Mapping[str, object],
) -> dict[str, object]:
    core = {
        "registry_schema_version": EXPERIMENT_REGISTRY_SCHEMA_VERSION,
        "freeze_id": dataset_manifest["freeze_id"],
        "dataset_manifest_sha256": dataset_manifest[
            "deterministic_manifest_sha256"
        ],
        "locked_search_decision_id": LOCK_DECISION_ID,
        "next_experiment_id": "EXP-0001",
        "experiments": [],
    }
    core["deterministic_registry_sha256"] = stable_sha256(core)
    return core


EXPERIMENT_ID_RE = re.compile(r"^EXP-\d{4}$")


def validate_experiment_manifest(
    manifest: Mapping[str, object],
    *,
    allow_template: bool = False,
) -> None:
    required = {
        "manifest_schema_version",
        "experiment_id",
        "status",
        "hypothesis",
        "dataset",
        "strategy",
        "search_space",
        "parameters",
        "filters",
        "execution_model",
        "result_metrics",
        "notes",
        "decision",
        "reproducibility",
    }
    missing = sorted(required - set(manifest))
    if missing:
        raise ValueError(f"Experiment manifest missing fields: {missing}")
    exp_id = str(manifest["experiment_id"])
    if allow_template:
        if exp_id != "EXP-XXXX" and not EXPERIMENT_ID_RE.match(exp_id):
            raise ValueError("Invalid experiment id")
    elif not EXPERIMENT_ID_RE.match(exp_id):
        raise ValueError("Experiment id must be EXP-0001 style")

    dataset = manifest["dataset"]
    if not isinstance(dataset, Mapping):
        raise ValueError("dataset must be an object")
    for key in (
        "freeze_id",
        "dataset_role",
        "dataset_manifest_sha256",
        "token_count",
        "date_time_range",
    ):
        if key not in dataset:
            raise ValueError(f"dataset missing {key}")

    strategy = manifest["strategy"]
    if not isinstance(strategy, Mapping) or not strategy.get("version"):
        raise ValueError("strategy/version is required")
    execution = manifest["execution_model"]
    if not isinstance(execution, Mapping) or "version" not in execution:
        raise ValueError("execution model/version is required")
    if "reference_position_size_sol" not in execution:
        raise ValueError("reference position size field is required")
    if not isinstance(manifest["filters"], list):
        raise ValueError("filters must be a list")
    if not isinstance(manifest["result_metrics"], Mapping):
        raise ValueError("result_metrics must be an object")


def validate_experiment_registry(registry: Mapping[str, object]) -> None:
    if registry.get("registry_schema_version") != EXPERIMENT_REGISTRY_SCHEMA_VERSION:
        raise ValueError("Wrong registry schema")
    next_id = str(registry.get("next_experiment_id") or "")
    if not EXPERIMENT_ID_RE.match(next_id):
        raise ValueError("next_experiment_id must be EXP-0001 style")
    experiments = registry.get("experiments")
    if not isinstance(experiments, list):
        raise ValueError("experiments must be a list")


def write_token_csv(
    path: str | Path,
    dev_mints: Sequence[str],
    t0_by_mint: Mapping[str, int],
) -> None:
    with Path(path).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["mint", "first_tradable_observed_at_us", "first_tradable_observed_at_utc"],
        )
        writer.writeheader()
        for mint in sorted(dev_mints):
            value = int(t0_by_mint[mint])
            writer.writerow(
                {
                    "mint": mint,
                    "first_tradable_observed_at_us": value,
                    "first_tradable_observed_at_utc": us_to_iso(value),
                }
            )
