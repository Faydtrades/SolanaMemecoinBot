from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from typing import Mapping

from phase2.models_v0_1 import IngestionSource
from phase2.quote_aware_price_v0_1 import (
    Phase1QuoteAwareAdapterV01,
    QuoteAwareNormalizedEvent,
)


MODEL_ID = "P4-PHASE1-GAP-SOURCE-COMPAT-0001"
SCHEMA_VERSION = "phase4_phase1_gap_source_compat_v0.1"
V034_GAP_SOURCE = "GAP_RECONCILIATION_V0_3_4"
ACCEPTED_GAP_SOURCE_ALIAS = "GAP_RECONCILIATION_V0_3_3"

_SPEC = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "accepted_quote_adapter": Phase1QuoteAwareAdapterV01.schema_version,
    "exact_aliases": {V034_GAP_SOURCE: ACCEPTED_GAP_SOURCE_ALIAS},
    "alias_scope": "EXACT_LABEL_ONLY_NO_PREFIX_REGEX_OR_FUTURE_VERSION",
    "row_handling": "COPY_FOR_NORMALIZATION_PRESERVE_RAW_ROW_AND_PROVENANCE",
    "semantic_result": "INGESTION_SOURCE_GAP_RECOVERY_NEVER_FRESH_LIVE",
}
MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(_SPEC, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "utf-8"
    )
).hexdigest()


@dataclass(frozen=True, slots=True)
class GapSourceCompatibilityResultV01:
    event: QuoteAwareNormalizedEvent | None
    skip_reason: str | None
    raw_source_decoded_file: str | None
    normalized_source_decoded_file: str | None
    compatibility_applied: bool


class Phase1GapSourceCompatibilityV01:
    """Exact v0.3.4 gap-label bridge into accepted Phase-2 semantics."""

    model_id = MODEL_ID
    model_fingerprint = MODEL_FINGERPRINT

    @staticmethod
    def row_to_event(
        row: sqlite3.Row | Mapping[str, object],
    ) -> GapSourceCompatibilityResultV01:
        raw_value = row["source_decoded_file"]
        raw_source = None if raw_value is None else str(raw_value)
        compatibility_applied = raw_source == V034_GAP_SOURCE

        normalization_row: sqlite3.Row | Mapping[str, object] = row
        normalized_source = raw_source
        if compatibility_applied:
            aliased = dict(row)
            aliased["source_decoded_file"] = ACCEPTED_GAP_SOURCE_ALIAS
            normalization_row = aliased
            normalized_source = ACCEPTED_GAP_SOURCE_ALIAS

        event, reason = Phase1QuoteAwareAdapterV01.row_to_event(normalization_row)
        if compatibility_applied and event is not None:
            if event.base.source != IngestionSource.GAP_RECOVERY:
                raise RuntimeError(
                    "v0.3.4 gap compatibility did not normalize as GAP_RECOVERY"
                )

        return GapSourceCompatibilityResultV01(
            event=event,
            skip_reason=reason,
            raw_source_decoded_file=raw_source,
            normalized_source_decoded_file=normalized_source,
            compatibility_applied=compatibility_applied,
        )
