from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Mapping

from .outcome_replay_v0_1_3 import CoverageInterval, CoverageStatus, IntervalCoverageProvider

SNAPSHOT_SCHEMA_VERSION = "P3COV-0.1"

def parse_utc(value: str) -> datetime:
    text=value.strip()
    if text.endswith("Z"):
        text=text[:-1]+"+00:00"
    dt=datetime.fromisoformat(text)
    if dt.tzinfo is None:
        raise ValueError(f"UTC timestamp lacks timezone: {value!r}")
    return dt.astimezone(timezone.utc)

@dataclass(frozen=True,slots=True)
class CollectorCoverageSnapshotV02:
    path:Path
    payload:Mapping[str,Any]

    @classmethod
    def load(cls,path:str|Path)->"CollectorCoverageSnapshotV02":
        p=Path(path)
        payload=json.loads(p.read_text(encoding="utf-8"))
        if payload.get("schema_version")!=SNAPSHOT_SCHEMA_VERSION:
            raise ValueError(
                f"Unexpected coverage schema {payload.get('schema_version')!r}; "
                f"expected {SNAPSHOT_SCHEMA_VERSION!r}"
            )
        if not payload.get("freeze_id"):
            raise ValueError("coverage snapshot missing freeze_id")
        return cls(p,payload)

    @property
    def freeze_id(self)->str:
        return str(self.payload["freeze_id"])

    @property
    def range_start(self)->datetime:
        return parse_utc(str(self.payload["dataset_range_utc"]["start"]))

    @property
    def range_end(self)->datetime:
        return parse_utc(str(self.payload["dataset_range_utc"]["end"]))

    def validate_binding(self,*,freeze_id:str,range_start:datetime,range_end:datetime)->None:
        if self.freeze_id!=freeze_id:
            raise ValueError(f"Coverage freeze_id={self.freeze_id!r} != {freeze_id!r}")
        if self.range_start!=range_start or self.range_end!=range_end:
            raise ValueError("Coverage dataset range does not exactly match freeze manifest")

    def provider(self)->IntervalCoverageProvider:
        intervals:list[CoverageInterval]=[]
        for row in self.payload.get("active_intervals",[]):
            if bool(row.get("uncertain_close")):
                continue
            intervals.append(CoverageInterval(
                start=parse_utc(str(row["start_utc"])),
                end=parse_utc(str(row["end_utc"])),
                status=CoverageStatus.COMPLETE,
            ))
        for row in self.payload.get("explicit_gaps",[]):
            intervals.append(CoverageInterval(
                start=parse_utc(str(row["start_utc"])),
                end=parse_utc(str(row["end_utc"])),
                status=CoverageStatus.GAP,
            ))
        return IntervalCoverageProvider(tuple(intervals))
