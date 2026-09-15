"""Exact reviewed physical proof certificate for the finite T010 constructor.

This is a deliberately narrow proof checker. A different profile, code premise,
engine or allocation model requires a new reviewed certificate; arbitrary JSON
coverage claims and point measurements cannot unlock the public profile.
"""
from __future__ import annotations
import hashlib
from pathlib import Path

from .authority_controls_v0_1 import require
from .t010_resource_measurement_v0_1 import logical_digest

# Exact arithmetic plus native-premise and current-source proof supplements.
# This certificate does not discharge timing, containment or public gates.
CERTIFICATE_SHA256 = "bdc6d661b6e929f5a9f7ae8ba3e95f2c961ef82c9fc7dd8c1f2e90031275e4d7"


def validate_model(model, derivation, environment):
    from .runtime_dry_profile_v0_1 import read_reference
    require(type(model) is dict and set(model) == {"scope", "derivation_digest", "logical_digest", "certificate"}
        and model["scope"] == "ALL_GROWABLE_STORES_PHYSICAL_UPPER_BOUND"
        and model["logical_digest"] == logical_digest(derivation), "T010_RESOURCE_PHYSICAL_MODEL_REQUIRED")
    require(CERTIFICATE_SHA256 != "0"*64 and model["certificate"]["sha256"] == CERTIFICATE_SHA256,
        "T010_RESOURCE_REVIEWED_PHYSICAL_CERTIFICATE_REQUIRED")
    certificate = read_reference(model["certificate"])
    require(certificate["schema"] == "MEME_LIVE_T010_PHYSICAL_PROOF_CERTIFICATE_V1"
        and certificate["logical_digest"] == model["logical_digest"]
        and certificate["native"] == environment["native"]
        and certificate["actual_pager_sector_bytes"] == 4096
        and certificate["unresolved_mathematical_constraints"] == [],
        "T010_RESOURCE_PHYSICAL_CERTIFICATE_CONFLICT")
    root = Path(__file__).resolve().parents[1]
    for relative, expected in certificate["source_bindings"].items():
        path = (root/relative).resolve(strict=True)
        require(path.is_relative_to(root) and hashlib.sha256(path.read_bytes()).hexdigest() == expected,
            "T010_RESOURCE_PHYSICAL_SOURCE_PREMISE_CHANGED")
    require(set(certificate["connections"]) == set(environment["connections"]),
        "T010_RESOURCE_CERTIFIED_SCHEMA_SET_CONFLICT")
    for name, expected in certificate["connections"].items():
        actual = dict(environment["connections"][name]); actual.pop("path", None)
        require(actual == expected, "T010_RESOURCE_CERTIFIED_SCHEMA_CHANGED")
    for key in ("owned_peak_bytes", "recovery_bytes", "temporary_peak_bytes", "reserve_floor_bytes"):
        require(environment[key] == certificate[key], "T010_RESOURCE_PHYSICAL_QUANTITY_CONFLICT")
    return certificate
