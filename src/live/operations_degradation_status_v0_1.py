"""Read-only sanitized historical C2 status for an already configured host.

This projection neither samples current resources nor grants Runtime permission.
The caller supplies existing trusted typed configuration; no payload parser,
initialization, acknowledgement, reset or economic mutation is provided.
"""
from dataclasses import asdict
from .operations_degradation_v0_1 import DegradationStore, METRICS, require
from .operations_degradation_monitor_v0_1 import (
    MonitorConfiguration, alert_view, content_fingerprint, ERRORS,
)


def recorded_status(configuration, domain, identity):
    """Map persisted incidents to the exact bound reviewed metric/limit only."""
    result = dict(view="HISTORICAL_RECORDED_ALERTS", grants_permission=False,
        may_sign=False, may_send=False, entry_held=True, unavailable_code=None,
        conditions=[])
    try:
        require(type(configuration) is MonitorConfiguration
            and configuration.policy.reviewed_configuration_digest == configuration.binding_for(identity)
            and configuration.runtime_code_digest == identity.runtime_code_digest)
        view = alert_view(DegradationStore(configuration.path, domain, configuration.policy).snapshot())
        policy = configuration.policy
        limits = {item.metric: item.limit for item in policy.resource_limits}
        subjects = {content_fingerprint((domain.economic_domain_id, "RESOURCE",
            (policy.content_digest, metric))): metric for metric in METRICS}
        coverage = content_fingerprint((domain.economic_domain_id, "PROFILE_COVERAGE", policy.content_digest))
        for row in view.conditions:
            item = asdict(row)
            metric = subjects.get(row.subject_digest) if row.condition in ("PROFILE_UNRESOLVED", "RESOURCE_EXCEEDED") else None
            item.update(metric=metric, configured_limit=limits.get(metric),
                limit_direction=("MINIMUM" if metric == "HOST_DISK_RESERVE_BYTES" else "MAXIMUM") if metric else None,
                profile_code="REQUIRED_METRIC_COVERAGE" if row.subject_digest == coverage and row.condition == "PROFILE_UNRESOLVED" else None)
            result["conditions"].append(item)
        result["entry_held"] = view.entry_held
    except ERRORS:
        result["conditions"] = []
        result["unavailable_code"] = "OPERATIONS_ALERT_STATUS_UNAVAILABLE"
    return result
