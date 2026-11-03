"""Maps every supported inbound payload onto one canonical alert record.

Supported shapes:
  1. Databricks job notification webhook  {event_type: "jobs.on_failure", workspace_id, run: {run_id}, job: {job_id, name}}
  2. Azure Monitor common alert schema    {schemaId: "azureMonitorCommonAlertSchema", data: {essentials, alertContext}}
     (what an Azure Data Factory metric alert such as PipelineFailedRuns sends to a webhook)
  3. Generic JSON from in-house checks    {source, event_type, status, pipeline, environment, message, event_id}

Any other JSON object is kept as source "unknown" (never silently dropped). A body that is not a JSON
object raises UnsupportedPayload.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from .config import Config

DATABRICKS_EVENTS: dict[str, dict[str, str]] = {
    "jobs.on_failure": {"event_type": "failure", "status": "FIRING", "detail": "Job run failed"},
    "jobs.on_duration_warning_threshold_exceeded": {
        "event_type": "sla_breach", "status": "FIRING",
        "detail": "Run exceeded the job duration warning threshold",
    },
    "jobs.on_streaming_backlog_exceeded": {
        "event_type": "sla_breach", "status": "FIRING",
        "detail": "Streaming backlog exceeded the configured threshold",
    },
    # A successful run resolves an earlier failure for the same job.
    "jobs.on_success": {"event_type": "failure", "status": "RESOLVED", "detail": "Job run succeeded"},
    "jobs.on_start": {"event_type": "info", "status": "FIRING", "detail": "Job run started"},
}

# Azure Data Factory metric names -> canonical event type.
AZURE_METRIC_EVENTS = {
    "PipelineFailedRuns": "failure",
    "ActivityFailedRuns": "failure",
    "TriggerFailedRuns": "failure",
    "PipelineElapsedTimeRuns": "sla_breach",
}

ENV_ALIASES = {"prd": "prod", "production": "prod", "development": "dev", "stg": "staging"}
_ENV_IN_NAME = re.compile(
    r"(?:^|[-_.])(prod|prd|production|dev|development|test|qa|uat|stg|staging|sandbox)(?:$|[-_.])", re.I
)

# Names used in the {source} path segment of the webhook URL.
SOURCE_PATHS = {"databricks": "databricks", "azure-monitor": "azure_monitor", "generic": "generic"}


class UnsupportedPayload(ValueError):
    """The request body cannot be turned into an alert."""


def _clean(value: Any, fallback: str = "unknown") -> str:
    text = "" if value is None else str(value).strip()
    return text or fallback


def _env_from_name(name: str | None) -> str | None:
    match = _ENV_IN_NAME.search(name or "")
    if not match:
        return None
    env = match.group(1).lower()
    return ENV_ALIASES.get(env, env)


def _normalise_env(value: Any) -> str:
    env = _clean(value).lower()
    return ENV_ALIASES.get(env, env)


def _iso_or_none(value: Any) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def fnv1a(text: str) -> str:
    """Small, dependency-free 32-bit FNV-1a hash, used only to derive an id when a payload has none."""
    value = 0x811C9DC5
    for byte in text.encode("utf-8"):
        value ^= byte
        value = (value * 0x01000193) & 0xFFFFFFFF
    return f"{value:08x}"


def _stable_json(body: Any) -> str:
    return json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)


def _from_databricks(body: dict[str, Any], config: Config) -> dict[str, Any]:
    event = body.get("event_type")
    mapped = DATABRICKS_EVENTS.get(event, {"event_type": "info", "status": "FIRING", "detail": str(event)})
    job = body.get("job") or {}
    run = body.get("run") or {}
    workspace_id = _clean(body.get("workspace_id"))
    workspace = config.databricks_workspaces.get(workspace_id)
    run_id = None if run.get("run_id") is None else str(run["run_id"])
    job_id = _clean(job.get("job_id"))
    host = workspace.host if workspace else None
    return {
        "source": "databricks",
        "source_label": "Databricks job",
        "source_event": event,
        # Databricks sends no event id; (workspace, job, run, event type) identifies one notification.
        "event_id": f"databricks:{workspace_id}:{job_id}:{run_id or 'na'}:{event}",
        "event_type": mapped["event_type"],
        "status": mapped["status"],
        "pipeline": _clean(job.get("name")),
        "environment": (workspace.environment if workspace else None) or _env_from_name(job.get("name")) or "unknown",
        "run_id": run_id,
        "run_url": f"{host}/jobs/{job_id}/runs/{run_id}" if host and run_id else None,
        # The webhook carries no error text. Enrichment via the Jobs API (runs/get-output) comes later.
        "error_message": mapped["detail"],
        "upstream_severity": None,
        "occurred_at": None,
    }


def _from_azure_monitor(body: dict[str, Any]) -> dict[str, Any]:
    data = body.get("data") or {}
    essentials = data.get("essentials") or {}
    context = data.get("alertContext") or {}
    all_of = (context.get("condition") or {}).get("allOf")
    condition = all_of[0] if isinstance(all_of, list) and all_of else {}
    dims = {d.get("name"): d.get("value") for d in condition.get("dimensions") or [] if isinstance(d, dict)}
    targets = essentials.get("alertTargetIDs") or []
    resource_id = targets[0] if targets else ""
    config_items = essentials.get("configurationItems") or [None]
    resource_name = _clean(resource_id.split("/")[-1] or config_items[0])
    metric = condition.get("metricName")
    is_data_factory = bool(
        re.search(r"microsoft\.datafactory", resource_id, re.I)
        or re.search(r"DataFactory", condition.get("metricNamespace") or "", re.I)
    )
    status = "RESOLVED" if essentials.get("monitorCondition") == "Resolved" else "FIRING"
    custom = data.get("customProperties") or {}

    detail = essentials.get("description") or None
    if not detail and metric:
        threshold = condition.get("threshold")
        detail = " ".join(str(p) for p in (metric, condition.get("operator") or "", "" if threshold is None else threshold) if p != "")
        if condition.get("metricValue") is not None:
            detail += f" (observed {condition['metricValue']})"
        if dims.get("FailureType"):
            detail += f", FailureType={dims['FailureType']}"

    alert_rule = essentials.get("alertRule") or ""
    if metric in AZURE_METRIC_EVENTS:
        event_type = AZURE_METRIC_EVENTS[metric]
    else:
        event_type = "failure" if re.search(r"fail", alert_rule, re.I) else "info"

    return {
        "source": "azure_monitor",
        "source_label": "ADF pipeline" if is_data_factory else "Azure resource",
        "source_event": f"{essentials.get('signalType') or 'Unknown'}/{metric or alert_rule or 'alert'}",
        # Fired and Resolved notifications share alertId, so the status is part of the idempotency key.
        "event_id": f"azure:{essentials.get('alertId') or fnv1a(_stable_json(body))}:{status}",
        "event_type": event_type,
        "status": status,
        "pipeline": _clean(dims.get("Name") or dims.get("PipelineName") or alert_rule),
        "environment": _normalise_env(custom.get("environment") or _env_from_name(resource_name) or "unknown"),
        "run_id": dims.get("RunId"),
        "run_url": f"https://portal.azure.com/#resource{resource_id}/overview" if resource_id else None,
        "error_message": detail,
        "upstream_severity": essentials.get("severity"),
        "occurred_at": _iso_or_none(
            essentials.get("resolvedDateTime") if status == "RESOLVED" else essentials.get("firedDateTime")
        ),
        "resource": resource_name,
        "alert_rule": alert_rule or None,
    }


def _from_generic(body: dict[str, Any]) -> dict[str, Any]:
    source = re.sub(r"[^a-z0-9_.-]+", "_", _clean(body.get("source")).lower())
    status = "RESOLVED" if str(body.get("status") or "FIRING").upper() == "RESOLVED" else "FIRING"
    event_type = body.get("event_type") if body.get("event_type") in ("failure", "sla_breach") else "info"
    event_id = str(body["event_id"]) if body.get("event_id") else fnv1a(_stable_json(body))
    return {
        "source": source,
        "source_label": _clean(body.get("source_label"), "Pipeline"),
        "source_event": body.get("event_type"),
        "event_id": f"{source}:{event_id}",
        "event_type": event_type,
        "status": status,
        "pipeline": _clean(body.get("pipeline")),
        "environment": _normalise_env(body.get("environment") or _env_from_name(body.get("pipeline")) or "unknown"),
        "run_id": None if body.get("run_id") is None else str(body["run_id"]),
        "run_url": body.get("run_url"),
        "error_message": body.get("message") or body.get("error"),
        "upstream_severity": body.get("severity"),
        "occurred_at": _iso_or_none(body.get("occurred_at")),
    }


def _from_unknown(body: dict[str, Any]) -> dict[str, Any]:
    text = _stable_json(body)
    return {
        "source": "unknown",
        "source_label": "Unrecognised sender",
        "source_event": None,
        "event_id": f"unknown:{fnv1a(text)}",
        "event_type": "info",
        "status": "FIRING",
        "pipeline": "unknown",
        "environment": "unknown",
        "run_id": None,
        "run_url": None,
        "error_message": f"Unrecognised payload: {text[:300]}",
        "upstream_severity": None,
        "occurred_at": None,
    }


def detect_source(body: Any) -> str:
    """Which parser a body belongs to: databricks, azure_monitor, generic or unknown."""
    if not isinstance(body, dict):
        raise UnsupportedPayload("Expected a JSON object in the request body")
    event = body.get("event_type")
    if isinstance(event, str) and event.startswith("jobs.") and body.get("job"):
        return "databricks"
    if body.get("schemaId") == "azureMonitorCommonAlertSchema" and (body.get("data") or {}).get("essentials"):
        return "azure_monitor"
    if body.get("source") and body.get("pipeline"):
        return "generic"
    return "unknown"


def normalise(body: Any, config: Config, *, expected: str | None = None,
              received_at: datetime | None = None) -> dict[str, Any]:
    """Turn a webhook body into a canonical alert.

    `expected` is the parser named in the webhook URL (databricks, azure_monitor or generic). When it is
    given, a body of another shape is rejected instead of being guessed at.
    """
    kind = detect_source(body)
    if expected and kind != expected:
        raise UnsupportedPayload(f"Payload is not a {expected} event (detected: {kind})")

    if kind == "databricks":
        alert = _from_databricks(body, config)
    elif kind == "azure_monitor":
        alert = _from_azure_monitor(body)
    elif kind == "generic":
        alert = _from_generic(body)
    else:
        alert = _from_unknown(body)

    alert["fingerprint"] = ":".join(
        [alert["source"], alert["environment"], alert["pipeline"], alert["event_type"]]
    ).lower()
    received = received_at or datetime.now(timezone.utc)
    alert["received_at"] = received.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    alert["raw"] = body
    return alert
