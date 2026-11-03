"""Builders for realistic sample alert payloads (adapted from the n8n-dataops-alerts project).

Payload shapes:
  * Databricks job notification webhook (notification destination of type "webhook"):
    {"event_type": "jobs.on_failure", "workspace_id": ..., "run": {"run_id": ...}, "job": {"job_id": ..., "name": ...}}
  * Azure Monitor common alert schema, as sent by an action group webhook for an Azure Data Factory
    metric alert (PipelineFailedRuns / PipelineElapsedTimeRuns).
  * A generic JSON event, as an in-house freshness/SLA checker or an ADF Web activity would post.

All identifiers (workspace ids, subscription id, run ids) are made up for the demo.
"""

from __future__ import annotations

import itertools
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

PROD_WORKSPACE_ID = "1234567890123456"
DEV_WORKSPACE_ID = "2345678901234567"
SUBSCRIPTION_ID = "00000000-0000-0000-0000-000000000000"
RESOURCE_GROUP = "rg-dataops-prod"
DATA_FACTORY = "adf-dataops-prod"

# Job ids for the sample Databricks jobs.
DATABRICKS_JOBS = {"nightly_sales_etl": 101, "customer_360_refresh": 102}


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


_run_counter = itertools.count(int(time.time() * 1000) % 1_000_000_000)


def next_run_id() -> int:
    """Run ids unique across simulator runs, so new events are never mistaken for webhook retries."""
    return next(_run_counter)


def databricks_job_event(event_type: str, job_name: str, *, run_id: int | None = None,
                         workspace_id: str = PROD_WORKSPACE_ID) -> dict[str, Any]:
    return {
        "event_type": event_type,
        "workspace_id": workspace_id,
        "run": {"run_id": run_id if run_id is not None else next_run_id()},
        "job": {"job_id": str(DATABRICKS_JOBS.get(job_name, 199)), "name": job_name},
    }


def azure_monitor_adf_alert(pipeline: str, *, metric: str = "PipelineFailedRuns", condition: str = "Fired",
                            severity: str = "Sev2", alert_guid: str | None = None, rule: str | None = None,
                            failure_type: str = "UserError", fired_at: datetime | None = None,
                            factory: str = DATA_FACTORY) -> dict[str, Any]:
    fired = fired_at or datetime.now(timezone.utc)
    alert_guid = alert_guid or str(uuid.uuid4())
    resource_id = (f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}"
                   f"/providers/Microsoft.DataFactory/factories/{factory}")
    if metric == "PipelineElapsedTimeRuns":
        dimensions = [{"name": "Name", "value": pipeline}, {"name": "RunId", "value": str(uuid.uuid4())}]
        operator, threshold, value = "GreaterThan", "1", 1.0
    else:
        dimensions = [{"name": "Name", "value": pipeline}, {"name": "FailureType", "value": failure_type}]
        operator, threshold, value = "GreaterThan", "0", 1.0
    rule = rule or f"{factory}-{pipeline}-{'sla' if metric == 'PipelineElapsedTimeRuns' else 'failed'}"
    essentials = {
        "alertId": f"/subscriptions/{SUBSCRIPTION_ID}/providers/Microsoft.AlertsManagement/alerts/{alert_guid}",
        "alertRule": rule,
        "severity": severity,
        "signalType": "Metric",
        "monitorCondition": condition,
        "monitoringService": "Platform",
        "alertTargetIDs": [resource_id.lower()],
        "configurationItems": [factory],
        "originAlertId": f"{SUBSCRIPTION_ID}_{RESOURCE_GROUP}_microsoft.insights_metricAlerts_{rule}_{int(alert_guid.replace('-', ''), 16) % 10**9}",
        "firedDateTime": _iso(fired),
        "description": "",
        "essentialsVersion": "1.0",
        "alertContextVersion": "1.0",
    }
    if condition == "Resolved":
        essentials["resolvedDateTime"] = _iso(fired + timedelta(minutes=12))
    return {
        "schemaId": "azureMonitorCommonAlertSchema",
        "data": {
            "essentials": essentials,
            "alertContext": {
                "properties": None,
                "conditionType": "SingleResourceMultipleMetricCriteria",
                "condition": {
                    "windowSize": "PT5M",
                    "allOf": [{
                        "metricName": metric,
                        "metricNamespace": "Microsoft.DataFactory/factories",
                        "operator": operator,
                        "threshold": threshold,
                        "timeAggregation": "Total",
                        "dimensions": dimensions,
                        "metricValue": value,
                        "webTestName": None,
                    }],
                    "windowStartTime": _iso(fired - timedelta(minutes=5)),
                    "windowEndTime": _iso(fired),
                },
            },
            "customProperties": None,
        },
    }


def generic_event(*, source: str, event_type: str, pipeline: str, environment: str, message: str,
                  status: str = "FIRING", event_id: str | None = None, run_id: str | None = None) -> dict[str, Any]:
    return {
        "source": source,
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": event_type,
        "status": status,
        "pipeline": pipeline,
        "environment": environment,
        "run_id": run_id,
        "message": message,
        "occurred_at": _iso(datetime.now(timezone.utc)),
    }


