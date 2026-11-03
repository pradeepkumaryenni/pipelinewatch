"""Demo scenarios: each step says which route PipelineWatch should answer, so the simulator doubles as
an end-to-end check against a running server."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .events import (DEV_WORKSPACE_ID, azure_monitor_adf_alert, databricks_job_event, generic_event)


@dataclass
class Step:
    label: str
    payload: Any
    source: str = "auto"              # {source} segment of the webhook URL
    expect_route: str | None = None   # route in the webhook response
    expect_replay: bool = False
    expect_http: int = 200


def build_scenarios() -> dict[str, list[Step]]:
    crm_fired = azure_monitor_adf_alert("pl_ingest_crm_accounts", alert_guid=str(uuid.uuid4()), severity="Sev2")
    gl_guid, gl_fired_at = str(uuid.uuid4()), datetime.now(timezone.utc)
    return {
        "databricks_failure": [
            Step("Databricks job nightly_sales_etl failed (prod, tier 1)",
                 databricks_job_event("jobs.on_failure", "nightly_sales_etl"), "databricks", "PAGE"),
        ],
        "adf_failure": [
            Step("ADF pl_ingest_crm_accounts PipelineFailedRuns fired (prod, tier 2)",
                 crm_fired, "azure-monitor", "NOTIFY"),
            Step("Azure Monitor retries the same webhook delivery",
                 crm_fired, "azure-monitor", "NOTIFY", expect_replay=True),
        ],
        "sla_breach": [
            Step("Databricks nightly_sales_etl exceeded its duration threshold (tier 1)",
                 databricks_job_event("jobs.on_duration_warning_threshold_exceeded", "nightly_sales_etl"),
                 "databricks", "NOTIFY"),
            Step("Freshness check: gold.sales_daily not refreshed by 06:00 (tier 1)",
                 generic_event(source="sla_monitor", event_type="sla_breach", pipeline="gold.sales_daily",
                               environment="prod",
                               message="gold.sales_daily last refreshed 26h ago; SLA is 06:00 IST daily"),
                 "generic", "NOTIFY"),
            Step("ADF pl_refresh_marketing_lookup elapsed-time breach (tier 3)",
                 azure_monitor_adf_alert("pl_refresh_marketing_lookup", metric="PipelineElapsedTimeRuns",
                                         severity="Sev3"), "azure-monitor", "DIGEST"),
        ],
        "non_prod_failure": [
            Step("Databricks nightly_sales_etl failed in the dev workspace",
                 databricks_job_event("jobs.on_failure", "nightly_sales_etl", workspace_id=DEV_WORKSPACE_ID),
                 "databricks", "DIGEST"),
        ],
        "recovery": [
            Step("ADF pl_load_finance_gl failed (prod, tier 1)",
                 azure_monitor_adf_alert("pl_load_finance_gl", alert_guid=gl_guid, fired_at=gl_fired_at,
                                         failure_type="SystemError"), "azure-monitor", "PAGE"),
            Step("ADF pl_load_finance_gl alert resolved",
                 azure_monitor_adf_alert("pl_load_finance_gl", alert_guid=gl_guid, fired_at=gl_fired_at,
                                         condition="Resolved", failure_type="SystemError"),
                 "azure-monitor", "RESOLVE"),
        ],
        "sev1_low_tier": [
            Step("Sev1 ADF alert on a tier-3 pipeline is still paged",
                 azure_monitor_adf_alert("pl_refresh_marketing_lookup", severity="Sev1",
                                         failure_type="SystemError"), "azure-monitor", "PAGE"),
        ],
        "unknown_payload": [
            Step("Unrecognised JSON from some other tool",
                 {"alert": "disk usage 91%", "host": "vm-shir-01"}, "auto", "DIGEST"),
        ],
        "wrong_source": [
            Step("A Databricks payload posted to the azure-monitor URL is rejected",
                 databricks_job_event("jobs.on_failure", "nightly_sales_etl"), "azure-monitor",
                 expect_http=422),
        ],
        "malformed_payload": [
            Step("Body is a JSON array, not an object",
                 [{"event_type": "jobs.on_failure"}], "auto", expect_http=422),
        ],
    }
