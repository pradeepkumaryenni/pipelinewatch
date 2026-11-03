"""Demo scenarios: each step says which route and final decision PipelineWatch should answer, so the
simulator doubles as an end-to-end check. Expectations assume a fresh server (empty store)."""

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
    source: str = "auto"                # {source} segment of the webhook URL
    expect_route: str | None = None     # what the severity rules decided
    expect_decision: str | None = None  # final decision after de-duplication
    expect_replay: bool = False
    expect_http: int = 200


def build_scenarios() -> dict[str, list[Step]]:
    crm_fired = azure_monitor_adf_alert("pl_ingest_crm_accounts", alert_guid=str(uuid.uuid4()), severity="Sev2")
    gl_guid, gl_fired_at = str(uuid.uuid4()), datetime.now(timezone.utc)
    return {
        "databricks_failure": [
            Step("Databricks job nightly_sales_etl failed (prod, tier 1)",
                 databricks_job_event("jobs.on_failure", "nightly_sales_etl"), "databricks", "PAGE", "PAGE"),
        ],
        "adf_failure": [
            Step("ADF pl_ingest_crm_accounts PipelineFailedRuns fired (prod, tier 2)",
                 crm_fired, "azure-monitor", "NOTIFY", "NOTIFY"),
            Step("Azure Monitor retries the same webhook delivery",
                 crm_fired, "azure-monitor", "NOTIFY", "NOTIFY", expect_replay=True),
        ],
        "duplicate_burst": [
            Step(f"customer_360_refresh failure #{i} (prod, tier 2)",
                 databricks_job_event("jobs.on_failure", "customer_360_refresh"), "databricks", "NOTIFY",
                 "NOTIFY" if i == 1 else "SUPPRESS")
            for i in range(1, 6)
        ],
        "escalation": [
            Step("pl_refresh_marketing_lookup failed with a Sev3 rule (prod, tier 3)",
                 azure_monitor_adf_alert("pl_refresh_marketing_lookup", severity="Sev3"),
                 "azure-monitor", "NOTIFY", "NOTIFY"),
            Step("Same pipeline, same window, but now a Sev1 rule fires: escalates and pages",
                 azure_monitor_adf_alert("pl_refresh_marketing_lookup", severity="Sev1"),
                 "azure-monitor", "PAGE", "PAGE"),
        ],
        "sla_breach": [
            Step("Databricks nightly_sales_etl exceeded its duration threshold (tier 1)",
                 databricks_job_event("jobs.on_duration_warning_threshold_exceeded", "nightly_sales_etl"),
                 "databricks", "NOTIFY", "NOTIFY"),
            Step("Freshness check: gold.sales_daily not refreshed by 06:00 (tier 1)",
                 generic_event(source="sla_monitor", event_type="sla_breach", pipeline="gold.sales_daily",
                               environment="prod",
                               message="gold.sales_daily last refreshed 26h ago; SLA is 06:00 IST daily"),
                 "generic", "NOTIFY", "NOTIFY"),
            Step("ADF pl_refresh_marketing_lookup elapsed-time breach (tier 3)",
                 azure_monitor_adf_alert("pl_refresh_marketing_lookup", metric="PipelineElapsedTimeRuns",
                                         severity="Sev3"), "azure-monitor", "DIGEST", "DIGEST"),
        ],
        "non_prod_failure": [
            Step("Databricks nightly_sales_etl failed in the dev workspace",
                 databricks_job_event("jobs.on_failure", "nightly_sales_etl", workspace_id=DEV_WORKSPACE_ID),
                 "databricks", "DIGEST", "DIGEST"),
        ],
        "recovery": [
            Step("ADF pl_load_finance_gl failed (prod, tier 1)",
                 azure_monitor_adf_alert("pl_load_finance_gl", alert_guid=gl_guid, fired_at=gl_fired_at,
                                         failure_type="SystemError"), "azure-monitor", "PAGE", "PAGE"),
            Step("ADF pl_load_finance_gl alert resolved",
                 azure_monitor_adf_alert("pl_load_finance_gl", alert_guid=gl_guid, fired_at=gl_fired_at,
                                         condition="Resolved", failure_type="SystemError"),
                 "azure-monitor", "RESOLVE", "RESOLVE"),
            Step("Dev nightly_sales_etl succeeded (only a LOW digest was open, so log only)",
                 databricks_job_event("jobs.on_success", "nightly_sales_etl", workspace_id=DEV_WORKSPACE_ID),
                 "databricks", "RESOLVE", "LOG_ONLY"),
        ],
        "unknown_payload": [
            Step("Unrecognised JSON from some other tool",
                 {"alert": "disk usage 91%", "host": "vm-shir-01"}, "auto", "DIGEST", "DIGEST"),
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
