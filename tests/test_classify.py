import pytest

from pipelinewatch.classify import classify
from pipelinewatch.normalise import normalise
from simulator.events import (DEV_WORKSPACE_ID, azure_monitor_adf_alert, databricks_job_event,
                              generic_event)

CASES = [
    ("tier-1 prod failure pages", databricks_job_event("jobs.on_failure", "nightly_sales_etl"),
     "CRITICAL", "R3_tier1_failure", "PAGE"),
    ("tier-2 prod failure notifies", azure_monitor_adf_alert("pl_ingest_crm_accounts", severity="Sev2"),
     "HIGH", "R5_prod_failure", "NOTIFY"),
    ("Sev1 upstream pages even on tier 3", azure_monitor_adf_alert("pl_refresh_marketing_lookup", severity="Sev1"),
     "CRITICAL", "R4_upstream_sev0_sev1", "PAGE"),
    ("dev failure goes to digest",
     databricks_job_event("jobs.on_failure", "nightly_sales_etl", workspace_id=DEV_WORKSPACE_ID),
     "LOW", "R2_non_prod", "DIGEST"),
    ("tier-1 SLA breach notifies",
     generic_event(source="sla_monitor", event_type="sla_breach", pipeline="gold.sales_daily",
                   environment="prod", message="late"),
     "HIGH", "R6_sla_breach_tier1_2", "NOTIFY"),
    ("tier-3 SLA breach goes to digest",
     azure_monitor_adf_alert("pl_refresh_marketing_lookup", metric="PipelineElapsedTimeRuns", severity="Sev3"),
     "LOW", "R7_default_low", "DIGEST"),
    ("recovery resolves", azure_monitor_adf_alert("pl_load_finance_gl", condition="Resolved"),
     "INFO", "R1_recovery", "RESOLVE"),
    ("unknown payload is low", {"alert": "disk usage 91%"}, "LOW", "R7_default_low", "DIGEST"),
]


@pytest.mark.parametrize("label,body,severity,rule,route", CASES, ids=[c[0] for c in CASES])
def test_rules(config, label, body, severity, rule, route):
    result = classify(normalise(body, config), config)
    assert (result["severity"], result["severity_rule"], result["route"]) == (severity, rule, route)


def test_recovery_wins_over_non_prod(config):
    body = databricks_job_event("jobs.on_success", "nightly_sales_etl", workspace_id=DEV_WORKSPACE_ID)
    assert classify(normalise(body, config), config)["route"] == "RESOLVE"


def test_summary_is_readable(config):
    result = classify(normalise(databricks_job_event("jobs.on_failure", "nightly_sales_etl", run_id=7), config), config)
    assert result["summary"] == (
        "[CRITICAL] Databricks job nightly_sales_etl failed in prod (run 7). Tier-1 pipeline failed in production."
    )
    assert result["tier"] == 1
