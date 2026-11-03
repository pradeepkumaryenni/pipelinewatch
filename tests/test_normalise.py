import pytest

from pipelinewatch.normalise import UnsupportedPayload, detect_source, fnv1a, normalise
from simulator.events import (DEV_WORKSPACE_ID, azure_monitor_adf_alert, databricks_job_event,
                              generic_event)


def test_databricks_failure_maps_workspace_to_prod_and_builds_run_link(config):
    alert = normalise(databricks_job_event("jobs.on_failure", "nightly_sales_etl", run_id=42), config)
    assert alert["source"] == "databricks"
    assert alert["event_type"] == "failure" and alert["status"] == "FIRING"
    assert alert["environment"] == "prod"
    assert alert["pipeline"] == "nightly_sales_etl"
    assert alert["run_url"].endswith("/jobs/101/runs/42")
    assert alert["event_id"] == "databricks:1234567890123456:101:42:jobs.on_failure"
    assert alert["fingerprint"] == "databricks:prod:nightly_sales_etl:failure"


def test_databricks_dev_workspace_and_success_resolves(config):
    alert = normalise(databricks_job_event("jobs.on_success", "nightly_sales_etl",
                                           workspace_id=DEV_WORKSPACE_ID), config)
    assert alert["environment"] == "dev"
    assert alert["status"] == "RESOLVED"


def test_unknown_workspace_falls_back_to_name(config):
    body = databricks_job_event("jobs.on_failure", "orders_load_prod", workspace_id="999")
    assert normalise(body, config)["environment"] == "prod"


def test_azure_monitor_fired_and_resolved_get_different_event_ids(config):
    fired = normalise(azure_monitor_adf_alert("pl_load_finance_gl", alert_guid="00000000-0000-0000-0000-000000000001"), config)
    resolved = normalise(azure_monitor_adf_alert("pl_load_finance_gl", alert_guid="00000000-0000-0000-0000-000000000001",
                                                 condition="Resolved"), config)
    assert fired["event_id"] != resolved["event_id"]
    assert fired["fingerprint"] == resolved["fingerprint"]
    assert fired["source_label"] == "ADF pipeline"
    assert fired["environment"] == "prod"  # from the factory name adf-dataops-prod
    assert fired["upstream_severity"] == "Sev2"
    assert "FailureType=UserError" in fired["error_message"]
    assert resolved["status"] == "RESOLVED"


def test_azure_elapsed_time_is_an_sla_breach(config):
    alert = normalise(azure_monitor_adf_alert("pl_x", metric="PipelineElapsedTimeRuns"), config)
    assert alert["event_type"] == "sla_breach"


def test_generic_event(config):
    alert = normalise(generic_event(source="SLA Monitor", event_type="sla_breach", pipeline="gold.sales_daily",
                                    environment="production", message="late", event_id="abc"), config)
    assert alert["source"] == "sla_monitor"
    assert alert["environment"] == "prod"
    assert alert["event_id"] == "sla_monitor:abc"
    assert alert["error_message"] == "late"


def test_unknown_payload_is_kept_not_dropped(config):
    alert = normalise({"alert": "disk usage 91%"}, config)
    assert alert["source"] == "unknown"
    assert "disk usage 91%" in alert["error_message"]


@pytest.mark.parametrize("body", [[1, 2], "text", 3, None])
def test_non_object_bodies_are_rejected(config, body):
    with pytest.raises(UnsupportedPayload):
        normalise(body, config)


def test_expected_source_mismatch_is_rejected(config):
    with pytest.raises(UnsupportedPayload, match="azure_monitor"):
        normalise(databricks_job_event("jobs.on_failure", "x"), config, expected="azure_monitor")


def test_detect_source():
    assert detect_source(databricks_job_event("jobs.on_start", "x")) == "databricks"
    assert detect_source(azure_monitor_adf_alert("x")) == "azure_monitor"
    assert detect_source({"source": "s", "pipeline": "p"}) == "generic"
    assert detect_source({"foo": 1}) == "unknown"


def test_fnv1a_matches_reference_values():
    # Reference vectors for 32-bit FNV-1a.
    assert fnv1a("") == "811c9dc5"
    assert fnv1a("a") == "e40c292c"
    assert fnv1a("foobar") == "bf9cf968"
