import pytest

from simulator.events import databricks_job_event
from simulator.scenarios import build_scenarios

SCENARIOS = build_scenarios()


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_simulator_scenarios(client, headers, name):
    """Every demo scenario gets the answer the simulator expects."""
    for step in SCENARIOS[name]:
        response = client.post(f"/webhooks/{step.source}", json=step.payload, headers=headers)
        assert response.status_code == step.expect_http, (step.label, response.text)
        if step.expect_route:
            body = response.json()
            assert body["route"] == step.expect_route, step.label
            assert body["replay"] is step.expect_replay, step.label


def test_health_needs_no_key(client):
    assert client.get("/health").json()["status"] == "ok"


@pytest.mark.parametrize("hdrs", [{}, {"X-PipelineWatch-Key": "wrong"}])
def test_webhook_rejects_missing_or_wrong_key(client, hdrs):
    response = client.post("/webhooks/auto", json=databricks_job_event("jobs.on_failure", "x"), headers=hdrs)
    assert response.status_code == 401


def test_key_can_be_passed_as_query_param(client):
    # Azure Monitor action groups can't set headers.
    response = client.post("/webhooks/auto?key=test-key", json=databricks_job_event("jobs.on_failure", "x"))
    assert response.status_code == 200


def test_no_keys_configured_refuses_requests(config):
    from fastapi.testclient import TestClient
    from pipelinewatch.app import create_app

    response = TestClient(create_app(config=config, api_keys=[])).post("/webhooks/auto", json={"a": 1})
    assert response.status_code == 503


def test_invalid_json_is_400(client, headers):
    response = client.post("/webhooks/auto", content=b"{not json", headers={**headers, "Content-Type": "application/json"})
    assert response.status_code == 400


def test_unknown_source_path_is_404(client, headers):
    assert client.post("/webhooks/jenkins", json={"a": 1}, headers=headers).status_code == 404


def test_replay_returns_the_original_decision(client, headers):
    payload = databricks_job_event("jobs.on_failure", "nightly_sales_etl", run_id=5)
    first = client.post("/webhooks/databricks", json=payload, headers=headers).json()
    second = client.post("/webhooks/databricks", json=payload, headers=headers).json()
    assert first["replay"] is False and second["replay"] is True
    assert first["event_id"] == second["event_id"] and second["route"] == "PAGE"


def test_alerts_lists_most_recent_first(client, headers):
    for run_id in (1, 2):
        client.post("/webhooks/databricks", json=databricks_job_event("jobs.on_failure", "nightly_sales_etl",
                                                                       run_id=run_id), headers=headers)
    alerts = client.get("/alerts?limit=10", headers=headers).json()
    assert [a["event_id"].split(":")[3] for a in alerts] == ["2", "1"]
    assert "raw" not in alerts[0]
