from fastapi.testclient import TestClient

from pipelinewatch.app import create_app
from pipelinewatch.notify import Notifier, Slack, digest_message
from pipelinewatch.store import InMemoryStore
from simulator.events import (DEV_WORKSPACE_ID, azure_monitor_adf_alert, databricks_job_event,
                              generic_event)

from .conftest import API_KEY, FakeTransport

H = {"X-PipelineWatch-Key": API_KEY}


def make_client(config, transport=None, digest=True):
    channels = {"slack": Slack("https://hooks.slack.example/T1")}
    notifier = Notifier(channels, config.notifications, transport or FakeTransport(), sleep=lambda s: None,
                        digest_channels=["slack"] if digest else [])
    store = InMemoryStore()
    return TestClient(create_app(config=config, api_keys=[API_KEY], store=store, notifier=notifier)), store


def post_low_alerts(client):
    # Three dev failures (LOW → DIGEST) across two jobs, plus one prod failure that notifies immediately.
    for run_id, job in ((1, "nightly_sales_etl"), (2, "customer_360_refresh")):
        client.post("/webhooks/databricks", headers=H,
                    json=databricks_job_event("jobs.on_failure", job, run_id=run_id, workspace_id=DEV_WORKSPACE_ID))
    client.post("/webhooks/azure-monitor", headers=H,
                json=azure_monitor_adf_alert("pl_refresh_marketing_lookup", metric="PipelineElapsedTimeRuns",
                                             severity="Sev3"))
    client.post("/webhooks/databricks", headers=H, json=databricks_job_event("jobs.on_failure", "nightly_sales_etl"))


def test_digest_sends_one_message_then_nothing(config):
    transport = FakeTransport()
    client, store = make_client(config, transport)
    post_low_alerts(client)
    immediate = len(transport.calls)  # the prod PAGE

    preview = client.post("/digest/send?dry_run=true", headers=H).json()
    assert preview["alerts"] == 3 and preview["sent"] == [] and len(transport.calls) == immediate

    sent = client.post("/digest/send", headers=H).json()
    assert sent["alerts"] == 3 and sent["sent"] == [["slack", "SENT"]]
    assert len(transport.calls) == immediate + 1
    text = transport.calls[-1][1]["text"]
    assert text.startswith("📝 Daily digest: 3 low-priority alerts")
    assert "nightly_sales_etl (dev): 1 failure" in text

    again = client.post("/digest/send", headers=H).json()
    assert again["alerts"] == 0 and len(transport.calls) == immediate + 1


def test_failed_digest_is_kept_for_next_time(config):
    client, store = make_client(config, FakeTransport([200, 500, 500, 500]))  # PAGE ok, digest fails 3 times
    post_low_alerts(client)
    result = client.post("/digest/send", headers=H).json()
    assert result["sent"] == [["slack", "FAILED"]]
    assert len(store.pending_digest()) == 3  # not marked, so the next scheduled run tries again


def test_digest_without_channel_is_409(config):
    client, _ = make_client(config, digest=False)
    post_low_alerts(client)
    assert client.post("/digest/send", headers=H).status_code == 409


def test_digest_message_groups_by_pipeline():
    alerts = [{"pipeline": "p1", "environment": "dev", "event_type": "failure", "explanation": None},
              {"pipeline": "p1", "environment": "dev", "event_type": "failure",
               "explanation": {"title": "Spark ran out of memory"}},
              {"pipeline": "p2", "environment": "prod", "event_type": "sla_breach"}]
    head, lines = digest_message(alerts)
    assert head.endswith("3 low-priority alerts")
    assert lines[0] == "• p1 (dev): 2 failure. Likely: Spark ran out of memory"
    assert lines[1] == "• p2 (prod): 1 sla breach"


def test_dashboard_shows_incidents_and_causes(config):
    client, _ = make_client(config)
    post_low_alerts(client)
    client.post("/webhooks/generic", headers=H, json=generic_event(
        source="adf_web_activity", event_type="failure", pipeline="pl_orders", environment="prod",
        message="io.delta.exceptions.ConcurrentAppendException: Files were added by a concurrent update."))
    assert client.get("/dashboard").status_code == 401
    html = client.get(f"/dashboard?key={API_KEY}").text
    assert "<title>PipelineWatch</title>" in html
    assert "Two jobs wrote to the same Delta table at once" in html
    open_section = html.split("<h2>Open incidents</h2>")[1].split("</section>")[0]
    assert "<b>nightly_sales_etl</b> (prod)" in open_section          # the CRITICAL prod failure
    assert "customer_360_refresh" not in open_section                # dev LOW alerts wait for the digest
    assert "pl_orders" in html


def test_dashboard_escapes_html(config):
    client, _ = make_client(config)
    client.post("/webhooks/generic", headers=H, json=generic_event(
        source="x", event_type="failure", pipeline="<script>alert(1)</script>", environment="prod", message="m"))
    html = client.get(f"/dashboard?key={API_KEY}").text
    assert "<script>alert(1)</script>" not in html and "&lt;script&gt;" in html
