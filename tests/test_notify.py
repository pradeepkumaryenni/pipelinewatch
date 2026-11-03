from fastapi.testclient import TestClient

from pipelinewatch.app import create_app
from pipelinewatch.classify import classify
from pipelinewatch.normalise import normalise
from pipelinewatch.notify import Notifier, Slack, Teams, Telegram, channels_from_env
from pipelinewatch.store import InMemoryStore
from simulator.events import databricks_job_event

from .conftest import API_KEY, FakeTransport

CHANNELS = {"slack": Slack("https://hooks.slack.example/T1"),
            "teams": Teams("https://teams.example/workflow"),
            "telegram": Telegram("123:ABC", "-1001")}


def page_alert(config):
    return classify(normalise(databricks_job_event("jobs.on_failure", "nightly_sales_etl", run_id=7), config), config)


def test_page_goes_to_all_three_channels_with_readable_text(config):
    transport, store = FakeTransport(), InMemoryStore()
    results = Notifier(CHANNELS, config.notifications, transport, sleep=lambda s: None).send(
        page_alert(config), "PAGE", 1, store)
    assert results == [("slack", "SENT"), ("teams", "SENT"), ("telegram", "SENT")]
    (slack_url, slack), (teams_url, teams), (tg_url, tg) = transport.calls
    assert slack_url == "https://hooks.slack.example/T1"
    assert slack["blocks"][0]["text"]["text"].startswith("🚨 [CRITICAL] nightly_sales_etl needs attention (prod)")
    assert "/jobs/101/runs/7" in slack["text"]
    assert teams["attachments"][0]["contentType"] == "application/vnd.microsoft.card.adaptive"
    assert tg_url == "https://api.telegram.org/bot123:ABC/sendMessage" and tg["chat_id"] == "-1001"
    assert [d["status"] for d in store.deliveries(1)] == ["SENT", "SENT", "SENT"]


def test_notify_route_only_uses_configured_channels(config):
    notifier = Notifier(CHANNELS, config.notifications, FakeTransport())
    assert [c.name for c in notifier.targets("NOTIFY")] == ["slack", "teams"]
    assert notifier.targets("SUPPRESS") == [] and notifier.targets("DIGEST") == []


def test_unconfigured_channel_is_skipped(config):
    notifier = Notifier({"slack": CHANNELS["slack"]}, config.notifications, FakeTransport())
    assert [c.name for c in notifier.targets("PAGE")] == ["slack"]


def test_retries_5xx_and_network_errors_then_succeeds(config):
    transport, sleeps = FakeTransport([503, ConnectionError("down"), 200]), []
    store = InMemoryStore()
    Notifier({"slack": CHANNELS["slack"]}, config.notifications, transport, sleep=sleeps.append).send(
        page_alert(config), "PAGE", 1, store)
    assert len(transport.calls) == 3 and sleeps == [0.5, 1.0]
    assert store.deliveries(1)[0]["status"] == "SENT"


def test_4xx_is_not_retried_and_is_logged_as_failed(config):
    transport, store = FakeTransport([400]), InMemoryStore()
    Notifier({"slack": CHANNELS["slack"]}, config.notifications, transport, sleep=lambda s: None).send(
        page_alert(config), "PAGE", 1, store)
    assert len(transport.calls) == 1
    delivery = store.deliveries(1)[0]
    assert delivery["status"] == "FAILED" and delivery["detail"].startswith("HTTP 400")


def test_channels_from_env_needs_both_telegram_values():
    env = {"PIPELINEWATCH_SLACK_WEBHOOK_URL": "https://x", "PIPELINEWATCH_TELEGRAM_BOT_TOKEN": "t"}
    assert sorted(channels_from_env(env)) == ["slack"]
    env["PIPELINEWATCH_TELEGRAM_CHAT_ID"] = "1"
    assert sorted(channels_from_env(env)) == ["slack", "telegram"]


def test_api_sends_once_for_a_burst_and_not_for_replays(config):
    transport = FakeTransport()
    store = InMemoryStore()
    notifier = Notifier({"slack": CHANNELS["slack"]}, config.notifications, transport, sleep=lambda s: None)
    client = TestClient(create_app(config=config, api_keys=[API_KEY], store=store, notifier=notifier))
    headers = {"X-PipelineWatch-Key": API_KEY}
    first = databricks_job_event("jobs.on_failure", "customer_360_refresh", run_id=1)
    for body in (first, first, databricks_job_event("jobs.on_failure", "customer_360_refresh", run_id=2)):
        client.post("/webhooks/databricks", json=body, headers=headers)
    assert len(transport.calls) == 1  # NOTIFY once; the replay and the SUPPRESS send nothing
    alert_id = client.get("/alerts?limit=1", headers=headers).json()[0]["alert_id"]
    assert client.get(f"/alerts/{alert_id}/deliveries", headers=headers).json() == []  # suppressed one
    assert client.get("/alerts/1/deliveries", headers=headers).json()[0]["status"] == "SENT"
