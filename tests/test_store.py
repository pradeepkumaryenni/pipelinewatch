"""Store behaviour, run against InMemoryStore and (when configured) PostgresStore."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from pipelinewatch.classify import classify
from pipelinewatch.normalise import normalise
from simulator.events import azure_monitor_adf_alert, databricks_job_event

T0 = datetime(2026, 11, 3, 2, 0, tzinfo=timezone.utc)


def classified(config, body):
    return classify(normalise(body, config), config)


def test_replay_returns_original_decision(store, config):
    a = classified(config, databricks_job_event("jobs.on_failure", "nightly_sales_etl", run_id=1))
    first, second = store.register(a, T0), store.register(a, T0 + timedelta(seconds=5))
    assert (first.decision, first.replay) == ("PAGE", False)
    assert (second.decision, second.replay, second.alert_id) == ("PAGE", True, first.alert_id)


def test_burst_of_identical_problems_notifies_once(store, config):
    alerts = [classified(config, databricks_job_event("jobs.on_failure", "customer_360_refresh", run_id=i))
              for i in range(20)]
    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(lambda a: store.register(a), alerts))
    decisions = sorted(r.decision for r in results)
    assert decisions.count("NOTIFY") == 1 and decisions.count("SUPPRESS") == 19
    assert sorted(r.occurrence for r in results) == list(range(1, 21))


def test_resolve_closes_window_so_next_failure_alerts(store, config):
    guid = "00000000-0000-0000-0000-0000000000aa"
    fired = classified(config, azure_monitor_adf_alert("pl_load_finance_gl", alert_guid=guid))
    resolved = classified(config, azure_monitor_adf_alert("pl_load_finance_gl", alert_guid=guid, condition="Resolved"))
    again = classified(config, azure_monitor_adf_alert("pl_load_finance_gl"))
    assert store.register(fired, T0).decision == "PAGE"
    assert store.register(resolved, T0 + timedelta(minutes=3)).decision == "RESOLVE"
    assert store.register(again, T0 + timedelta(minutes=4)).decision == "PAGE"


def test_recent_and_deliveries(store, config):
    reg = store.register(classified(config, databricks_job_event("jobs.on_failure", "nightly_sales_etl", run_id=9)))
    store.log_delivery(reg.alert_id, "slack", "SENT", "HTTP 200")
    recent = store.recent(5)
    assert recent[0]["alert_id"] == reg.alert_id and recent[0]["decision"] == "PAGE"
    deliveries = store.deliveries(reg.alert_id)
    assert [(d["channel"], d["status"]) for d in deliveries] == [("slack", "SENT")]
