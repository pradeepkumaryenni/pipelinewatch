import pytest

from pipelinewatch.classify import classify
from pipelinewatch.explain import default_catalog, explain, load_catalog
from pipelinewatch.normalise import normalise
from simulator.events import azure_monitor_adf_alert, databricks_job_event, generic_event

CATALOG = default_catalog()
SPECIFIC = [e for e in CATALOG if not e.fallback]


def generic_failure(message, config, event_type="failure"):
    body = generic_event(source="adf_web_activity", event_type=event_type, pipeline="pl_orders",
                         environment="prod", message=message)
    return classify(normalise(body, config), config)


def test_catalog_shape():
    assert len(CATALOG) >= 15
    for entry in CATALOG:
        assert entry.title and entry.cause, entry.id
        assert len(entry.fix) >= 2, f"{entry.id}: give at least two things to try"
        assert entry.examples, f"{entry.id}: add examples so the catalog tests itself"
        assert entry.patterns or entry.fallback, f"{entry.id}: needs patterns or fallback: true"


@pytest.mark.parametrize("entry", SPECIFIC, ids=[e.id for e in SPECIFIC])
def test_every_example_matches_its_own_entry_first(config, entry):
    """Each example error text resolves to this entry, i.e. no earlier entry steals it."""
    for example in entry.examples:
        result = explain(generic_failure(example, config))
        assert result and result["id"] == entry.id, f"{example!r} -> {result and result['id']}"
        assert result["matched_on"] == "error text"


def test_fallbacks(config):
    def explained(body):
        return explain(classify(normalise(body, config), config))["id"]

    assert explained(azure_monitor_adf_alert("pl_x", failure_type="UserError")) == "adf_user_error"
    assert explained(azure_monitor_adf_alert("pl_x", failure_type="SystemError")) == "adf_system_error"
    assert explained(databricks_job_event("jobs.on_failure", "nightly_sales_etl")) == "databricks_job_failed"
    assert explained(generic_event(source="sla_monitor", event_type="sla_breach", pipeline="gold.sales_daily",
                                   environment="prod", message="last refreshed 26h ago")) == "freshness_sla_breach"
    assert explained(databricks_job_event("jobs.on_duration_warning_threshold_exceeded", "x")) == "freshness_sla_breach"


def test_specific_text_beats_fallback_for_sla_events(config):
    result = explain(generic_failure("Operation on target Copy_Orders failed: The operation has timed out.",
                                     config, event_type="sla_breach"))
    assert result["id"] == "timeout"


def test_no_explanation_for_recoveries_info_or_unmatched(config):
    resolved = azure_monitor_adf_alert("pl_x", condition="Resolved")
    assert explain(classify(normalise(resolved, config), config)) is None
    assert explain(classify(normalise({"alert": "disk 91%"}, config), config)) is None
    assert explain(generic_failure("something odd happened", config)) is None  # generic source, no fallback


def test_duplicate_ids_are_rejected():
    text = "failures:\n" + "  - {id: a, title: t, cause: c, fix: [x, y], fallback: true}\n" * 2
    with pytest.raises(ValueError, match="duplicate"):
        load_catalog(text)
