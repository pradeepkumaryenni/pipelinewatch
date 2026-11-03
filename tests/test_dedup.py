from datetime import datetime, timedelta, timezone

from pipelinewatch.dedup import WindowState, decide

T0 = datetime(2026, 11, 3, 2, 0, tzinfo=timezone.utc)
FP = "databricks:prod:nightly_sales_etl:failure"


def alert(severity="HIGH", route="NOTIFY", status="FIRING"):
    return {"fingerprint": FP, "severity": severity, "route": route, "status": status}


def window(max_severity="HIGH", started=T0, occurrences=1):
    return WindowState(FP, started, started, occurrences, max_severity, 1)


def test_first_alert_notifies_and_opens_a_window():
    d = decide(alert(), None, T0, 15)
    assert (d.decision, d.occurrence) == ("NOTIFY", 1)
    assert d.next_state.window_started_at == T0 and d.next_state.max_severity == "HIGH"


def test_repeat_inside_window_is_suppressed_and_counted():
    d = decide(alert(), window(occurrences=3), T0 + timedelta(minutes=5), 15)
    assert (d.decision, d.occurrence) == ("SUPPRESS", 4)
    assert d.next_state.occurrences == 4 and d.next_state.window_started_at == T0


def test_lower_severity_repeat_is_suppressed():
    assert decide(alert("LOW", "DIGEST"), window("HIGH"), T0 + timedelta(minutes=1), 15).decision == "SUPPRESS"


def test_escalation_inside_window_notifies_again():
    d = decide(alert("CRITICAL", "PAGE"), window("HIGH"), T0 + timedelta(minutes=1), 15)
    assert d.decision == "PAGE" and d.next_state.max_severity == "CRITICAL"


def test_expired_window_notifies_again():
    assert decide(alert(), window(), T0 + timedelta(minutes=16), 15).decision == "NOTIFY"


def test_recovery_after_notified_failure_resolves_and_closes_window():
    d = decide(alert("INFO", "RESOLVE", "RESOLVED"), window("HIGH"), T0 + timedelta(minutes=5), 15)
    assert d.decision == "RESOLVE" and d.keep_state is False


def test_recovery_with_nothing_open_is_log_only():
    assert decide(alert("INFO", "RESOLVE", "RESOLVED"), None, T0, 15).decision == "LOG_ONLY"


def test_recovery_after_only_low_is_log_only():
    assert decide(alert("INFO", "RESOLVE", "RESOLVED"), window("LOW"), T0, 15).decision == "LOG_ONLY"
