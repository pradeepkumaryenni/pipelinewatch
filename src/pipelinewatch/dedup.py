"""De-duplication: one notification per problem per window.

The decision is a pure function of the incoming alert and the open window for its fingerprint, so the
in-memory and Postgres stores share exactly the same behaviour (each store only adds locking and storage).

Decisions:
    PAGE / NOTIFY / DIGEST  first alert for this fingerprint in the window, or an escalation (route from the rules)
    SUPPRESS                same fingerprint inside the window and not more severe than what was already sent
    RESOLVE                 recovery for a fingerprint with an open HIGH/CRITICAL window (closes it)
    LOG_ONLY                recovery with nothing open (or only LOW): logged, nobody notified
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any

SEVERITY_RANK = {"CRITICAL": 3, "HIGH": 2, "LOW": 1, "INFO": 0}
NOTIFYING = frozenset({"PAGE", "NOTIFY", "DIGEST"})


@dataclass(frozen=True)
class WindowState:
    """The open notification window for one fingerprint."""
    fingerprint: str
    window_started_at: datetime
    last_seen_at: datetime
    occurrences: int
    max_severity: str
    opened_by_alert_id: int | None = None


@dataclass(frozen=True)
class Decision:
    decision: str
    occurrence: int
    window_started_at: datetime | None
    # What to store afterwards: a new/updated window, or None to delete it.
    next_state: WindowState | None
    keep_state: bool  # False means "delete any window for this fingerprint"


def decide(alert: dict[str, Any], state: WindowState | None, now: datetime, window_minutes: int) -> Decision:
    fingerprint, severity = alert["fingerprint"], alert["severity"]

    if alert["status"] == "RESOLVED":
        # A recovery closes the window, so the next failure alerts again.
        was_notified = state is not None and SEVERITY_RANK.get(state.max_severity, 0) >= SEVERITY_RANK["HIGH"]
        return Decision("RESOLVE" if was_notified else "LOG_ONLY", 1,
                        state.window_started_at if state else None, None, keep_state=False)

    in_window = state is not None and state.window_started_at > now - timedelta(minutes=window_minutes)
    escalation = state is not None and SEVERITY_RANK.get(severity, 0) > SEVERITY_RANK.get(state.max_severity, 0)
    if in_window and not escalation:
        occurrence = state.occurrences + 1
        return Decision("SUPPRESS", occurrence, state.window_started_at,
                        replace(state, occurrences=occurrence, last_seen_at=now), keep_state=True)

    # New problem, expired window, or escalation: open a fresh window and notify.
    route = alert.get("route") or "DIGEST"
    return Decision(route, 1, now,
                    WindowState(fingerprint, now, now, 1, severity), keep_state=True)
