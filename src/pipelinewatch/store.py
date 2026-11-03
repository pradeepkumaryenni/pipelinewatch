"""Alert stores. Both apply the same `dedup.decide`; they differ only in locking and persistence.

    InMemoryStore  default for local runs and tests (lost on restart)
    PostgresStore  in `pg_store.py`, used when PIPELINEWATCH_DATABASE_URL is set
"""

from __future__ import annotations

import itertools
import threading
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Protocol

from .dedup import NOTIFYING, WindowState, decide


@dataclass(frozen=True)
class Registered:
    alert_id: int
    decision: str
    occurrence: int
    replay: bool
    window_started_at: datetime | None
    alert: dict[str, Any]


class Store(Protocol):
    def register(self, alert: dict[str, Any], now: datetime | None = None) -> Registered: ...
    def recent(self, limit: int = 50) -> list[dict[str, Any]]: ...
    def log_delivery(self, alert_id: int, channel: str, status: str, detail: str | None = None) -> None: ...
    def deliveries(self, alert_id: int) -> list[dict[str, Any]]: ...
    def pending_digest(self, limit: int = 500) -> list[dict[str, Any]]: ...
    def mark_digested(self, alert_ids: list[int], when: datetime | None = None) -> None: ...
    def open_windows(self) -> list[WindowState]: ...


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class InMemoryStore:
    def __init__(self, window_minutes: int = 15, max_alerts: int = 5000) -> None:
        self.window_minutes = window_minutes
        self._max = max_alerts
        self._lock = threading.Lock()
        self._ids = itertools.count(1)
        self._by_event: dict[str, dict[str, Any]] = {}
        self._log: list[dict[str, Any]] = []
        self._windows: dict[str, WindowState] = {}
        self._deliveries: list[dict[str, Any]] = []

    def register(self, alert: dict[str, Any], now: datetime | None = None) -> Registered:
        now = now or utcnow()
        with self._lock:  # serialises everything, so a burst still yields exactly one notification
            existing = self._by_event.get(alert["event_id"])
            if existing:
                return Registered(existing["alert_id"], existing["decision"], existing["occurrence"], True,
                                  None, existing)

            state = self._windows.get(alert["fingerprint"])
            result = decide(alert, state, now, self.window_minutes)
            alert_id = next(self._ids)
            row = {**alert, "alert_id": alert_id, "decision": result.decision, "occurrence": result.occurrence}
            self._by_event[alert["event_id"]] = row
            self._log.append(row)
            if len(self._log) > self._max:
                dropped = self._log.pop(0)
                self._by_event.pop(dropped["event_id"], None)

            if not result.keep_state:
                self._windows.pop(alert["fingerprint"], None)
            elif result.next_state is not None:
                next_state = result.next_state
                if result.decision in NOTIFYING:
                    next_state = replace(next_state, opened_by_alert_id=alert_id)
                self._windows[alert["fingerprint"]] = next_state
            return Registered(alert_id, result.decision, result.occurrence, False, result.window_started_at, row)

    def recent(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            return list(reversed(self._log))[:limit]

    def log_delivery(self, alert_id: int, channel: str, status: str, detail: str | None = None) -> None:
        with self._lock:
            self._deliveries.append({"alert_id": alert_id, "channel": channel, "status": status,
                                     "detail": detail, "delivered_at": utcnow()})

    def deliveries(self, alert_id: int) -> list[dict[str, Any]]:
        with self._lock:
            return [d for d in self._deliveries if d["alert_id"] == alert_id]

    def pending_digest(self, limit: int = 500) -> list[dict[str, Any]]:
        """DIGEST alerts not yet included in a digest, oldest first."""
        with self._lock:
            return [r for r in self._log if r["decision"] == "DIGEST" and not r.get("digested_at")][:limit]

    def mark_digested(self, alert_ids: list[int], when: datetime | None = None) -> None:
        when, wanted = when or utcnow(), set(alert_ids)
        with self._lock:
            for row in self._log:
                if row["alert_id"] in wanted:
                    row["digested_at"] = when

    def open_windows(self) -> list[WindowState]:
        with self._lock:
            return sorted(self._windows.values(), key=lambda w: w.window_started_at, reverse=True)
