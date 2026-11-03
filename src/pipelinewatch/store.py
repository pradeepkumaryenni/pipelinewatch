"""Alert store. Week 1 keeps alerts in memory; Postgres with a dedup window replaces it in v0.2."""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any


class InMemoryStore:
    """Keeps the most recent alerts keyed by event id, so webhook retries are recognised as replays."""

    def __init__(self, max_alerts: int = 1000) -> None:
        self._alerts: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._max = max_alerts
        self._lock = threading.Lock()

    def add(self, alert: dict[str, Any]) -> bool:
        """Store an alert. Returns True if this event id was already stored (a replayed delivery)."""
        with self._lock:
            event_id = alert["event_id"]
            if event_id in self._alerts:
                return True
            self._alerts[event_id] = alert
            while len(self._alerts) > self._max:
                self._alerts.popitem(last=False)
            return False

    def get(self, event_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._alerts.get(event_id)

    def recent(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            return list(reversed(self._alerts.values()))[:limit]
