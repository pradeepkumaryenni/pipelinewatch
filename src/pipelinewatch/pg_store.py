"""Postgres store (psycopg 3). Install with `pip install -e ".[postgres]"`.

Each `register` call runs in one transaction under an advisory lock on the fingerprint, so concurrent
deliveries of the same problem are serialised and a burst still produces exactly one notification.
The decision itself comes from `dedup.decide`, the same function the in-memory store uses.
"""

from __future__ import annotations

import json
from datetime import datetime
from importlib import resources
from typing import Any

from .dedup import NOTIFYING, WindowState, decide
from .store import Registered, utcnow

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb
except ImportError as exc:  # pragma: no cover - only hit without the optional dependency
    raise ImportError('PostgresStore needs psycopg: pip install -e ".[postgres]"') from exc

ALERT_COLUMNS = ("event_id", "fingerprint", "received_at", "occurred_at", "source", "event_type", "status",
                 "severity", "pipeline", "environment", "decision", "occurrence", "severity_rule", "summary")


class PostgresStore:
    def __init__(self, dsn: str, window_minutes: int = 15) -> None:
        self.dsn = dsn
        self.window_minutes = window_minutes

    def _connect(self) -> "psycopg.Connection[Any]":
        return psycopg.connect(self.dsn, row_factory=dict_row)

    def init_schema(self) -> None:
        sql = resources.files("pipelinewatch").joinpath("schema.sql").read_text(encoding="utf-8")
        with self._connect() as conn:
            conn.execute(sql)

    def register(self, alert: dict[str, Any], now: datetime | None = None) -> Registered:
        now = now or utcnow()
        fp = alert["fingerprint"]
        with self._connect() as conn, conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (fp,))

            existing = conn.execute(
                "SELECT alert_id, decision, occurrence, alert FROM pipelinewatch.alert_log WHERE event_id = %s",
                (alert["event_id"],)).fetchone()
            if existing:
                return Registered(existing["alert_id"], existing["decision"], existing["occurrence"], True,
                                  None, existing["alert"])

            row = conn.execute("SELECT * FROM pipelinewatch.dedup_state WHERE fingerprint = %s", (fp,)).fetchone()
            state = WindowState(**row) if row else None
            result = decide(alert, state, now, self.window_minutes)

            values = {**{c: alert.get(c) for c in ALERT_COLUMNS},
                      "received_at": now, "decision": result.decision, "occurrence": result.occurrence}
            stored = {**alert, "decision": result.decision, "occurrence": result.occurrence}
            alert_id = conn.execute(
                f"INSERT INTO pipelinewatch.alert_log ({', '.join(ALERT_COLUMNS)}, alert) "
                f"VALUES ({', '.join(['%s'] * len(ALERT_COLUMNS))}, %s) RETURNING alert_id",
                (*[values[c] for c in ALERT_COLUMNS], Jsonb(json.loads(json.dumps(stored, default=str)))),
            ).fetchone()["alert_id"]

            if not result.keep_state:
                conn.execute("DELETE FROM pipelinewatch.dedup_state WHERE fingerprint = %s", (fp,))
            elif result.next_state is not None:
                s = result.next_state
                opened_by = alert_id if result.decision in NOTIFYING else s.opened_by_alert_id
                conn.execute(
                    """INSERT INTO pipelinewatch.dedup_state AS d
                           (fingerprint, window_started_at, last_seen_at, occurrences, max_severity, opened_by_alert_id)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT (fingerprint) DO UPDATE SET
                           window_started_at = EXCLUDED.window_started_at, last_seen_at = EXCLUDED.last_seen_at,
                           occurrences = EXCLUDED.occurrences, max_severity = EXCLUDED.max_severity,
                           opened_by_alert_id = EXCLUDED.opened_by_alert_id""",
                    (s.fingerprint, s.window_started_at, s.last_seen_at, s.occurrences, s.max_severity, opened_by))
            stored["alert_id"] = alert_id
            return Registered(alert_id, result.decision, result.occurrence, False, result.window_started_at, stored)

    def recent(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT alert_id, alert FROM pipelinewatch.alert_log ORDER BY alert_id DESC LIMIT %s",
                                (limit,)).fetchall()
        return [{**r["alert"], "alert_id": r["alert_id"]} for r in rows]

    def log_delivery(self, alert_id: int, channel: str, status: str, detail: str | None = None) -> None:
        with self._connect() as conn:
            conn.execute("INSERT INTO pipelinewatch.delivery_log (alert_id, channel, status, detail) "
                         "VALUES (%s, %s, %s, %s)", (alert_id, channel, status, detail))

    def deliveries(self, alert_id: int) -> list[dict[str, Any]]:
        with self._connect() as conn:
            return conn.execute("SELECT alert_id, channel, status, detail, delivered_at FROM pipelinewatch.delivery_log "
                                "WHERE alert_id = %s ORDER BY delivery_id", (alert_id,)).fetchall()

    def reset(self) -> None:
        """Delete all data (tests and demos only)."""
        with self._connect() as conn:
            conn.execute("TRUNCATE pipelinewatch.delivery_log, pipelinewatch.alert_log, pipelinewatch.dedup_state "
                         "RESTART IDENTITY")
