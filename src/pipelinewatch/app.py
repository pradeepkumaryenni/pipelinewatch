"""HTTP API: receive pipeline webhooks, classify and de-duplicate them, notify, and list recent alerts.

Run locally:
    PIPELINEWATCH_API_KEYS=dev-key uvicorn pipelinewatch.app:app --reload
Optional environment:
    PIPELINEWATCH_DATABASE_URL   Postgres DSN (otherwise alerts are kept in memory)
    PIPELINEWATCH_CONFIG         path to a YAML config (default config/example.yaml)
    Slack / Teams / Telegram secrets, see notify.py
"""

from __future__ import annotations

import json
import os
import secrets
from typing import Any

from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Query, Request

from . import __version__
from .classify import classify
from .config import Config, api_keys_from_env
from .normalise import SOURCE_PATHS, UnsupportedPayload, normalise
from .notify import Notifier, channels_from_env
from .store import InMemoryStore, Store

# Fields returned to the sender; the full alert (including the raw payload) stays in the store.
RESPONSE_FIELDS = ("event_id", "source", "pipeline", "environment", "event_type", "status", "tier",
                   "severity", "severity_rule", "route", "summary", "fingerprint")


def store_from_env(config: Config) -> Store:
    dsn = os.environ.get("PIPELINEWATCH_DATABASE_URL")
    if not dsn:
        return InMemoryStore(window_minutes=config.dedup_window_minutes)
    from .pg_store import PostgresStore  # optional dependency
    store = PostgresStore(dsn, window_minutes=config.dedup_window_minutes)
    store.init_schema()
    return store


def create_app(config: Config | None = None, api_keys: list[str] | None = None,
               store: Store | None = None, notifier: Notifier | None = None) -> FastAPI:
    config = config or Config.load()
    keys = api_keys if api_keys is not None else api_keys_from_env()
    store = store if store is not None else store_from_env(config)
    notifier = notifier or Notifier(channels_from_env(), config.notifications)

    app = FastAPI(title="PipelineWatch", version=__version__)
    app.state.config, app.state.store, app.state.notifier = config, store, notifier

    def require_key(x_pipelinewatch_key: str | None = Header(default=None),
                    key: str | None = Query(default=None, description="For senders that can't set headers")) -> None:
        # Azure Monitor action groups can't add custom headers, so the key may also come as ?key=.
        if not keys:
            raise HTTPException(503, "No API keys configured: set PIPELINEWATCH_API_KEYS")
        supplied = x_pipelinewatch_key or key or ""
        if not any(secrets.compare_digest(supplied, k) for k in keys):
            raise HTTPException(401, "Missing or invalid API key")

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "version": __version__, "store": type(store).__name__,
                "channels": sorted(notifier.channels)}

    @app.post("/webhooks/{source}", dependencies=[Depends(require_key)])
    async def receive(source: str, request: Request, background: BackgroundTasks) -> dict[str, Any]:
        if source != "auto" and source not in SOURCE_PATHS:
            raise HTTPException(404, f"Unknown source '{source}'. Use one of: auto, {', '.join(SOURCE_PATHS)}")
        try:
            body = json.loads(await request.body())
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise HTTPException(400, "Request body is not valid JSON")
        try:
            alert = normalise(body, config, expected=None if source == "auto" else SOURCE_PATHS[source])
        except UnsupportedPayload as exc:
            raise HTTPException(422, str(exc))

        registered = store.register(classify(alert, config))
        if not registered.replay and notifier.targets(registered.decision):
            # Answer the sender straight away; deliveries (with retries) happen after the response.
            background.add_task(notifier.send, registered.alert, registered.decision, registered.alert_id, store)
        saved = registered.alert
        return {**{k: saved.get(k) for k in RESPONSE_FIELDS}, "alert_id": registered.alert_id,
                "decision": registered.decision, "occurrence": registered.occurrence, "replay": registered.replay}

    @app.get("/alerts", dependencies=[Depends(require_key)])
    def recent_alerts(limit: int = Query(default=50, ge=1, le=500)) -> list[dict[str, Any]]:
        fields = RESPONSE_FIELDS + ("alert_id", "decision", "occurrence", "received_at", "error_message", "run_url")
        return [{k: a.get(k) for k in fields} for a in store.recent(limit)]

    @app.get("/alerts/{alert_id}/deliveries", dependencies=[Depends(require_key)])
    def alert_deliveries(alert_id: int) -> list[dict[str, Any]]:
        return store.deliveries(alert_id)

    return app


def _default_app() -> FastAPI:
    return create_app()


app = _default_app()
