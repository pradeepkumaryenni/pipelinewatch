"""HTTP API: receive pipeline webhooks, classify them, and list recent alerts.

Run locally:
    PIPELINEWATCH_API_KEYS=dev-key uvicorn pipelinewatch.app:app --reload
"""

from __future__ import annotations

import json
import secrets
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request

from . import __version__
from .classify import classify
from .config import Config, api_keys_from_env
from .normalise import SOURCE_PATHS, UnsupportedPayload, normalise
from .store import InMemoryStore

# Fields returned to the sender; the full alert (including the raw payload) stays in the store.
RESPONSE_FIELDS = ("event_id", "source", "pipeline", "environment", "event_type", "status", "tier",
                   "severity", "severity_rule", "route", "summary", "fingerprint")


def create_app(config: Config | None = None, api_keys: list[str] | None = None,
               store: InMemoryStore | None = None) -> FastAPI:
    config = config or Config.load()
    keys = api_keys if api_keys is not None else api_keys_from_env()
    store = store or InMemoryStore()

    app = FastAPI(title="PipelineWatch", version=__version__)
    app.state.config, app.state.store = config, store

    def require_key(x_pipelinewatch_key: str | None = Header(default=None),
                    key: str | None = Query(default=None, description="For senders that can't set headers")) -> None:
        # Azure Monitor action groups can't add custom headers, so the key may also come as ?key=.
        if not keys:
            raise HTTPException(503, "No API keys configured: set PIPELINEWATCH_API_KEYS")
        supplied = x_pipelinewatch_key or key or ""
        if not any(secrets.compare_digest(supplied, k) for k in keys):
            raise HTTPException(401, "Missing or invalid API key")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    @app.post("/webhooks/{source}", dependencies=[Depends(require_key)])
    async def receive(source: str, request: Request) -> dict[str, Any]:
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
        classified = classify(alert, config)
        replay = store.add(classified)
        if replay:
            classified = store.get(classified["event_id"]) or classified
        return {**{k: classified.get(k) for k in RESPONSE_FIELDS}, "replay": replay}

    @app.get("/alerts", dependencies=[Depends(require_key)])
    def recent_alerts(limit: int = Query(default=50, ge=1, le=500)) -> list[dict[str, Any]]:
        return [{k: a.get(k) for k in RESPONSE_FIELDS + ("received_at", "error_message", "run_url")}
                for a in store.recent(limit)]

    return app


app = create_app()
