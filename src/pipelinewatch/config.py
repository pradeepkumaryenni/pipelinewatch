"""Loads the YAML config (workspace map and pipeline tiers) and the API keys."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "example.yaml"


@dataclass(frozen=True)
class Workspace:
    environment: str
    host: str | None = None


@dataclass(frozen=True)
class Config:
    databricks_workspaces: dict[str, Workspace] = field(default_factory=dict)
    pipeline_tiers: dict[str, int] = field(default_factory=dict)

    def tier(self, pipeline: str) -> int:
        """Business tier of a pipeline; anything not configured is tier 3."""
        return self.pipeline_tiers.get(pipeline, 3)

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "Config":
        data = data or {}
        workspaces = {
            str(ws_id): Workspace(environment=str(ws.get("environment", "unknown")).lower(), host=ws.get("host"))
            for ws_id, ws in (data.get("databricks_workspaces") or {}).items()
        }
        tiers = {str(name): int(tier) for name, tier in (data.get("pipeline_tiers") or {}).items()}
        return cls(databricks_workspaces=workspaces, pipeline_tiers=tiers)

    @classmethod
    def load(cls, path: str | Path | None = None) -> "Config":
        path = Path(path or os.environ.get("PIPELINEWATCH_CONFIG") or DEFAULT_CONFIG)
        with path.open(encoding="utf-8") as f:
            return cls.from_dict(yaml.safe_load(f))


def api_keys_from_env() -> list[str]:
    """Comma-separated keys in PIPELINEWATCH_API_KEYS; blanks are ignored."""
    raw = os.environ.get("PIPELINEWATCH_API_KEYS", "")
    return [key.strip() for key in raw.split(",") if key.strip()]
