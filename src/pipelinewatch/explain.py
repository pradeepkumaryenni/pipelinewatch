"""Plain-English explanations from the failure catalog (catalog/failures.yaml).

`explain(alert)` returns the first catalog entry whose patterns match the alert's error text, or a
fallback entry for the alert's source/event type, or None. Rule-based, so it costs nothing to run.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache
from importlib import resources
from typing import Any

import yaml


@dataclass(frozen=True)
class Failure:
    id: str
    title: str
    cause: str
    fix: tuple[str, ...]
    patterns: tuple[re.Pattern[str], ...] = ()
    sources: frozenset[str] = frozenset()
    event_types: frozenset[str] = frozenset()
    failure_type: str | None = None
    fallback: bool = False
    severity_hint: str | None = None
    examples: tuple[str, ...] = field(default=(), compare=False)

    def applies_to(self, alert: dict[str, Any]) -> bool:
        if self.sources and alert.get("source") not in self.sources:
            return False
        if self.event_types and alert.get("event_type") not in self.event_types:
            return False
        if self.failure_type and failure_type_of(alert) != self.failure_type:
            return False
        return True

    def matches_text(self, text: str) -> bool:
        return any(p.search(text) for p in self.patterns)

    def as_dict(self) -> dict[str, Any]:
        return {"id": self.id, "title": self.title, "cause": self.cause, "fix": list(self.fix)}


def failure_type_of(alert: dict[str, Any]) -> str | None:
    """ADF's FailureType dimension (UserError / SystemError), if the alert came from Azure Monitor."""
    match = re.search(r"FailureType=(\w+)", alert.get("error_message") or "")
    return match.group(1) if match else None


def error_text(alert: dict[str, Any]) -> str:
    """Everything that might describe the error: the normalised message plus common raw fields."""
    raw = alert.get("raw") if isinstance(alert.get("raw"), dict) else {}
    parts = [alert.get("error_message"), raw.get("error"), raw.get("error_code"), raw.get("message")]
    return " | ".join(str(p) for p in parts if p)


def _entry(data: dict[str, Any]) -> Failure:
    return Failure(
        id=data["id"], title=data["title"], cause=" ".join(data["cause"].split()), fix=tuple(data["fix"]),
        patterns=tuple(re.compile(p, re.I) for p in data.get("patterns") or []),
        sources=frozenset(data.get("sources") or []), event_types=frozenset(data.get("event_types") or []),
        failure_type=data.get("failure_type"), fallback=bool(data.get("fallback")),
        severity_hint=data.get("severity_hint"), examples=tuple(data.get("examples") or []),
    )


def load_catalog(text: str | None = None) -> tuple[Failure, ...]:
    if text is None:
        text = resources.files("pipelinewatch").joinpath("catalog/failures.yaml").read_text(encoding="utf-8")
    entries = tuple(_entry(d) for d in (yaml.safe_load(text) or {}).get("failures", []))
    ids = [e.id for e in entries]
    if len(ids) != len(set(ids)):
        raise ValueError("failure catalog has duplicate ids")
    return entries


@lru_cache(maxsize=1)
def default_catalog() -> tuple[Failure, ...]:
    return load_catalog()


def explain(alert: dict[str, Any], catalog: tuple[Failure, ...] | None = None) -> dict[str, Any] | None:
    """Best explanation for a failing or late alert; None for recoveries and info events."""
    if alert.get("status") == "RESOLVED" or alert.get("event_type") not in ("failure", "sla_breach"):
        return None
    catalog = catalog if catalog is not None else default_catalog()
    text = error_text(alert)
    for entry in catalog:
        if not entry.fallback and entry.patterns and entry.applies_to(alert) and entry.matches_text(text):
            return {**entry.as_dict(), "matched_on": "error text"}
    for entry in catalog:
        if entry.fallback and entry.applies_to(alert):
            return {**entry.as_dict(), "matched_on": "source and event type"}
    return None
