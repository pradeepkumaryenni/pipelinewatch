"""Severity rules. Ordered, first match wins, and every decision records which rule made it.

    severity  -> route
    CRITICAL  -> PAGE     notify on-call immediately
    HIGH      -> NOTIFY   team channel
    LOW       -> DIGEST   daily digest
    INFO      -> RESOLVE  recovery events
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .config import Config

NON_PROD = frozenset({"dev", "test", "qa", "uat", "staging", "sandbox"})
ROUTE_BY_SEVERITY = {"CRITICAL": "PAGE", "HIGH": "NOTIFY", "LOW": "DIGEST", "INFO": "RESOLVE"}


@dataclass(frozen=True)
class Rule:
    id: str
    severity: str
    reason: str
    when: Callable[[dict[str, Any]], bool]


RULES: tuple[Rule, ...] = (
    Rule("R1_recovery", "INFO", "Recovery event, closes any open alert for the same pipeline",
         lambda a: a["status"] == "RESOLVED"),
    Rule("R2_non_prod", "LOW", "Non-production environment",
         lambda a: a["environment"] in NON_PROD),
    Rule("R3_tier1_failure", "CRITICAL", "Tier-1 pipeline failed in production",
         lambda a: a["event_type"] == "failure" and a["tier"] == 1),
    Rule("R4_upstream_sev0_sev1", "CRITICAL", "Azure Monitor alert rule is Sev0/Sev1",
         lambda a: a["event_type"] != "info" and a.get("upstream_severity") in ("Sev0", "Sev1")),
    Rule("R5_prod_failure", "HIGH", "Production pipeline failure",
         lambda a: a["event_type"] == "failure"),
    Rule("R6_sla_breach_tier1_2", "HIGH", "SLA breach on a tier-1/2 pipeline",
         lambda a: a["event_type"] == "sla_breach" and a["tier"] <= 2),
    Rule("R7_default_low", "LOW", "Low-impact or informational event",
         lambda a: True),
)


def _describe(alert: dict[str, Any]) -> str:
    if alert["status"] == "RESOLVED":
        return "recovered"
    if alert["event_type"] == "failure":
        return "failed"
    if alert["event_type"] == "sla_breach":
        return "breached its SLA"
    return "reported an event"


def classify(alert: dict[str, Any], config: Config) -> dict[str, Any]:
    """Return a copy of the alert with tier, severity, the matching rule, route and a one-line summary."""
    ctx = {**alert, "tier": config.tier(alert["pipeline"])}
    rule = next(r for r in RULES if r.when(ctx))
    run = f" (run {alert['run_id']})" if alert.get("run_id") else ""
    return {
        **ctx,
        "severity": rule.severity,
        "severity_rule": rule.id,
        "severity_reason": rule.reason,
        "route": ROUTE_BY_SEVERITY[rule.severity],
        "summary": (
            f"[{rule.severity}] {alert['source_label']} {alert['pipeline']} {_describe(alert)} "
            f"in {alert['environment']}{run}. {rule.reason}."
        ),
    }
