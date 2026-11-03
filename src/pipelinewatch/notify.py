"""Notifications to Slack, Microsoft Teams and Telegram.

Which decisions go to which channels is set in the YAML config (`notifications`, plus `digest` for the
daily digest). Secrets never live in the config file: a channel is enabled only when its environment
variables are set.

    Slack     PIPELINEWATCH_SLACK_WEBHOOK_URL      (Slack incoming webhook)
    Teams     PIPELINEWATCH_TEAMS_WEBHOOK_URL      (Teams "Workflows" webhook that accepts an Adaptive Card)
    Telegram  PIPELINEWATCH_TELEGRAM_BOT_TOKEN + PIPELINEWATCH_TELEGRAM_CHAT_ID
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Protocol

log = logging.getLogger("pipelinewatch.notify")

# (url, json body) -> (http status, short response text). Swappable in tests.
Transport = Callable[[str, dict[str, Any]], tuple[int, str]]

EMOJI = {"PAGE": "🚨", "NOTIFY": "⚠️", "RESOLVE": "✅", "DIGEST": "📝"}


def urllib_transport(url: str, body: dict[str, Any], timeout: float = 10.0) -> tuple[int, str]:
    request = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read(200).decode("utf-8", "replace")
    except urllib.error.HTTPError as err:
        return err.code, err.read(200).decode("utf-8", "replace")


def title(alert: dict[str, Any], decision: str) -> str:
    verb = {"RESOLVE": "recovered"}.get(decision, "needs attention")
    return f"{EMOJI.get(decision, '')} [{alert['severity']}] {alert['pipeline']} {verb} ({alert['environment']})".strip()


def detail_lines(alert: dict[str, Any]) -> list[str]:
    lines = [alert["summary"]]
    if alert.get("error_message"):
        lines.append(f"Details: {alert['error_message']}")
    explanation = alert.get("explanation")
    if explanation:
        lines.append(f"Likely cause: {explanation['title']}. {explanation['cause']}")
        lines.append("Try first: " + " ".join(f"{i}) {step}" for i, step in enumerate(explanation["fix"][:3], 1)))
    if alert.get("run_url"):
        lines.append(f"Run: {alert['run_url']}")
    lines.append(f"Rule: {alert['severity_rule']} · Source: {alert['source_label']}")
    return lines


def digest_message(alerts: list[dict[str, Any]]) -> tuple[str, list[str]]:
    """One message for many LOW alerts, grouped by pipeline, busiest first."""
    by_pipeline: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for a in alerts:
        by_pipeline[(a["pipeline"], a["environment"])].append(a)
    head = f"{EMOJI['DIGEST']} Daily digest: {len(alerts)} low-priority alert{'s' if len(alerts) != 1 else ''}"
    lines = []
    for (pipeline, env), items in sorted(by_pipeline.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        kinds = Counter(i["event_type"].replace("_", " ") for i in items)
        causes = {i["explanation"]["title"] for i in items if i.get("explanation")}
        line = f"• {pipeline} ({env}): " + ", ".join(f"{n} {k}" for k, n in kinds.most_common())
        if causes:
            line += f". Likely: {'; '.join(sorted(causes))}"
        lines.append(line)
    return head, lines


class Channel(Protocol):
    name: str
    def message(self, head: str, lines: list[str]) -> tuple[str, dict[str, Any]]: ...


@dataclass
class Slack:
    webhook_url: str
    name: str = "slack"

    def message(self, head: str, lines: list[str]) -> tuple[str, dict[str, Any]]:
        body = "\n".join(lines)
        return self.webhook_url, {
            "text": f"{head}\n{body}",  # fallback for notifications
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": head[:150]}},
                {"type": "section", "text": {"type": "mrkdwn", "text": body[:2900]}},
            ],
        }


@dataclass
class Teams:
    webhook_url: str
    name: str = "teams"

    def message(self, head: str, lines: list[str]) -> tuple[str, dict[str, Any]]:
        card = {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [{"type": "TextBlock", "text": head, "weight": "Bolder", "wrap": True}]
                    + [{"type": "TextBlock", "text": line, "wrap": True} for line in lines],
        }
        return self.webhook_url, {"type": "message", "attachments": [
            {"contentType": "application/vnd.microsoft.card.adaptive", "content": card}]}


@dataclass
class Telegram:
    bot_token: str
    chat_id: str
    name: str = "telegram"

    def message(self, head: str, lines: list[str]) -> tuple[str, dict[str, Any]]:
        text = "\n".join([head, *lines])
        return (f"https://api.telegram.org/bot{self.bot_token}/sendMessage",
                {"chat_id": self.chat_id, "text": text[:4000], "disable_web_page_preview": True})


def channels_from_env(env: dict[str, str] | None = None) -> dict[str, Channel]:
    env = os.environ if env is None else env
    found: dict[str, Channel] = {}
    if env.get("PIPELINEWATCH_SLACK_WEBHOOK_URL"):
        found["slack"] = Slack(env["PIPELINEWATCH_SLACK_WEBHOOK_URL"])
    if env.get("PIPELINEWATCH_TEAMS_WEBHOOK_URL"):
        found["teams"] = Teams(env["PIPELINEWATCH_TEAMS_WEBHOOK_URL"])
    if env.get("PIPELINEWATCH_TELEGRAM_BOT_TOKEN") and env.get("PIPELINEWATCH_TELEGRAM_CHAT_ID"):
        found["telegram"] = Telegram(env["PIPELINEWATCH_TELEGRAM_BOT_TOKEN"], env["PIPELINEWATCH_TELEGRAM_CHAT_ID"])
    return found


class Notifier:
    """Sends alerts (and digests) to configured channels, with retries, and logs each delivery."""

    RETRYABLE = frozenset({408, 429, 500, 502, 503, 504})

    def __init__(self, channels: dict[str, Channel], routes: dict[str, list[str]],
                 transport: Transport = urllib_transport, attempts: int = 3,
                 sleep: Callable[[float], None] = time.sleep, digest_channels: Iterable[str] = ()) -> None:
        self.channels, self.routes = channels, routes
        self.digest_channels = [c for c in digest_channels if c in channels]
        self.transport, self.attempts, self.sleep = transport, attempts, sleep

    def targets(self, decision: str) -> list[Channel]:
        return [self.channels[name] for name in self.routes.get(decision, []) if name in self.channels]

    def _deliver(self, channel: Channel, head: str, lines: list[str]) -> tuple[str, str]:
        url, body = channel.message(head, lines)
        status, detail = "FAILED", "not attempted"
        for attempt in range(1, self.attempts + 1):
            try:
                code, text = self.transport(url, body)
            except Exception as exc:  # network errors are retried like 5xx
                code, text = 0, type(exc).__name__
            if 200 <= code < 300:
                return "SENT", f"HTTP {code}"
            detail = f"HTTP {code}: {text[:120]}" if code else f"error: {text}"
            if code and code not in self.RETRYABLE:
                break
            if attempt < self.attempts:
                self.sleep(0.5 * 2 ** (attempt - 1))
        return status, detail

    def send(self, alert: dict[str, Any], decision: str, alert_id: int, store: Any) -> list[tuple[str, str]]:
        results = []
        for channel in self.targets(decision):
            status, detail = self._deliver(channel, title(alert, decision), detail_lines(alert))
            store.log_delivery(alert_id, channel.name, status, detail)
            if status == "FAILED":
                log.warning("Delivery to %s failed for alert %s: %s", channel.name, alert_id, detail)
            results.append((channel.name, status))
        return results

    def send_digest(self, alerts: list[dict[str, Any]], store: Any) -> list[tuple[str, str]]:
        """One digest message per digest channel; each included alert gets a delivery log row."""
        if not alerts:
            return []
        head, lines = digest_message(alerts)
        results = []
        for name in self.digest_channels:
            status, detail = self._deliver(self.channels[name], head, lines)
            for a in alerts:
                store.log_delivery(a["alert_id"], name, status, f"digest: {detail}")
            results.append((name, status))
        return results
