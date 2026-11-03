"""Notifications to Slack, Microsoft Teams and Telegram.

Which decisions go to which channels is set in the YAML config (`notifications`). Secrets never live in
the config file: a channel is enabled only when its environment variables are set.

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
from dataclasses import dataclass
from typing import Any, Callable, Protocol

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
    if alert.get("run_url"):
        lines.append(f"Run: {alert['run_url']}")
    lines.append(f"Rule: {alert['severity_rule']} · Source: {alert['source_label']}")
    return lines


class Channel(Protocol):
    name: str
    def request(self, alert: dict[str, Any], decision: str) -> tuple[str, dict[str, Any]]: ...


@dataclass
class Slack:
    webhook_url: str
    name: str = "slack"

    def request(self, alert: dict[str, Any], decision: str) -> tuple[str, dict[str, Any]]:
        head = title(alert, decision)
        body = "\n".join(detail_lines(alert))
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

    def request(self, alert: dict[str, Any], decision: str) -> tuple[str, dict[str, Any]]:
        card = {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [{"type": "TextBlock", "text": title(alert, decision), "weight": "Bolder", "wrap": True}]
                    + [{"type": "TextBlock", "text": line, "wrap": True} for line in detail_lines(alert)],
        }
        return self.webhook_url, {"type": "message", "attachments": [
            {"contentType": "application/vnd.microsoft.card.adaptive", "content": card}]}


@dataclass
class Telegram:
    bot_token: str
    chat_id: str
    name: str = "telegram"

    def request(self, alert: dict[str, Any], decision: str) -> tuple[str, dict[str, Any]]:
        text = "\n".join([title(alert, decision), *detail_lines(alert)])
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
    """Sends one alert to the channels configured for its decision, with retries, and logs each delivery."""

    RETRYABLE = frozenset({408, 429, 500, 502, 503, 504})

    def __init__(self, channels: dict[str, Channel], routes: dict[str, list[str]],
                 transport: Transport = urllib_transport, attempts: int = 3,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.channels, self.routes = channels, routes
        self.transport, self.attempts, self.sleep = transport, attempts, sleep

    def targets(self, decision: str) -> list[Channel]:
        return [self.channels[name] for name in self.routes.get(decision, []) if name in self.channels]

    def send(self, alert: dict[str, Any], decision: str, alert_id: int, store: Any) -> list[tuple[str, str]]:
        results = []
        for channel in self.targets(decision):
            url, body = channel.request(alert, decision)
            status, detail = "FAILED", "not attempted"
            for attempt in range(1, self.attempts + 1):
                try:
                    code, text = self.transport(url, body)
                except Exception as exc:  # network errors are retried like 5xx
                    code, text = 0, type(exc).__name__
                if 200 <= code < 300:
                    status, detail = "SENT", f"HTTP {code}"
                    break
                detail = f"HTTP {code}: {text[:120]}" if code else f"error: {text}"
                if code and code not in self.RETRYABLE:
                    break
                if attempt < self.attempts:
                    self.sleep(0.5 * 2 ** (attempt - 1))
            store.log_delivery(alert_id, channel.name, status, detail)
            if status == "FAILED":
                log.warning("Delivery to %s failed for alert %s: %s", channel.name, alert_id, detail)
            results.append((channel.name, status))
        return results
