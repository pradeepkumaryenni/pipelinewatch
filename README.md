# PipelineWatch

Plain-English alerts for failed **Azure Data Factory** and **Databricks** pipelines, built for small data teams that can't justify an enterprise observability tool.

> **Portfolio project, built with AI-assisted development (Claude Code).** All events are simulated with synthetic ids. It is not code from any employer or client. This is v0.2 (week 2 of the roadmap below); see [What is and isn't verified](#what-is-and-isnt-verified).

## What it does (v0.2)

```
Databricks job webhook ─┐                                                    ┌─▶ Slack
Azure Monitor (ADF)  ───┼─▶ POST /webhooks/{source} ─▶ normalise ─▶ classify ─▶ de-duplicate ─▶ notify ─┼─▶ Teams
Generic JSON check   ───┘        (API key)             one shape    severity     one alert per      ├─▶ Telegram
                                                                     + route      problem per window └─▶ delivery log
```

1. **Normalise:** turns a Databricks job notification, an Azure Monitor common-alert-schema payload (what an ADF `PipelineFailedRuns` / `PipelineElapsedTimeRuns` metric alert sends) or a generic JSON check into one alert record, with environment, pipeline, run link and an idempotency key. Unknown JSON is kept, never dropped.
2. **Classify:** ordered rules, first match wins, and the rule id is recorded so every decision can be explained:

   | Rule | When | Severity → route |
   |---|---|---|
   | R1 | Recovery (Resolved / job succeeded) | INFO → RESOLVE |
   | R2 | Non-production environment | LOW → DIGEST |
   | R3 | Tier-1 pipeline failed | CRITICAL → PAGE |
   | R4 | Azure Monitor rule is Sev0/Sev1 | CRITICAL → PAGE |
   | R5 | Any other production failure | HIGH → NOTIFY |
   | R6 | SLA breach on a tier-1/2 pipeline | HIGH → NOTIFY |
   | R7 | Everything else | LOW → DIGEST |

3. **De-duplicate:** the same problem (source + environment + pipeline + event type) inside a 15-minute window notifies once:

   | Decision | When |
   |---|---|
   | PAGE / NOTIFY / DIGEST | first alert in the window, an expired window, or an **escalation** (e.g. a Sev3 failure followed by a Sev1 one) |
   | SUPPRESS | a repeat inside the window that isn't more severe (counted as `occurrence` 2, 3, …) |
   | RESOLVE | a recovery after a HIGH/CRITICAL alert, which closes the window |
   | LOG_ONLY | a recovery when nothing (or only LOW) was open |
   | `replay: true` | the exact same delivery again (Azure Monitor and Databricks both retry webhooks): answered with the original decision, nothing sent |

   The rules live in one pure function (`dedup.decide`) shared by both stores. The Postgres store runs each alert in a transaction under an advisory lock on the fingerprint, so a burst of 20 parallel deliveries still produces exactly one notification (tested).
4. **Notify:** Slack (incoming webhook), Microsoft Teams (Workflows webhook with an Adaptive Card) and Telegram (bot). Delivery happens after the HTTP response, with retries on 429/5xx/network errors, and every attempt's outcome goes to a delivery log.

Workspace ids, pipeline tiers, the dedup window and which decisions notify which channels live in [`config/example.yaml`](config/example.yaml), not in code.

## Run it locally

```bash
python -m venv .venv
.venv/Scripts/activate              # Windows (source .venv/bin/activate on macOS/Linux)
pip install -e ".[dev,postgres]"
pytest -q                           # Postgres tests are skipped unless PIPELINEWATCH_TEST_DATABASE_URL is set

set PIPELINEWATCH_API_KEYS=dev-key  # Windows cmd (export ... on macOS/Linux)
uvicorn pipelinewatch.app:app --reload

# in another terminal: send every demo scenario to a fresh server and check the answers
python -m simulator.send --url http://127.0.0.1:8000 --key dev-key
```

### Storage
In memory by default (lost on restart). For Postgres:
```bash
docker compose up -d
set PIPELINEWATCH_DATABASE_URL=postgresql://pipelinewatch:pipelinewatch@localhost:5432/pipelinewatch
```
The schema ([`schema.sql`](src/pipelinewatch/schema.sql): `alert_log`, `dedup_state`, `delivery_log`) is created on start-up.

### Notifications
A channel sends only when its secrets are set as environment variables (never in the config file):

| Channel | Environment variables |
|---|---|
| Slack | `PIPELINEWATCH_SLACK_WEBHOOK_URL` |
| Teams | `PIPELINEWATCH_TEAMS_WEBHOOK_URL` (a Teams Workflows "post to a channel when a webhook request is received" URL) |
| Telegram | `PIPELINEWATCH_TELEGRAM_BOT_TOKEN` and `PIPELINEWATCH_TELEGRAM_CHAT_ID` |

### API
| Method | Path | Auth | What |
|---|---|---|---|
| GET | `/health` | none | liveness, active store and channels |
| POST | `/webhooks/{source}` | `X-PipelineWatch-Key` header or `?key=` | `source` = `databricks`, `azure-monitor`, `generic` or `auto`. Returns `route`, `decision`, `occurrence`, `replay`. A body that doesn't match the named source gets a 422. |
| GET | `/alerts?limit=50` | key | most recent alerts first (raw payload omitted) |
| GET | `/alerts/{id}/deliveries` | key | notification attempts for one alert |

The `?key=` form exists because Azure Monitor action-group webhooks can't add custom headers. Use HTTPS in production so the key isn't exposed.

## Roadmap
| Version | Adds |
|---|---|
| v0.1 | Normalise + classify + replay detection, simulator, tests, CI |
| **v0.2** (now) | De-duplication windows with escalation and recovery, Postgres store, Slack / Teams / Telegram notifiers with retries and a delivery log |
| v0.3 | **Failure catalog**: plain-English cause + fix steps for common failures (expired credential or Key Vault secret, ADF copy type mismatch, schema drift, missing/late source file, timeouts, Delta concurrent writes, Spark OOM/skew, cluster terminated), daily digest, small dashboard |
| v1.0 | Deployed (Render free tier + Supabase Postgres), demo GIF |
| v1.1 | Microsoft Fabric pipelines and Airflow DAG failures; error-text enrichment via the Databricks Jobs API and ADF activity-runs API |

## What is and isn't verified
- **Verified locally:** 62 tests (normalising, rules, dedup, the in-memory store including a 20-thread burst, notifier payloads and retries, the API). The simulator ran end to end against a local server with a local stand-in for Slack: all 25 steps passed, a second burst inside the window was fully suppressed, and exactly the 9 expected messages were delivered.
- **Verified in CI:** the same tests plus the Postgres store variants (replay, a 20-thread burst under the advisory lock, resolve-then-reopen, delivery log) against Postgres 16.
- **Not yet verified:** real webhooks from a live Databricks workspace or Azure Monitor action group, and real Slack/Teams/Telegram workspaces. The payload formats follow the vendors' documented shapes.

## Credits
The payload builders in `simulator/events.py`, the rules and the dedup design come from my earlier [n8n-dataops-alerts](https://github.com/pradeepkumaryenni/n8n-dataops-alerts) project, re-implemented here as a standalone Python service.

## License
MIT
