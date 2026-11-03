# PipelineWatch

Plain-English alerts for failed **Azure Data Factory** and **Databricks** pipelines, built for small data teams that can't justify an enterprise observability tool.

When a pipeline fails, PipelineWatch tells the team **what failed, how bad it is, the likely cause and what to try first**, once per problem rather than once per retry.

> **Portfolio project, built with AI-assisted development (Claude Code).** All events are simulated with synthetic ids. It is not code from any employer or client. This is v0.3 (week 3 of the roadmap below); see [What is and isn't verified](#what-is-and-isnt-verified).

![PipelineWatch dashboard after a simulator run](docs/dashboard.png)

## What it does (v0.3)

```
Databricks job webhook ─┐                                                                       ┌─▶ Slack
Azure Monitor (ADF)  ───┼─▶ POST /webhooks/{source} ─▶ normalise ─▶ classify ─▶ explain ─▶ de-duplicate ─▶ notify ─┼─▶ Teams
Generic JSON check   ───┘        (API key)            one shape    severity   failure     one alert per          ├─▶ Telegram
                                                                    + route    catalog     problem per window     └─▶ delivery log
                                     LOW alerts ─▶ daily digest (POST /digest/send)        dashboard (GET /dashboard)
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

3. **Explain:** the [failure catalog](src/pipelinewatch/catalog/failures.yaml) maps error text to a plain-English cause and the first things to try. It covers 13 specific failures:
   - expired credentials and Key Vault secrets
   - values that don't fit the column type
   - schema drift
   - duplicate keys in MERGE
   - concurrent Delta writes
   - Spark out of memory
   - cluster terminated or failed to start
   - permission denied
   - missing source file
   - self-hosted integration runtime offline
   - network and firewall errors
   - throttling
   - timeouts

   It also has fallbacks for ADF user/system errors, Databricks failures and late-data SLA breaches. **The catalog tests itself:** every specific entry carries real-looking example errors, and the test suite checks each one is matched by that entry and not taken by an earlier one. It's rule-based, so it costs nothing to run.
4. **De-duplicate:** the same problem (source + environment + pipeline + event type) inside a 15-minute window notifies once:

   | Decision | When |
   |---|---|
   | PAGE / NOTIFY / DIGEST | first alert in the window, an expired window, or an **escalation** (e.g. a Sev3 failure followed by a Sev1 one) |
   | SUPPRESS | a repeat inside the window that isn't more severe (counted as `occurrence` 2, 3, …) |
   | RESOLVE | a recovery after a HIGH/CRITICAL alert, which closes the window |
   | LOG_ONLY | a recovery when nothing (or only LOW) was open |
   | `replay: true` | the exact same delivery again (Azure Monitor and Databricks both retry webhooks): answered with the original decision, nothing sent |

   The rules live in one pure function (`dedup.decide`) shared by both stores. The Postgres store runs each alert in a transaction under an advisory lock on the fingerprint, so a burst of 20 parallel deliveries still produces exactly one notification (tested).
5. **Notify:** Slack (incoming webhook), Microsoft Teams (Workflows webhook with an Adaptive Card) and Telegram (bot). Each message includes the likely cause and the first three things to try. Delivery happens after the HTTP response, with retries on 429/5xx/network errors, and every attempt's outcome goes to a delivery log.
6. **Daily digest:** LOW alerts (dev environments, low-tier SLA misses, unknown senders) don't interrupt anyone. `POST /digest/send` sends them as **one** message grouped by pipeline, and marks them as sent only if delivery succeeded, so a failed digest is retried next time. Call it once a day from any scheduler (cron, GitHub Actions, Render cron). `?dry_run=true` previews it.
7. **Dashboard:** `GET /dashboard` shows the last 24 hours: counts by decision, open HIGH/CRITICAL incidents, repeat offenders, top likely causes and recent alerts.

Workspace ids, pipeline tiers, the dedup window, which decisions notify which channels, and the digest channels all live in [`config/example.yaml`](config/example.yaml), not in code.

## Run it locally

```bash
python -m venv .venv
.venv/Scripts/activate              # Windows (source .venv/bin/activate on macOS/Linux)
pip install -e ".[dev,postgres]"
pytest -q                           # Postgres tests are skipped unless PIPELINEWATCH_TEST_DATABASE_URL is set

python scripts/demo.py              # full demo on free local ports: simulator + digest + dashboard screenshot
```

Or run the pieces yourself:
```bash
set PIPELINEWATCH_API_KEYS=dev-key  # Windows cmd (export ... on macOS/Linux)
uvicorn pipelinewatch.app:app --reload
python -m simulator.send --url http://127.0.0.1:8000 --key dev-key     # against a fresh server
# then open http://127.0.0.1:8000/dashboard?key=dev-key
```

### Storage
In memory by default (lost on restart). For Postgres:
```bash
docker compose up -d
set PIPELINEWATCH_DATABASE_URL=postgresql://pipelinewatch:pipelinewatch@localhost:5432/pipelinewatch
```
The schema ([`schema.sql`](src/pipelinewatch/schema.sql): `alert_log`, `dedup_state`, `delivery_log`) is created or upgraded on start-up.

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
| POST | `/webhooks/{source}` | `X-PipelineWatch-Key` header or `?key=` | `source` = `databricks`, `azure-monitor`, `generic` or `auto`. Returns `route`, `decision`, `occurrence`, `replay` and the `explanation` id/title. A body that doesn't match the named source gets a 422. |
| GET | `/alerts?limit=50` | key | most recent alerts first (raw payload omitted) |
| GET | `/alerts/{id}/deliveries` | key | notification attempts for one alert |
| POST | `/digest/send?dry_run=false` | key | send the daily digest of LOW alerts |
| GET | `/dashboard` | key (`?key=` in a browser) | HTML dashboard |

The `?key=` form exists because Azure Monitor action-group webhooks can't add custom headers and browsers can't either. Use HTTPS in production so the key isn't exposed.

### Adding to the failure catalog
Add an entry to [`failures.yaml`](src/pipelinewatch/catalog/failures.yaml) with `patterns` (case-insensitive regexes), a short `cause`, at least two `fix` steps and some `examples` of the real error text. Then run `pytest tests/test_explain.py`: it fails if an example doesn't match your entry or is caught by an earlier one.

## Roadmap
| Version | Adds |
|---|---|
| v0.1 | Normalise + classify + replay detection, simulator, tests, CI |
| v0.2 | De-duplication windows with escalation and recovery, Postgres store, Slack / Teams / Telegram notifiers with retries and a delivery log |
| **v0.3** (now) | Self-testing failure catalog with plain-English causes and fixes, daily digest, dashboard, demo script |
| v1.0 | Deployed (Render free tier + Supabase Postgres) |
| v1.1 | Microsoft Fabric pipelines and Airflow DAG failures; error-text enrichment via the Databricks Jobs API and ADF activity-runs API (so ADF/Databricks alerts get specific explanations, not just the fallbacks) |

## What is and isn't verified
- **Verified locally:** 88 tests (normalising, rules, every failure-catalog example, dedup, the in-memory store including a 20-thread burst, notifier payloads and retries, digest, dashboard including HTML escaping, the API). `scripts/demo.py` ran end to end against a local server with a local stand-in for Slack: every simulator step passed, the digest went out as one message, and the dashboard screenshot above came from that run.
- **Verified in CI:** the same tests plus the Postgres store variants (replay, a 20-thread burst under the advisory lock, resolve-then-reopen, digest queue, open incidents, delivery log) against Postgres 16.
- **Not yet verified:** real webhooks from a live Databricks workspace or Azure Monitor action group, and real Slack/Teams/Telegram workspaces. The payload formats follow the vendors' documented shapes.
- **Known limit:** Databricks job webhooks and ADF metric alerts don't carry the error text, so until enrichment (v1.1) those alerts get the fallback explanation. Specific explanations work today for senders that include the error message, such as an ADF Web activity in an on-failure path or a notebook's exception handler posting to `/webhooks/generic`.

## Credits
The payload builders in `simulator/events.py`, the rules and the dedup design come from my earlier [n8n-dataops-alerts](https://github.com/pradeepkumaryenni/n8n-dataops-alerts) project, re-implemented here as a standalone Python service.

## License
MIT
