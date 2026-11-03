# PipelineWatch

Plain-English alerts for failed **Azure Data Factory** and **Databricks** pipelines, built for small data teams that can't justify an enterprise observability tool.

> **Portfolio project, built with AI-assisted development (Claude Code).** All events are simulated with synthetic ids. It is not code from any employer or client. This is v0.1 (week 1 of the roadmap below); see [What is and isn't verified](#what-is-and-isnt-verified).

## What it does today (v0.1)

```
Databricks job webhook ─┐
Azure Monitor (ADF)  ───┼─▶ POST /webhooks/{source} ─▶ normalise ─▶ classify ─▶ store ─▶ JSON answer
Generic JSON check   ───┘        (API key)            one shape    severity +   replay     route + summary
                                                                   route        detection
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

3. **Replay detection:** a webhook delivered twice (Azure Monitor and Databricks both retry) is answered with the original decision and `"replay": true`.

Workspace ids and pipeline tiers live in [`config/example.yaml`](config/example.yaml), not in code.

## Run it locally

```bash
python -m venv .venv
.venv/Scripts/activate          # Windows (source .venv/bin/activate on macOS/Linux)
pip install -e ".[dev]"
pytest -q                       # 42 tests

# start the API
set PIPELINEWATCH_API_KEYS=dev-key        # Windows cmd  (export ... on macOS/Linux)
uvicorn pipelinewatch.app:app --reload

# in another terminal: send every demo scenario and check the answers
python -m simulator.send --url http://127.0.0.1:8000 --key dev-key
```

### API
| Method | Path | Auth | What |
|---|---|---|---|
| GET | `/health` | none | liveness |
| POST | `/webhooks/{source}` | `X-PipelineWatch-Key` header or `?key=` | `source` = `databricks`, `azure-monitor`, `generic` or `auto` (detect). A body that doesn't match the named source is rejected with 422. |
| GET | `/alerts?limit=50` | key | most recent alerts first (raw payload omitted) |

The `?key=` form exists because Azure Monitor action-group webhooks can't add custom headers. Use HTTPS in production so the key isn't exposed.

## Roadmap
| Version | Adds |
|---|---|
| **v0.1** (now) | Normalise + classify + replay detection, simulator, tests, CI |
| v0.2 | Postgres alert log with a fingerprint de-duplication window (one notification per incident), Slack / Teams / Telegram notifiers |
| v0.3 | **Failure catalog**: plain-English cause + fix steps for common failures (expired credential or Key Vault secret, ADF copy type mismatch, schema drift, missing/late source file, timeouts, Delta concurrent writes, Spark OOM/skew, cluster terminated), plus a small dashboard |
| v1.0 | Deployed (Render free tier + Supabase Postgres), demo GIF |
| v1.1 | Microsoft Fabric pipelines and Airflow DAG failures; error-text enrichment via the Databricks Jobs API and ADF activity-runs API |

## What is and isn't verified
- **Verified:** unit and API tests (`pytest`), and the simulator run end to end against a local server (all 13 steps pass).
- **Not yet verified:** real webhooks from a live Databricks workspace or Azure Monitor action group. The payload shapes follow the documented formats (Databricks job notification webhook, Azure Monitor common alert schema), but they haven't been sent from real services.
- Storage is in memory in v0.1, so alerts are lost on restart.

## Credits
The payload builders in `simulator/events.py` and the rules come from my earlier [n8n-dataops-alerts](https://github.com/pradeepkumaryenni/n8n-dataops-alerts) project, re-implemented here as a standalone Python service.

## License
MIT
