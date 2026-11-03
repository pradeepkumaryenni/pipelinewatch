"""A small, dependency-free HTML dashboard: summary, open incidents, repeat offenders, recent alerts."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Any

from .store import Store

DECISION_CLASS = {"PAGE": "crit", "NOTIFY": "high", "RESOLVE": "ok", "DIGEST": "low", "SUPPRESS": "muted",
                  "LOG_ONLY": "muted"}


def _parse(ts: Any) -> datetime | None:
    if isinstance(ts, datetime):
        return ts
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None


def _ago(ts: Any, now: datetime) -> str:
    when = _parse(ts)
    if not when:
        return ""
    minutes = int((now - when).total_seconds() // 60)
    if minutes < 1:
        return "just now"
    if minutes < 60:
        return f"{minutes} min ago"
    return f"{minutes // 60} h {minutes % 60} min ago"


def _incident_label(fingerprint: str) -> str:
    """'databricks:prod:nightly_sales_etl:failure' -> 'nightly_sales_etl (prod) · failure', HTML-escaped."""
    parts = fingerprint.split(":")
    if len(parts) < 4:
        return escape(fingerprint)
    source, env, kind = parts[0], parts[1], parts[-1]
    pipeline = ":".join(parts[2:-1])
    return (f"<b>{escape(pipeline)}</b> ({escape(env)})"
            f'<div class="cause">{escape(kind.replace("_", " "))} · {escape(source)}</div>')


def summarize(alerts: list[dict[str, Any]], now: datetime, hours: int = 24) -> dict[str, Any]:
    since = now - timedelta(hours=hours)
    window = [a for a in alerts if (_parse(a.get("received_at")) or now) >= since]
    decisions = Counter(a["decision"] for a in window)
    repeats = Counter((a["pipeline"], a["environment"]) for a in window
                      if a["status"] == "FIRING" and a["event_type"] in ("failure", "sla_breach"))
    causes = Counter(a["explanation"]["title"] for a in window if a.get("explanation"))
    return {"total": len(window), "decisions": decisions, "repeat_offenders": repeats.most_common(5),
            "top_causes": causes.most_common(5)}


def render(store: Store, now: datetime | None = None, limit: int = 100) -> str:
    now = now or datetime.now(timezone.utc)
    alerts = store.recent(500)
    s = summarize(alerts, now)
    # Only incidents someone was told about; LOW ones wait for the digest.
    windows = [w for w in store.open_windows() if w.max_severity in ("CRITICAL", "HIGH")]

    tiles = "".join(
        f'<div class="tile {DECISION_CLASS.get(d, "")}"><b>{s["decisions"].get(d, 0)}</b><span>{label}</span></div>'
        for d, label in (("PAGE", "paged"), ("NOTIFY", "notified"), ("SUPPRESS", "suppressed duplicates"),
                         ("DIGEST", "for digest"), ("RESOLVE", "recovered")))
    open_rows = "".join(
        f'<tr><td>{_incident_label(w.fingerprint)}</td>'
        f'<td><span class="pill {"crit" if w.max_severity == "CRITICAL" else "high"}">{escape(w.max_severity)}</span></td>'
        f"<td>{w.occurrences}</td><td>{_ago(w.window_started_at, now)}</td></tr>" for w in windows
    ) or '<tr><td colspan="4" class="empty">Nothing open.</td></tr>'
    repeat_rows = "".join(
        f"<tr><td>{escape(p)}</td><td>{escape(e)}</td><td>{n}</td></tr>" for (p, e), n in s["repeat_offenders"]
    ) or '<tr><td colspan="3" class="empty">No failures in the last 24 hours.</td></tr>'
    cause_rows = "".join(f"<li>{escape(t)} <b>×{n}</b></li>" for t, n in s["top_causes"]) or "<li>None yet</li>"
    recent_rows = "".join(
        f'<tr><td>{_ago(a.get("received_at"), now)}</td>'
        f'<td><span class="pill {DECISION_CLASS.get(a["decision"], "")}">{escape(a["decision"])}</span></td>'
        f'<td>{escape(a["pipeline"])}</td><td>{escape(a["environment"])}</td>'
        f'<td>{escape(a.get("summary") or "")}'
        + (f'<div class="cause">Likely: {escape(a["explanation"]["title"])}</div>' if a.get("explanation") else "")
        + "</td></tr>"
        for a in alerts[:limit]
    ) or '<tr><td colspan="5" class="empty">No alerts yet. Send one with the simulator.</td></tr>'

    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>PipelineWatch</title>
<style>
:root{{--bg:#f4f6f8;--card:#fff;--fg:#14202b;--muted:#5d6b78;--line:#dde3e8;--crit:#b4232c;--high:#b85c00;
--low:#3d6b99;--ok:#1d7a46}}
@media (prefers-color-scheme:dark){{:root{{--bg:#0f161c;--card:#17212a;--fg:#e4ebf0;--muted:#93a3b0;--line:#27343f;
--crit:#ff6b6b;--high:#f0a04b;--low:#7fb2e5;--ok:#5fd394}}}}
body{{margin:0;background:var(--bg);color:var(--fg);font:15px/1.5 system-ui,sans-serif}}
main{{max-width:1100px;margin:0 auto;padding:24px 16px;display:grid;gap:20px}}
h1{{margin:0;font-size:24px}} h2{{margin:0 0 8px;font-size:16px}} .muted{{color:var(--muted)}}
.tiles{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}}
.tile{{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px}}
.tile b{{display:block;font-size:26px}} .tile span{{color:var(--muted);font-size:13px}}
.tile.crit b,.pill.crit{{color:var(--crit)}} .tile.high b,.pill.high{{color:var(--high)}}
.tile.low b,.pill.low{{color:var(--low)}} .tile.ok b,.pill.ok{{color:var(--ok)}} .pill.muted{{color:var(--muted)}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:16px}}
section{{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px;overflow-x:auto}}
table{{width:100%;border-collapse:collapse;font-size:14px}} td,th{{text-align:left;padding:6px 8px;
border-bottom:1px solid var(--line);vertical-align:top}} th{{color:var(--muted);font-weight:600}}
.pill{{font-weight:700;font-size:12px}} .cause{{color:var(--muted);font-size:13px}} .empty{{color:var(--muted)}}
</style></head><body><main>
<header><h1>PipelineWatch</h1><div class="muted">Last 24 hours · {s["total"]} alerts · updated {now:%H:%M} UTC</div></header>
<div class="tiles">{tiles}</div>
<div class="grid">
<section><h2>Open incidents</h2><table><tr><th>Problem</th><th>Severity</th><th>Seen</th><th>Opened</th></tr>{open_rows}</table></section>
<section><h2>Repeat offenders</h2><table><tr><th>Pipeline</th><th>Env</th><th>Failures</th></tr>{repeat_rows}</table>
<h2 style="margin-top:14px">Top likely causes</h2><ul>{cause_rows}</ul></section>
</div>
<section><h2>Recent alerts</h2><table><tr><th>When</th><th>Decision</th><th>Pipeline</th><th>Env</th><th>Summary</th></tr>{recent_rows}</table></section>
</main></body></html>"""
