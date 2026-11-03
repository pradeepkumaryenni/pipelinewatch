"""Send the demo scenarios to a running PipelineWatch server and check every answer.

    python -m simulator.send --url http://127.0.0.1:8000 --key dev-key
    python -m simulator.send --url http://127.0.0.1:8000 --key dev-key --only adf_failure recovery
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request

from .scenarios import build_scenarios


def post(url: str, key: str, payload: object) -> tuple[int, dict]:
    request = urllib.request.Request(
        url, data=json.dumps(payload).encode(), method="POST",
        headers={"Content-Type": "application/json", "X-PipelineWatch-Key": key},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read() or b"{}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--key", required=True, help="An API key from PIPELINEWATCH_API_KEYS")
    parser.add_argument("--only", nargs="*", help="Scenario names to run (default: all)")
    args = parser.parse_args(argv)

    scenarios = build_scenarios()
    failures = 0
    for name, steps in scenarios.items():
        if args.only and name not in args.only:
            continue
        print(f"\n== {name}")
        for step in steps:
            status, body = post(f"{args.url.rstrip('/')}/webhooks/{step.source}", args.key, step.payload)
            ok = status == step.expect_http
            if ok and step.expect_route:
                ok = body.get("route") == step.expect_route and body.get("replay", False) == step.expect_replay
            failures += not ok
            got = body.get("route") or body.get("detail")
            print(f"  {'PASS' if ok else 'FAIL'}  {step.label}\n        HTTP {status} -> {got}"
                  + (" (replay)" if body.get("replay") else ""))
            if body.get("summary"):
                print(f"        {body['summary']}")
    print(f"\n{'All steps passed.' if not failures else f'{failures} step(s) failed.'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
