"""End-to-end demo on free local ports, with nothing leaving the machine.

Starts PipelineWatch (in-memory store) plus a local stand-in for a Slack webhook, sends every simulator
scenario, sends the daily digest, prints what "Slack" received, and screenshots the dashboard to
docs/dashboard.png (needs Microsoft Edge or Chrome for the screenshot; skipped otherwise).

    python scripts/demo.py
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BROWSERS = [r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            "microsoft-edge", "google-chrome", "chromium", "chromium-browser"]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main() -> int:
    received: list[dict] = []

    class Hook(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *args):
            pass

    hook = HTTPServer(("127.0.0.1", 0), Hook)
    threading.Thread(target=hook.serve_forever, daemon=True).start()
    port = free_port()
    env = {**os.environ, "PIPELINEWATCH_API_KEYS": "demo-key",
           "PIPELINEWATCH_SLACK_WEBHOOK_URL": f"http://127.0.0.1:{hook.server_port}/slack"}
    env.pop("PIPELINEWATCH_DATABASE_URL", None)
    server = subprocess.Popen([sys.executable, "-m", "uvicorn", "pipelinewatch.app:app", "--port", str(port)],
                              cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    base, headers = f"http://127.0.0.1:{port}", {"X-PipelineWatch-Key": "demo-key"}

    def call(path: str, method: str = "GET") -> str:
        req = urllib.request.Request(base + path, method=method, headers=headers,
                                     data=b"" if method == "POST" else None)
        return urllib.request.urlopen(req, timeout=10).read().decode()

    try:
        for _ in range(40):
            try:
                call("/health")
                break
            except OSError:
                time.sleep(0.25)
        sim = subprocess.run([sys.executable, "-m", "simulator.send", "--url", base, "--key", "demo-key"],
                             cwd=ROOT, capture_output=True, text=True, encoding="utf-8", timeout=120)
        print(sim.stdout)
        time.sleep(1.0)  # notifications are sent after each response
        digest = json.loads(call("/digest/send", "POST"))
        time.sleep(0.5)
        print(f"Digest: {digest['alerts']} alerts, sent {digest['sent']}")
        print(f"Messages delivered to the stand-in Slack: {len(received)}")

        browser = next((b for b in BROWSERS if Path(b).exists() or shutil.which(b)), None)
        if browser:
            page = Path(tempfile.gettempdir()) / "pipelinewatch_dashboard.html"
            page.write_text(call("/dashboard?key=demo-key"), encoding="utf-8")
            png = ROOT / "docs" / "dashboard.png"
            png.parent.mkdir(exist_ok=True)
            subprocess.run([browser, "--headless=new", "--disable-gpu", "--hide-scrollbars",
                            "--window-size=1200,1300", f"--screenshot={png}", page.as_uri()],
                           capture_output=True, timeout=60)
            print(f"Dashboard screenshot: {png}")
        return sim.returncode
    finally:
        server.terminate()
        server.wait(10)
        hook.shutdown()


if __name__ == "__main__":
    sys.exit(main())
