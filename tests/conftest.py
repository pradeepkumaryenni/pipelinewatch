import os

import pytest
from fastapi.testclient import TestClient

from pipelinewatch.app import create_app
from pipelinewatch.config import Config
from pipelinewatch.notify import Notifier
from pipelinewatch.store import InMemoryStore

API_KEY = "test-key"
PG_DSN = os.environ.get("PIPELINEWATCH_TEST_DATABASE_URL")


@pytest.fixture
def config() -> Config:
    return Config.load()  # config/example.yaml, matching the simulator's sample ids


class FakeTransport:
    """Records every notification request instead of sending it; answers with queued status codes."""

    def __init__(self, statuses=None):
        self.calls = []
        self.statuses = list(statuses or [])

    def __call__(self, url, body):
        self.calls.append((url, body))
        status = self.statuses.pop(0) if self.statuses else 200
        if isinstance(status, Exception):
            raise status
        return status, "ok" if status < 300 else "error"


@pytest.fixture
def client(config: Config) -> TestClient:
    notifier = Notifier({}, config.notifications)  # no channels: nothing leaves the test
    return TestClient(create_app(config=config, api_keys=[API_KEY], store=InMemoryStore(), notifier=notifier))


@pytest.fixture
def headers() -> dict[str, str]:
    return {"X-PipelineWatch-Key": API_KEY}


def _pg_store():
    from pipelinewatch.pg_store import PostgresStore
    store = PostgresStore(PG_DSN, window_minutes=15)
    store.init_schema()
    store.reset()
    return store


@pytest.fixture(params=["memory", "postgres"])
def store(request):
    """Runs a test against both stores; Postgres only when PIPELINEWATCH_TEST_DATABASE_URL is set."""
    if request.param == "memory":
        return InMemoryStore(window_minutes=15)
    if not PG_DSN:
        pytest.skip("PIPELINEWATCH_TEST_DATABASE_URL not set")
    return _pg_store()
