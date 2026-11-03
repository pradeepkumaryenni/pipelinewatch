import pytest
from fastapi.testclient import TestClient

from pipelinewatch.app import create_app
from pipelinewatch.config import Config
from pipelinewatch.store import InMemoryStore

API_KEY = "test-key"


@pytest.fixture
def config() -> Config:
    return Config.load()  # config/example.yaml, matching the simulator's sample ids


@pytest.fixture
def client(config: Config) -> TestClient:
    return TestClient(create_app(config=config, api_keys=[API_KEY], store=InMemoryStore()))


@pytest.fixture
def headers() -> dict[str, str]:
    return {"X-PipelineWatch-Key": API_KEY}
