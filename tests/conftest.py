# tests/conftest.py

import pytest
import fakeredis
from services.redis_service import RedisService

@pytest.fixture(autouse=True)
def reset_redis(monkeypatch):
    # Create a fresh FakeRedis instance for each test
    fake_client = fakeredis.FakeRedis(decode_responses=True)
    # Monkeypatch the singleton instance to use the fake client
    service = RedisService()
    service.client = fake_client
    # Register scripts on the fake client (fakeredis supports register_script)
    from rate_limiter import redis_scripts
    service.scripts = {
        name: fake_client.register_script(script)
        for name, script in redis_scripts.LUA_SCRIPTS.items()
    }
    # Ensure any previous state is cleared
    fake_client.flushdb()
    yield
    # Cleanup after test (not strictly needed for fakeredis)
    fake_client.flushdb()
