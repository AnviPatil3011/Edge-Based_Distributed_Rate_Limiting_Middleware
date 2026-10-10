import pytest
from rate_limiter.factory import get_limiter
from services.redis_service import RedisService

@pytest.fixture
def policy_id():
    svc = RedisService()
    policy = {
        "algorithm": "sliding_window",
        "limit": "5",
        "window_seconds": "2",
    }
    pid = svc.store_policy(policy)
    return pid

def test_sliding_window_basic_allow(policy_id):
    limiter = get_limiter('sliding_window')
    user = "user1"
    edge = "edgeA"
    # First 5 requests should be allowed
    for i in range(5):
        result = limiter.is_allowed(user, edge, policy_id, f"req{i}")
        assert result.allowed is True
        assert result.remaining == 4 - i
    # 6th request should be rejected
    result = limiter.is_allowed(user, edge, policy_id, "req6")
    assert result.allowed is False
    assert result.retry_after is not None

def test_sliding_window_multiple_users(policy_id):
    limiter = get_limiter('sliding_window')
    edge = "edgeB"
    for user in ["alice", "bob"]:
        for i in range(5):
            result = limiter.is_allowed(user, edge, policy_id, f"{user}_req{i}")
            assert result.allowed is True
        # next request blocked for this user
        result = limiter.is_allowed(user, edge, policy_id, f"{user}_req6")
        assert result.allowed is False
