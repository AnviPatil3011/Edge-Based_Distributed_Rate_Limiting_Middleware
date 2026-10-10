# tests/test_token_bucket.py

import time
import pytest
from rate_limiter.factory import get_limiter
from services.redis_service import RedisService

@pytest.fixture
def policy_id():
    svc = RedisService()
    # Define a token bucket policy
    policy = {
        "algorithm": "token_bucket",
        "limit": "5",  # used for TTL calculation but not essential here
        "bucket_capacity": "5",
        "refill_rate": "1",
    }
    pid = svc.store_policy(policy)
    return pid

def test_token_bucket_basic_allow(policy_id):
    limiter = get_limiter('token_bucket')
    user = "user1"
    edge = "edgeA"
    request_id = "req1"
    # First 5 requests should be allowed
    for i in range(5):
        result = limiter.is_allowed(user, edge, policy_id, f"req{i}")
        assert result.allowed is True
        assert result.remaining == 4 - i  # tokens left after this request
    # 6th request should be rejected
    result = limiter.is_allowed(user, edge, policy_id, "req6")
    assert result.allowed is False
    assert result.remaining == 0
    assert result.retry_after is not None

def test_token_bucket_multiple_users(policy_id):
    limiter = get_limiter('token_bucket')
    edge = "edgeB"
    # Two separate users should have independent buckets
    for user in ["alice", "bob"]:
        for i in range(5):
            result = limiter.is_allowed(user, edge, policy_id, f"{user}_req{i}")
            assert result.allowed is True
        # next request blocked for this user
        result = limiter.is_allowed(user, edge, policy_id, f"{user}_req6")
        assert result.allowed is False

