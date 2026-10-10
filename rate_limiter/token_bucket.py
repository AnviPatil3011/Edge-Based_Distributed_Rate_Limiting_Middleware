# rate_limiter/token_bucket.py

import time
import uuid
from typing import Dict

from services.redis_service import RedisService
from rate_limiter.base import RateLimiter, RateLimiterResult

class TokenBucketRateLimiter(RateLimiter):
    """Token Bucket implementation using a Redis Lua script.

    The policy parameters are read from the policy hash in Redis using the provided
    ``policy_id``. Expected fields in the hash:
        - algorithm (must be "token_bucket")
        - limit (int) – treated as the window size for TTL purposes
        - bucket_capacity (int)
        - refill_rate (float) – tokens per second
    """

    def __init__(self) -> None:
        self.redis = RedisService()

    def _load_policy(self, policy_id: str) -> Dict[str, str]:
        policy = self.redis.get_policy(policy_id)
        if not policy:
            raise ValueError(f"Policy {policy_id} not found in Redis")
        if policy.get('algorithm') != 'token_bucket':
            raise ValueError('Policy algorithm mismatch for TokenBucketRateLimiter')
        return policy

    def is_allowed(self, user_id: str, edge_id: str, policy_id: str, request_id: str) -> RateLimiterResult:
        policy = self._load_policy(policy_id)
        limit = int(policy.get('limit', 5))
        capacity = int(policy.get('bucket_capacity', limit))
        refill_rate = float(policy.get('refill_rate', 1))
        now_ms = int(time.time() * 1000)
        # Redis key for this user/policy
        key = f"rl:tb:{user_id}:{policy_id}"
        metrics_key = "metrics"
        # Execute Lua script
        allowed, remaining, retry_after, ts = self.redis.execute_lua(
            name='TOKEN_BUCKET',
            keys=[key, metrics_key],
            args=[str(limit), str(refill_rate), str(capacity), str(now_ms)],
        )
        # Lua returns allowed (1/0), remaining tokens, retry_after (ms or -1), timestamp
        allowed_bool = bool(int(allowed))
        retry = int(retry_after) if int(retry_after) != -1 else None
        result = RateLimiterResult(
            allowed=allowed_bool,
            algorithm='token_bucket',
            limit=limit,
            remaining=remaining,
            retry_after=retry,
            reset_at=now_ms + int((capacity - remaining) / refill_rate * 1000) if allowed_bool else now_ms + (retry or 0),
            current_usage=capacity - remaining,
            edge_id=edge_id,
            user_id=user_id,
            policy_id=policy_id,
            request_id=request_id,
        )
        return result
