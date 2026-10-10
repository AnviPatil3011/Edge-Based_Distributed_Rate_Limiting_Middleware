# rate_limiter/sliding_window.py

import time
import uuid
from typing import Dict

from services.redis_service import RedisService
from rate_limiter.base import RateLimiter, RateLimiterResult

class SlidingWindowRateLimiter(RateLimiter):
    """Sliding Window implementation using a Redis Lua script.

    Policy fields expected in ``policy:{policy_id}`` hash:
        - algorithm (must be "sliding_window")
        - limit (int)
        - window_seconds (int)
    """

    def __init__(self) -> None:
        self.redis = RedisService()

    def _load_policy(self, policy_id: str) -> Dict[str, str]:
        policy = self.redis.get_policy(policy_id)
        if not policy:
            raise ValueError(f"Policy {policy_id} not found in Redis")
        if policy.get('algorithm') != 'sliding_window':
            raise ValueError('Policy algorithm mismatch for SlidingWindowRateLimiter')
        return policy

    def is_allowed(self, user_id: str, edge_id: str, policy_id: str, request_id: str) -> RateLimiterResult:
        policy = self._load_policy(policy_id)
        limit = int(policy.get('limit', 5))
        window_seconds = int(policy.get('window_seconds', 60))
        now_ms = int(time.time() * 1000)
        window_ms = window_seconds * 1000
        # Redis key for this user/policy
        key = f"rl:sw:{user_id}:{policy_id}"
        metrics_key = "metrics"
        member = str(uuid.uuid4())
        allowed, remaining, retry_after, ts = self.redis.execute_lua(
            name='SLIDING_WINDOW',
            keys=[key, metrics_key],
            args=[str(limit), str(window_ms), str(now_ms), member],
        )
        allowed_bool = bool(int(allowed))
        retry = int(retry_after) if int(retry_after) != -1 else None
        result = RateLimiterResult(
            allowed=allowed_bool,
            algorithm='sliding_window',
            limit=limit,
            remaining=remaining,
            retry_after=retry,
            reset_at=now_ms + (retry if retry else window_ms),
            current_usage=limit - remaining if allowed_bool else limit,
            edge_id=edge_id,
            user_id=user_id,
            policy_id=policy_id,
            request_id=request_id,
        )
        return result
