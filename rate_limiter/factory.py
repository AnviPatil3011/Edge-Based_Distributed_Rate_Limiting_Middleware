# rate_limiter/factory.py

from .base import RateLimiter
from .token_bucket import TokenBucketRateLimiter
from .sliding_window import SlidingWindowRateLimiter

def get_limiter(algorithm: str, policy: dict | None = None) -> RateLimiter:
    """Factory to obtain a RateLimiter implementation.

    The *policy* argument is retained for backward compatibility; the concrete
    limiter implementations retrieve the policy directly from Redis, so it is
    ignored here.
    """
    algo = algorithm.lower()
    if algo == 'token_bucket':
        return TokenBucketRateLimiter()
    elif algo == 'sliding_window':
        return SlidingWindowRateLimiter()
    else:
        raise ValueError(f"Unsupported algorithm: {algorithm}")
    """Factory to obtain a RateLimiter implementation.

    Supported algorithms: "token_bucket", "sliding_window".
    Raises ValueError for unknown names.
    """
    algo = algorithm.lower()
    if algo == 'token_bucket':
        return TokenBucketRateLimiter()
    elif algo == 'sliding_window':
        return SlidingWindowRateLimiter()
    else:
        raise ValueError(f"Unsupported algorithm: {algorithm}")
