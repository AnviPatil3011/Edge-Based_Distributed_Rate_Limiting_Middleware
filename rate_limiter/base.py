import abc
import time
from dataclasses import dataclass
from typing import Optional, Dict

@dataclass(frozen=True)
class RateLimiterResult:
    allowed: bool
    algorithm: str
    limit: int
    remaining: int
    retry_after: Optional[int]
    reset_at: int
    current_usage: int
    edge_id: str
    user_id: str
    policy_id: str
    request_id: str

class RateLimiter(abc.ABC):
    @abc.abstractmethod
    def is_allowed(self, user_id: str, edge_id: str, policy_id: str, request_id: str) -> RateLimiterResult:
        """Perform rate‑limit check and update state atomically.
        Implementations must use Redis (via a provided service) to ensure concurrency safety.
        """
        raise NotImplementedError
