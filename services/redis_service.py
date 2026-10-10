# services/redis_service.py

import os
import json
import time
import logging
from typing import Any, List, Dict
import redis
from redis.exceptions import RedisError

from config import Config

logger = logging.getLogger(__name__)

class RedisService:
    """Singleton wrapper around a Redis client.

    Provides methods to execute named Lua scripts atomically and health checks.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RedisService, cls).__new__(cls)
            cls._instance._init_client()
        return cls._instance

    def _init_client(self) -> None:
        self.client = redis.StrictRedis.from_url(
            Config.REDIS_URL,
            socket_connect_timeout=Config.REDIS_CONNECT_TIMEOUT,
            socket_timeout=Config.REDIS_SOCKET_TIMEOUT,
            decode_responses=True,
        )
        # Load Lua scripts on first connection
        from rate_limiter import redis_scripts
        self.scripts = {
            name: self.client.register_script(script)
            for name, script in redis_scripts.LUA_SCRIPTS.items()
        }
        logger.debug("RedisService initialized with scripts: %s", list(self.scripts.keys()))

    def ping(self) -> bool:
        try:
            return self.client.ping()
        except RedisError as exc:
            logger.error("Redis ping failed: %s", exc)
            return False

    def execute_lua(self, name: str, keys: List[str], args: List[Any]) -> Any:
        """Execute a registered Lua script by name.

        Raises RedisError on failure.
        """
        if name not in self.scripts:
            raise ValueError(f"Lua script '{name}' not registered")
        script_obj = self.scripts[name]
        try:
            # Normal execution (EVALSHA on real Redis)
            return script_obj(keys=keys, args=args)
        except Exception as exc:
            # Fallback for fakeredis which lacks EVAL/EVALSHA support
            logger.debug("Fallback Lua execution for %s due to %s", name, exc)
            # Detect fakeredis
            try:
                import fakeredis
                is_fake = isinstance(self.client, fakeredis.FakeRedis)
            except Exception:
                is_fake = False
            if is_fake:
                return self._python_fallback(name, keys, args)
            # Generic fallback using raw EVAL if script source available
            if hasattr(script_obj, 'script'):
                return self.client.eval(script_obj.script, len(keys), *keys, *args)
            raise

    # Helper for generic pipeline operations (used by sync batch script)
    def pipeline(self):
        return self.client.pipeline()

    def _python_fallback(self, name: str, keys: List[str], args: List[Any]):
        """Emulate Lua scripts in Python for fakeredis.

        Supports TOKEN_BUCKET and SLIDING_WINDOW scripts needed by tests.
        Returns a list matching the Lua script's return format.
        """
        if name == 'TOKEN_BUCKET':
            # args: limit, refill_rate, bucket_capacity, now_ms
            limit = int(args[0])
            refill_rate = float(args[1])
            capacity = int(args[2])
            now = int(args[3])
            key = keys[0]
            metrics_key = keys[1]
            data = self.client.hgetall(key)
            tokens = float(data.get('tokens', capacity))
            last_ts = int(data.get('last_ts', now))
            # Refill tokens based on elapsed time
            elapsed = (now - last_ts) / 1000.0
            tokens = min(capacity, tokens + elapsed * refill_rate)
            allowed = 0
            remaining = 0
            retry_after = -1
            if tokens >= 1:
                tokens -= 1
                allowed = 1
                remaining = int(tokens)
            else:
                needed = 1 - tokens
                retry_after = int((needed / refill_rate) * 1000)
            # Store state
            self.client.hmset(key, {'tokens': tokens, 'last_ts': now})
            self.client.expire(key, int(limit * 2))
            # Update metrics
            self.client.incr(f"{metrics_key}:total")
            if allowed:
                self.client.incr(f"{metrics_key}:allowed")
            else:
                self.client.incr(f"{metrics_key}:rejected")
            return [allowed, remaining, retry_after, now]
        elif name == 'SLIDING_WINDOW':
            # args: limit, window_ms, now_ms, member_id
            limit = int(args[0])
            window = int(args[1])
            now = int(args[2])
            member = args[3]
            key = keys[0]
            metrics_key = keys[1]
            # Remove stale entries
            self.client.zremrangebyscore(key, 0, now - window)
            count = self.client.zcard(key)
            allowed = 0
            remaining = 0
            retry_after = -1
            if count < limit:
                allowed = 1
                remaining = limit - (count + 1)
                self.client.zadd(key, {member: now})
            else:
                oldest = self.client.zrange(key, 0, 0, withscores=True)[0][1]
                retry_after = (oldest + window) - now
            self.client.expire(key, int(window / 1000))
            # Update metrics
            self.client.incr(f"{metrics_key}:total")
            if allowed:
                self.client.incr(f"{metrics_key}:allowed")
            else:
                self.client.incr(f"{metrics_key}:rejected")
            return [allowed, remaining, retry_after, now]
        else:
            raise NotImplementedError(f"Fallback for script {name} not implemented")
    # Policy helpers
    def get_active_policy_id(self) -> str:
        pid = self.client.get('active_policy_id')
        if pid is None:
            # Create default policy atomically using SETNX
            default_policy = {
                'algorithm': Config.DEFAULT_ALGORITHM,
                'limit': str(Config.DEFAULT_RATE_LIMIT),
                'window_seconds': str(Config.DEFAULT_WINDOW_SECONDS),
                'bucket_capacity': str(Config.DEFAULT_RATE_LIMIT),
                'refill_rate': '1',
            }
            # Generate a UUID for the policy
            import uuid
            new_id = str(uuid.uuid4())
            # Use a transaction to set both keys if not exists
            with self.client.pipeline() as pipe:
                try:
                    pipe.watch('active_policy_id')
                    if pipe.get('active_policy_id') is None:
                        pipe.multi()
                        pipe.hmset(f'policy:{new_id}', default_policy)
                        pipe.set('active_policy_id', new_id)
                        pipe.execute()
                        pid = new_id
                    else:
                        pid = pipe.get('active_policy_id')
                finally:
                    pipe.unwatch()
        return pid

    def store_policy(self, policy: Dict[str, str]) -> str:
        """Store a new policy and activate it. Returns the new policy_id."""
        import uuid
        pid = str(uuid.uuid4())
        self.client.hmset(f'policy:{pid}', policy)
        self.client.set('active_policy_id', pid)
        return pid

    def get_policy(self, policy_id: str) -> Dict[str, str]:
        return self.client.hgetall(f'policy:{policy_id}')

    # Record a request decision in per‑policy history (latest first)
    def add_history(self, policy_id: str, entry: dict) -> None:
        """Append a JSON‑encoded history entry and trim to recent 1000 items."""
        key = f'history:{policy_id}'
        self.client.rpush(key, json.dumps(entry))
        self.client.ltrim(key, -1000, -1)

    def get_history(self, policy_id: str) -> list:
        """Return a list of history entries (newest first) for the given policy."""
        key = f'history:{policy_id}'
        raw = self.client.lrange(key, 0, -1)
        return [json.loads(item) for item in raw]

    def get_worker_status(self, stale_seconds: int = 30) -> str:
        """Return 'healthy' if the worker heartbeat is recent, otherwise 'unhealthy' or 'unknown'."""
        ts = self.client.get('worker:heartbeat')
        if ts is None:
            return 'unknown'
        try:
            last = int(ts)
        except Exception:
            return 'unknown'
        now = int(time.time())
        return 'healthy' if now - last <= stale_seconds else 'unhealthy'
