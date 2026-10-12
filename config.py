# config.py

import os
from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class Config:
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    RABBITMQ_URL: str = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672//")
    DEFAULT_RATE_LIMIT: int = int(os.getenv("DEFAULT_RATE_LIMIT", "5"))
    DEFAULT_WINDOW_SECONDS: int = int(os.getenv("DEFAULT_WINDOW_SECONDS", "60"))
    DEFAULT_ALGORITHM: str = os.getenv("DEFAULT_ALGORITHM", "sliding_window")
    SYNC_BATCH_SIZE: int = int(os.getenv("SYNC_BATCH_SIZE", "10"))
    SYNC_FLUSH_INTERVAL_SECONDS: int = int(os.getenv("SYNC_FLUSH_INTERVAL_SECONDS", "2"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "5"))
    DEAD_LETTER_QUEUE: str = os.getenv("DEAD_LETTER_QUEUE", "rate_limit_dlq")
    REDIS_CONNECT_TIMEOUT: int = int(os.getenv("REDIS_CONNECT_TIMEOUT", "5"))
    REDIS_SOCKET_TIMEOUT: int = int(os.getenv("REDIS_SOCKET_TIMEOUT", "5"))
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    FLASK_ENV: str = os.getenv("FLASK_ENV", "development")

    @staticmethod
    def validate() -> None:
        if Config.DEFAULT_RATE_LIMIT <= 0:
            raise ValueError("DEFAULT_RATE_LIMIT must be > 0")
        if Config.DEFAULT_WINDOW_SECONDS <= 0:
            raise ValueError("DEFAULT_WINDOW_SECONDS must be > 0")
        if Config.DEFAULT_ALGORITHM not in {"token_bucket", "sliding_window"}:
            raise ValueError("DEFAULT_ALGORITHM must be token_bucket or sliding_window")
