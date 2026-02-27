import os

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://admin:admin@postgres:5432/trading",
)

# Redis list keys
QUEUE_KEY = os.getenv("TRADE_QUEUE_KEY", "trades:queue")
PROCESSING_KEY = os.getenv("TRADE_PROCESSING_KEY", "trades:processing")
DLQ_KEY = os.getenv("TRADE_DLQ_KEY", "trades:dlq")

# Worker loop behavior
BLPOP_TIMEOUT_SECONDS = int(os.getenv("BLPOP_TIMEOUT_SECONDS", "5"))
IDLE_SLEEP_SECONDS = float(os.getenv("IDLE_SLEEP_SECONDS", "0.2"))

# Retry behavior
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "5"))
RETRY_BACKOFF_SECONDS = float(os.getenv("RETRY_BACKOFF_SECONDS", "0.5"))