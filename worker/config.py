import os

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://admin:admin@postgres:5432/trading",
)

QUEUE_NAME = os.getenv("TRADE_QUEUE_NAME", "trade_queue")
BRPOP_TIMEOUT_SECONDS = int(os.getenv("BRPOP_TIMEOUT_SECONDS", "5"))
IDLE_SLEEP_SECONDS = float(os.getenv("IDLE_SLEEP_SECONDS", "0.2"))