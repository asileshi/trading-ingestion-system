from fastapi import APIRouter
import redis
import os
from pydantic import BaseModel
import json

router = APIRouter()

r = redis.from_url(os.getenv("REDIS_URL"))

class Trade(BaseModel):
    trade_id: str
    user: str
    symbol: str
    price: float

@router.post("/ingest")
def ingest_trade(trade: Trade):
    """
    Push trade to Redis queue.
    trade_id is mandatory.
    """
    r.lpush("trade_queue", trade.json())
    return {"status": "queued", "trade_id": trade.trade_id}

@router.get("/queue")
def get_queue(limit: int = 20):
    """
    Peek at the most recently queued trades (does not remove them).
    """
    # LRANGE 0..limit-1 reads from the *left* side of the list (newest first, because you LPUSH)
    items = r.lrange("trade_queue", 0, max(limit - 1, 0))
    trades = []
    for item in items:
        # redis-py returns bytes
        if isinstance(item, (bytes, bytearray)):
            item = item.decode("utf-8")

        try:
            trades.append(json.loads(item))
        except json.JSONDecodeError:
            # If something non-JSON got into the list, keep it visible for debugging
            trades.append({"raw": item, "error": "invalid_json"})

    return {"queue": trades, "count": len(trades), "limit": limit}