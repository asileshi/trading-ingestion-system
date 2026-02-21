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