from fastapi import APIRouter
import redis
import os
import json

router = APIRouter()

r = redis.from_url(os.getenv("REDIS_URL"))

@router.get("/")
def health():
    return {"status": "ok"}

@router.post("/ingest")
def ingest_trade(trade: dict):
    r.lpush("trade_queue", json.dumps(trade))
    return {"queued": True}
