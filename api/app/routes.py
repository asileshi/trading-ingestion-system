from fastapi import APIRouter, HTTPException
import redis
import os
from pydantic import BaseModel
import json
from datetime import datetime, timezone

from . import database

router = APIRouter()

r = redis.from_url(os.getenv("REDIS_URL"))

# Redis list key used as the trade queue
QUEUE_KEY = os.getenv("TRADE_QUEUE_KEY", "trades:queue")
DLQ_KEY = os.getenv("TRADE_DLQ_KEY", "trades:dlq")


class Trade(BaseModel):
    trade_id: str
    user: str
    symbol: str
    price: float


@router.post("/ingest")
def ingest_trade(trade: Trade):
    """
    Push trade to Redis queue.
    """
    message = {
        "trade": trade.model_dump() if hasattr(trade, "model_dump") else trade.dict(),
        "meta": {
            "attempt": 0,
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
        },
    }
    r.lpush(QUEUE_KEY, json.dumps(message))
    return {"status": "queued", "trade_id": trade.trade_id}


@router.get("/queue")
def get_queue(limit: int = 20):
    """
    Peek at the most recently queued trades (does not remove them).
    """
    items = r.lrange(QUEUE_KEY, 0, max(limit - 1, 0))
    messages = []
    for item in items:
        if isinstance(item, (bytes, bytearray)):
            item = item.decode("utf-8")

        try:
            messages.append(json.loads(item))
        except json.JSONDecodeError:
            messages.append({"raw": item, "error": "invalid_json"})

    return {"queue": messages, "count": len(messages), "limit": limit}


@router.get("/trades/{trade_id}")
def get_trade(trade_id: str):
    """
    Fetch a trade from Postgres (source of truth).
    """
    conn = database.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trade_id, user_id, symbol, price, created_at
                FROM trades_raw
                WHERE trade_id = %s
                """,
                (trade_id,),
            )
            row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Trade not found")

        trade_id, user_id, symbol, price, created_at = row
        return {
            "trade_id": trade_id,
            "user_id": user_id,
            "symbol": symbol,
            "price": float(price) if price is not None else None,
            "created_at": created_at.isoformat() if created_at else None,
        }
    finally:
        conn.close()

@router.get("/trades")
def list_trades(limit: int = 50, symbol: str | None = None, user_id: str | None = None):
    limit = max(1, min(limit, 200))

    where = []
    params = []

    if symbol:
        where.append("symbol = %s")
        params.append(symbol)

    if user_id:
        where.append("user_id = %s")
        params.append(user_id)

    where_sql = ""
    if where:
        where_sql = "WHERE " + " AND ".join(where)

    conn = database.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT trade_id, user_id, symbol, price, created_at
                FROM trades_raw
                {where_sql}
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (*params, limit),
            )
            rows = cur.fetchall()

        results = []
        for trade_id, user_id, symbol, price, created_at in rows:
            results.append(
                {
                    "trade_id": trade_id,
                    "user_id": user_id,
                    "symbol": symbol,
                    "price": float(price) if price is not None else None,
                    "created_at": created_at.isoformat() if created_at else None,
                }
            )

        return {"trades": results, "count": len(results), "limit": limit}
    finally:
        conn.close()

@router.get("/dlq")
def get_dlq(limit: int = 20):
    limit = max(1, min(limit, 200))
    items = r.lrange(DLQ_KEY, 0, limit - 1)

    out = []
    for item in items:
        if isinstance(item, (bytes, bytearray)):
            item = item.decode("utf-8")
        try:
            out.append(json.loads(item))
        except json.JSONDecodeError:
            out.append({"raw": item, "error": "invalid_json"})

    return {"dlq": out, "count": len(out), "limit": limit, "key": DLQ_KEY}