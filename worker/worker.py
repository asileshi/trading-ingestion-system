import json
import time
from datetime import datetime, timezone

import redis

import config
import database
import tasks


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def decode_payload(payload):
    if isinstance(payload, (bytes, bytearray)):
        return payload.decode("utf-8")
    return payload


def requeue_stuck_processing(r: redis.Redis) -> int:
    """
    Move all items from PROCESSING_KEY back to QUEUE_KEY and clear processing.
    This is a simple crash-recovery approach for Redis Lists.
    """
    items = r.lrange(config.PROCESSING_KEY, 0, -1)
    if not items:
        return 0

    # Push back to the main queue in the same order they were in processing
    for item in items:
        r.lpush(config.QUEUE_KEY, item)

    r.delete(config.PROCESSING_KEY)
    return len(items)


def parse_message(payload_str: str) -> dict:
    """
    Expected formats:
    1) New format:
       {"trade": {...}, "meta": {"attempt": 0, ...}}
    2) Backward compatible old format:
       {...trade fields...}
    """
    msg = json.loads(payload_str)

    if isinstance(msg, dict) and "trade" in msg:
        # Ensure meta exists
        meta = msg.get("meta") or {}
        if "attempt" not in meta:
            meta["attempt"] = 0
        msg["meta"] = meta
        return msg

    # Backward compatibility: wrap old trade dict
    return {"trade": msg, "meta": {"attempt": 0, "wrapped_at": utc_now_iso()}}


def ack_processing(r: redis.Redis, original_payload_str: str) -> None:
    # Remove exactly one occurrence of this payload from processing
    r.lrem(config.PROCESSING_KEY, 1, original_payload_str)


def send_to_dlq(r: redis.Redis, message: dict, error: str) -> None:
    dlq_item = {
        "failed_at": utc_now_iso(),
        "error": error,
        "message": message,
    }
    r.lpush(config.DLQ_KEY, json.dumps(dlq_item))


def main():
    r = redis.from_url(config.REDIS_URL, decode_responses=True)
    conn = database.get_conn()
    cur = conn.cursor()

    requeued = requeue_stuck_processing(r)
    print(f"Worker started... requeued_from_processing={requeued}")

    while True:
        try:
            payload_str = r.blmove(
                config.QUEUE_KEY,
                config.PROCESSING_KEY,
                src="RIGHT",
                dest="LEFT",
                timeout=config.BLPOP_TIMEOUT_SECONDS,
            )
        except Exception as e:
            print("Redis error while waiting for work:", repr(e))
            time.sleep(1.0)
            continue

        if not payload_str:
            time.sleep(config.IDLE_SLEEP_SECONDS)
            continue

        payload_str = decode_payload(payload_str)

        # Parse JSON
        try:
            message = parse_message(payload_str)
        except json.JSONDecodeError:
            print("Invalid JSON payload, sending to DLQ:", payload_str)
            send_to_dlq(r, {"raw_payload": payload_str}, "invalid_json")
            ack_processing(r, payload_str)
            continue
        except Exception as e:
            print("Unexpected parse error, sending to DLQ:", repr(e))
            send_to_dlq(r, {"raw_payload": payload_str}, f"parse_error: {repr(e)}")
            ack_processing(r, payload_str)
            continue

        trade = message.get("trade") or {}
        meta = message.get("meta") or {}
        attempt = int(meta.get("attempt", 0))

        try:
            tasks.upsert_trade(cur, trade)
            print(f"Upserted trade {trade.get('trade_id')}")
            ack_processing(r, payload_str)
        except Exception as e:
            # Roll back DB work
            try:
                conn.rollback()
            except Exception:
                pass

            attempt += 1
            meta["attempt"] = attempt
            message["meta"] = meta

            err = repr(e)
            trade_id = trade.get("trade_id")

            if attempt >= config.MAX_ATTEMPTS:
                print(f"Failed trade {trade_id} permanently (attempt={attempt}), DLQ:", err)
                send_to_dlq(r, message, err)
                ack_processing(r, payload_str)
            else:
                print(f"Failed trade {trade_id} (attempt={attempt}), retrying:", err)

                # Ack the original from processing and requeue updated message
                ack_processing(r, payload_str)
                r.lpush(config.QUEUE_KEY, json.dumps(message))

                time.sleep(config.RETRY_BACKOFF_SECONDS)


if __name__ == "__main__":
    main()