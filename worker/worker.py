import json
import time
import redis

import config
import database
import tasks


def main():
    r = redis.from_url(config.REDIS_URL)
    conn = database.get_conn()
    cur = conn.cursor()

    print("Worker started...")

    while True:
        item = r.brpop(config.QUEUE_NAME, timeout=config.BRPOP_TIMEOUT_SECONDS)

        if not item:
            time.sleep(config.IDLE_SLEEP_SECONDS)
            continue

        _, payload = item

        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8")

        try:
            trade = json.loads(payload)
        except json.JSONDecodeError:
            print("Invalid JSON payload:", payload)
            continue

        try:
            tasks.upsert_trade(cur, trade)
            print(f"Upserted trade {trade.get('trade_id')}")
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            print("Failed processing trade:", trade, "error:", repr(e))


if __name__ == "__main__":
    main()