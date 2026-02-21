import redis
import psycopg2
import json
import time
import os

# Redis connection
r = redis.from_url(os.getenv("REDIS_URL"))

# Postgres connection
conn = psycopg2.connect(
    host="postgres",        # must match service name
    database="trading",
    user="admin",
    password="admin"
)
conn.autocommit = True
cur = conn.cursor()

print("Worker started...")

while True:
    item = r.brpop("trade_queue", timeout=5)

    if item:
        _, data = item
        trade = json.loads(data)

        try:
            cur.execute(
                """
                INSERT INTO trades_raw (trade_id, user_id, symbol, price)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    trade["trade_id"],
                    trade["user"],
                    trade["symbol"],
                    trade["price"]
                )
            )
            print(f"Inserted trade {trade['trade_id']}")

        except psycopg2.errors.UniqueViolation:
            conn.rollback()  # important after exception
            print(f"Duplicate trade ignored: {trade['trade_id']}")

    else:
        time.sleep(1)