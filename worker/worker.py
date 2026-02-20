import redis
import os
import json
import time

r = redis.from_url(os.getenv("REDIS_URL"))

print("Worker started...")

while True:
    item = r.brpop("trade_queue", timeout=5)

    if item:
        _, payload = item
        trade = json.loads(payload)

        print("Processing trade:", trade)

    time.sleep(1)
