import time

import psycopg2
from psycopg2.extras import RealDictCursor

import config


def get_conn():
    # Retry because Postgres may not be ready when the worker container starts.
    last_err = None
    for _ in range(30):  # ~30 seconds
        try:
            conn = psycopg2.connect(config.DATABASE_URL, cursor_factory=RealDictCursor)
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as e:
            last_err = e
            time.sleep(1)

    raise last_err