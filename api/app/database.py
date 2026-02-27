import os
import psycopg2


def get_conn():
    """
    Create a new Postgres connection.

    Note: For learning/simplicity we open a new connection per request.
    Later we can add connection pooling.
    """
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://admin:admin@postgres:5432/trading",
    )
    return psycopg2.connect(database_url)