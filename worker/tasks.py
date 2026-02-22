def upsert_trade(cur, trade: dict) -> None:
    """
    Expected trade shape (from API/queue):
      {
        "trade_id": "...",
        "user": "...",   # mapped to user_id
        "symbol": "...",
        "price": 123.45
      }
    """
    cur.execute(
        """
        INSERT INTO trades_raw (trade_id, user_id, symbol, price)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (trade_id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            symbol  = EXCLUDED.symbol,
            price   = EXCLUDED.price
        """,
        (
            trade["trade_id"],
            trade["user"],   # mapping queue field "user" -> DB column user_id
            trade["symbol"],
            trade["price"],
        ),
    )