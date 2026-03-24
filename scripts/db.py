import os
import psycopg2


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        database=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )


def insert_prices(rows):
    conn = get_connection()
    cur = conn.cursor()

    query = """
    INSERT INTO crypto_prices (coin_id, price_usd, source_last_updated_at)
    VALUES (%s, %s, %s)
    ON CONFLICT (coin_id, source_last_updated_at) DO NOTHING;
    """

    for row in rows:
        cur.execute(
            query,
            (row["coin_id"], row["price_usd"], row["source_last_updated_at"])
        )

    conn.commit()
    cur.close()
    conn.close()