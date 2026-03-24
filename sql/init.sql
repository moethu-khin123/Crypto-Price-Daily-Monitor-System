CREATE TABLE IF NOT EXISTS crypto_prices (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(20) NOT NULL,
    price_usd NUMERIC(18,8) NOT NULL,
    source_last_updated_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (coin_id, source_last_updated_at)
);


DROP TABLE IF EXISTS daily_summary;

CREATE TABLE IF NOT EXISTS daily_summary (
    coin_id VARCHAR(20),
    summary_date DATE,
    avg_price NUMERIC(18,8),
    min_price NUMERIC(18,8),
    max_price NUMERIC(18,8),
    PRIMARY KEY (coin_id, summary_date)
);
