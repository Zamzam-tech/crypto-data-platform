WITH prices AS (
    SELECT * FROM {{ ref('stg_coins') }}
),

coin_dim AS (
    SELECT * FROM {{ ref('dim_coin') }}
),

date_dim AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT
    coin_dim.coin_key,
    date_dim.date_key,
    prices.current_price,
    prices.market_cap,
    prices.total_volume,
    prices.last_updated,
    prices.ingested_at
FROM prices
LEFT JOIN coin_dim
    ON prices.coin_id = coin_dim.coin_id
LEFT JOIN date_dim
    ON DATE(prices.last_updated) = date_dim.full_date
