WITH source AS(
    SELECT * FROM {{ source('crypto_data','raw_crypto_data') }}
),
renamed AS (
    SELECT
        id as coin_id,
        symbol,
        name as coin_name,
        current_price,
        market_cap,
        total_volume,
        CAST(last_updated_at AS TIMESTAMP) as last_updated,
        CAST(ingested_at AS TIMESTAMP) as ingested_at
    FROM source
)
SELECT * FROM renamed

