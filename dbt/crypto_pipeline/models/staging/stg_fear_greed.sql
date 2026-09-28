WITH source AS(
    SELECT * FROM {{ source('crypto_data','raw_fear_greed') }}
),
renamed AS (
    SELECT
        score as fear_greed_score,
        classification,
        fetch_date,
        fetched_at
    FROM source
)
SELECT * FROM renamed
