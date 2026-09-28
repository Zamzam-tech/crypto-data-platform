WITH source AS(
      SELECT DISTINCT coin_id, symbol,coin_name FROM {{ref ('stg_coins')}}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['coin_id']) }} AS coin_key,
    coin_id,
    symbol,
    coin_name
FROM source
