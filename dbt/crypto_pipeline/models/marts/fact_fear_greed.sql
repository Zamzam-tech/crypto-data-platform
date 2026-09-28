WITH  date_dim AS(
         SELECT date_key, full_date FROM {{ref ('dim_date')}}
),
sentiment AS(
         SELECT * FROM {{ref ('stg_fear_greed')}}
)
SELECT 
date_dim.date_key,
sentiment.fear_greed_score,
sentiment.classification,
sentiment.fetch_date,
sentiment.fetched_at
FROM sentiment
LEFT JOIN date_dim 
   ON sentiment.fetch_date=date_dim.full_date


