SELECT 
    ticker, 
    ROUND(current_price, 2) AS current_price, 
    ROUND(price_change, 2) AS price_change, 
    ROUND(percent_change, 2) AS percent_change, 
    ROUND(day_high, 2) AS day_high,
    ROUND(day_open, 2) AS day_open, 
    ROUND(prev_close, 2) AS prev_close,
    market_timestamp, 
    fetched_at
FROM {{ ref('stg_bronze_stock_quotes') }}
WHERE current_price IS NOT NULL 