WITH enriched_candle AS (
    SELECT
        ticker,
        DATE_TRUNC('hour', market_timestamp) AS hour_candle,
        current_price,
        day_high,
        day_low,
        FIRST_VALUE(current_price) OVER(
            PARTITION BY ticker, CAST(market_timestamp AS date)
            ORDER BY market_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS candle_open,
        LAST_VALUE(current_price) OVER(
            PARTITION BY ticker, CAST(market_timestamp AS date)
            ORDER BY market_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS candle_close
    FROM {{ ref('silver_clean_stock_quotes') }}
),

candle_stick AS (
    SELECT
        ticker,
        hour_candle AS candle_time,
        MIN(current_price) AS candle_low,
        MAX(current_price) AS candle_high,
        ANY_VALUE(candle_open) AS candle_open,
        ANY_VALUE(candle_close) AS candle_close,
        AVG(current_price) AS trend_line
    FROM enriched_candle
    GROUP BY ticker, hour_candle
),

ranked AS (
    SELECT
        *,
        row_number() OVER(PARTITION BY ticker ORDER BY candle_time DESC) AS rank
    FROM candle_stick
)

SELECT
    ticker,
    candle_time,
    candle_low,
    candle_high,
    candle_open,
    candle_close,
    trend_line
FROM ranked
WHERE rank <= 12
ORDER BY ticker, candle_time