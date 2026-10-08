WITH latest_day as ( 
    SELECT 
        CAST(MAX(market_timestamp) as date) as max_day
    FROM {{ ref('silver_clean_stock_quotes') }}
), 

latest_prices as ( 
    SELECT 
        ticker, 
        avg(current_price) AS avg_price, 
    FROM {{ ref('silver_clean_stock_quotes') }}
    JOIN latest_day ld 
    ON CAST(market_timestamp AS date) = ld.max_day
    GROUP BY ticker
), 

-- volatility across all data
volatility as ( 
    SELECT 
        ticker,
        stddev(current_price) AS volatility, 
        stddev(current_price) / NULLIF(AVG(current_price)), 0) AS relative_volatility
    FROM {{ ref('silver_clean_stock_quotes') }}
    GROUP BY ticker,
)

SELECT 
    lp.ticker,
    lp.avg_price,
    v.volatility,
    v.relative_volatility,
FROM latest_prices lp
JOIN volatility v
ON LP.ticker = v.ticker
ORDER BY lp.ticker