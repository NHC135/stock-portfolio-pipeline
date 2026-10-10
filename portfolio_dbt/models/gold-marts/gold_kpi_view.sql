WITH ordered_stock_prices AS ( 
SELECT 
    *,
    ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY fetched_at DESC) AS ticker_order
FROM {{ ref('silver_clean_stock_quotes') }}
)

SELECT 
    ticker, 
    current_price, 
    price_change, 
    percent_change
FROM ordered_stock_prices
WHERE ticker_order = 1