WITH ordered_stock_prices AS ( 
SELECT 
    *,
    ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY fetched_at DESC) AS ticker_order
FROM {{ ref ('silver_clean_stock_quotes') }}
)

SELECT 
symbol, 
current_price, 
change_amount, 
change_percent, 
FROM ordered_stock_prices
WHERE ticker_order = 1