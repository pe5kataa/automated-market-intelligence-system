SELECT
    "Ticker" as ticker,
    "Date" as date,
    "Open" as open,
    "High" as high,
    "Low" as low,
    "Close" as close,
    "Volume" as volume
FROM {{ source('raw', 'raw_stock_prices') }}
ORDER BY date DESC;


