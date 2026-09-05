with base as (
    select
        ticker,
        date,
        close,
        close - lag(close) over (partition by ticker order by date asc) as price_change,
        (close - lag(close) over (partition by ticker order by date asc))
            / lag(close) over (partition by ticker order by date asc) as daily_return
    from {{ ref('stg_stock_prices') }}
)

select
    *,
    avg(close) over (
        partition by ticker order by date asc
        rows between 6 preceding and current row
    ) as ma_7d,

    avg(close) over (
        partition by ticker order by date asc
        rows between 29 preceding and current row
    ) as ma_30d,

    stddev(daily_return) over (
        partition by ticker order by date asc
        rows between 6 preceding and current row
    ) as vol_7d,

    stddev(daily_return) over (
        partition by ticker order by date asc
        rows between 29 preceding and current row
    ) as vol_30d
from base
order by date DESC
