select
    date,
    ticker,
    close,
    lag(close) over (partition by ticker order by date) as prev_close,
    round(
        (close - lag(close) over (partition by ticker order by date))
        / nullif(lag(close) over (partition by ticker order by date), 0) * 100,
        4
    ) as daily_return_pct
from {{ ref('fct_stock_prices') }}
