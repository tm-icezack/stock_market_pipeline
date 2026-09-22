select
    date,
    ticker,
    open,
    high,
    low,
    close,
    adj_close,
    volume
from {{ ref('stg_stock_prices') }}
