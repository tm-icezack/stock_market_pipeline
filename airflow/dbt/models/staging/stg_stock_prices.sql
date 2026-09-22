select
    date,
    ticker,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    ingestion_timestamp,
    data_source
from {{ source('staging', 'stg_stock_prices') }}
where close is not null
  and date  is not null
