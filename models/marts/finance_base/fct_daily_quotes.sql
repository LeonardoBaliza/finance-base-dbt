select
    md5(concat_ws('|', ticker, date::string)) as quote_key,
    ticker,
    date,
    open_price,
    high_price,
    low_price,
    close_price,
    financial_volume,
    trade_count,
    created_at,
    updated_at
from {{ ref('stg_finance_base__quotes') }}
