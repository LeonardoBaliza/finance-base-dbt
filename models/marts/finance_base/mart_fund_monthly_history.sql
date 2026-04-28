select
    md5(concat_ws('|', cnpj, month_start::string)) as monthly_history_key,
    report_id,
    cnpj,
    ticker,
    fund_type,
    month_start,
    net_worth,
    shareholder_count,
    share_count,
    book_value_per_share,
    quote_date,
    close_price,
    price_to_book_ratio
from {{ ref('int_finance_base__monthly_metrics') }}
