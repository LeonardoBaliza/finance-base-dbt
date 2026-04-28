select
    distribution_key,
    distribution_id,
    doc_id,
    cnpj,
    ticker as original_ticker,
    current_ticker,
    fund_type,
    trading_code,
    base_date,
    payment_date,
    amount,
    adjusted_amount,
    cumulative_split_factor,
    quote_date,
    base_close_price,
    distribution_yield,
    distribution_type
from {{ ref('int_finance_base__distribution_yields') }}
