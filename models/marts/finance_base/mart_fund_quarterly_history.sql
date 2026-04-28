select
    md5(concat_ws('|', cnpj, quarter_start::string)) as quarterly_history_key,
    report_id,
    cnpj,
    ticker,
    quarter_start,
    ffo,
    net_income,
    net_worth,
    distributions_per_share,
    distribution_to_ffo_ratio
from {{ ref('int_finance_base__quarterly_ffo') }}
