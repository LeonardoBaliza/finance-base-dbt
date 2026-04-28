with monthly_reports as (

    select *
    from {{ ref('stg_finance_base__monthly_reports') }}
    qualify row_number() over (
        partition by cnpj, month_start
        order by updated_at desc nulls last, created_at desc nulls last, report_id desc
    ) = 1

),

all_tickers as (

    select cnpj, ticker, 1 as ticker_priority
    from {{ ref('int_finance_base__current_funds') }}

    union all

    select cnpj, ticker, 2 as ticker_priority
    from {{ ref('stg_finance_base__fund_ticker_history') }}

),

quotes_in_month as (

    select
        monthly_reports.report_id,
        quotes.date as quote_date,
        quotes.close_price,
        row_number() over (
            partition by monthly_reports.report_id
            order by all_tickers.ticker_priority asc, quotes.date desc
        ) as quote_rank
    from monthly_reports
    inner join all_tickers
        on monthly_reports.cnpj = all_tickers.cnpj
    inner join {{ ref('stg_finance_base__quotes') }} as quotes
        on all_tickers.ticker = quotes.ticker
        and quotes.date >= monthly_reports.month_start
        and quotes.date < dateadd(month, 1, monthly_reports.month_start)
        and quotes.close_price is not null

),

latest_quotes_in_month as (

    select
        report_id,
        quote_date,
        close_price
    from quotes_in_month
    where quote_rank = 1

)

select
    monthly_reports.report_id,
    monthly_reports.cnpj,
    monthly_reports.ticker,
    monthly_reports.fund_type,
    monthly_reports.month_start,
    monthly_reports.net_worth,
    monthly_reports.shareholder_count,
    monthly_reports.share_count,
    monthly_reports.book_value_per_share,
    latest_quotes_in_month.quote_date,
    latest_quotes_in_month.close_price,
    case
        when monthly_reports.book_value_per_share > 0
            then latest_quotes_in_month.close_price / monthly_reports.book_value_per_share
    end as price_to_book_ratio
from monthly_reports
left join latest_quotes_in_month
    on monthly_reports.report_id = latest_quotes_in_month.report_id
