with funds as (

    select *
    from {{ ref('dim_funds') }}

),

latest_quotes as (

    select
        ticker,
        date as latest_quote_date,
        close_price as latest_close_price,
        row_number() over (
            partition by ticker
            order by date desc
        ) as quote_rank
    from {{ ref('fct_daily_quotes') }}

),

current_quotes as (

    select
        ticker,
        latest_quote_date,
        latest_close_price
    from latest_quotes
    where quote_rank = 1

),

distributions_12m as (

    select
        distributions.current_ticker as ticker,
        sum(distributions.adjusted_amount) as distributions_per_share_12m
    from {{ ref('fct_distributions') }} as distributions
    inner join current_quotes
        on distributions.current_ticker = current_quotes.ticker
        and distributions.base_date > dateadd(month, -12, current_quotes.latest_quote_date)
        and distributions.base_date <= current_quotes.latest_quote_date
    group by distributions.current_ticker

),

latest_monthly as (

    select
        cnpj,
        ticker,
        month_start,
        net_worth,
        shareholder_count,
        book_value_per_share,
        row_number() over (
            partition by cnpj
            order by month_start desc
        ) as monthly_rank
    from {{ ref('int_finance_base__monthly_metrics') }}

),

current_monthly as (

    select
        cnpj,
        ticker,
        month_start as latest_month_start,
        net_worth,
        shareholder_count,
        book_value_per_share
    from latest_monthly
    where monthly_rank = 1

)

select
    funds.cnpj,
    funds.ticker,
    funds.fund_name,
    funds.fund_type,
    funds.segment,
    current_quotes.latest_quote_date,
    current_quotes.latest_close_price,
    current_monthly.latest_month_start,
    current_monthly.net_worth,
    current_monthly.shareholder_count,
    current_monthly.book_value_per_share,
    case
        when current_quotes.latest_close_price > 0
            and current_monthly.book_value_per_share > 0
            then current_quotes.latest_close_price / current_monthly.book_value_per_share
    end as price_to_book_ratio,
    distributions_12m.distributions_per_share_12m,
    case
        when current_quotes.latest_close_price > 0
            then distributions_12m.distributions_per_share_12m / current_quotes.latest_close_price
    end as dividend_yield_12m
from funds
left join current_quotes
    on funds.ticker = current_quotes.ticker
left join current_monthly
    on funds.cnpj = current_monthly.cnpj
left join distributions_12m
    on funds.ticker = distributions_12m.ticker
