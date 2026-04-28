with distributions as (

    select
        md5(concat_ws('|', cnpj, base_date::string, coalesce(distribution_type, ''), coalesce(trading_code, ''), coalesce(doc_id, distribution_id, ''))) as distribution_key,
        distribution_id,
        doc_id,
        cnpj,
        ticker,
        trading_code,
        base_date,
        payment_date,
        amount,
        distribution_type,
        created_at,
        updated_at
    from {{ ref('stg_finance_base__fund_distributions') }}

),

deduplicated_distributions as (

    select
        distribution_key,
        distribution_id,
        doc_id,
        cnpj,
        ticker,
        trading_code,
        base_date,
        payment_date,
        amount,
        distribution_type
    from distributions
    qualify row_number() over (
        partition by distribution_key
        order by updated_at desc nulls last, created_at desc nulls last, distribution_id desc
    ) = 1

),

funds as (

    select
        cnpj,
        ticker as current_ticker,
        fund_type
    from {{ ref('int_finance_base__current_funds') }}

),

all_tickers as (

    select
        cnpj,
        current_ticker as ticker,
        1 as ticker_priority
    from funds

    union all

    select
        cnpj,
        ticker,
        2 as ticker_priority
    from {{ ref('stg_finance_base__fund_ticker_history') }}

),

resolved_distributions as (

    select
        deduplicated_distributions.distribution_key,
        deduplicated_distributions.distribution_id,
        deduplicated_distributions.doc_id,
        deduplicated_distributions.cnpj,
        deduplicated_distributions.ticker,
        funds.current_ticker,
        funds.fund_type,
        deduplicated_distributions.trading_code,
        deduplicated_distributions.base_date,
        deduplicated_distributions.payment_date,
        deduplicated_distributions.amount,
        deduplicated_distributions.distribution_type
    from deduplicated_distributions
    inner join funds
        on deduplicated_distributions.cnpj = funds.cnpj
    where deduplicated_distributions.trading_code is null
       or deduplicated_distributions.trading_code = funds.current_ticker
       or exists (
            select 1
            from {{ ref('stg_finance_base__fund_ticker_history') }} as ticker_history
            where ticker_history.cnpj = deduplicated_distributions.cnpj
              and ticker_history.ticker = deduplicated_distributions.trading_code
       )

),

split_factors as (

    select
        distribution_dates.cnpj,
        distribution_dates.base_date,
        coalesce(nullif(exp(sum(ln(splits.split_factor))), 0), 1) as cumulative_split_factor
    from (
        select distinct cnpj, base_date
        from resolved_distributions
    ) as distribution_dates
    left join {{ ref('stg_finance_base__funds_splits') }} as splits
        on distribution_dates.cnpj = splits.cnpj
        and splits.split_date >= distribution_dates.base_date
    group by distribution_dates.cnpj, distribution_dates.base_date

),

best_quotes as (

    select
        resolved_distributions.cnpj,
        resolved_distributions.base_date,
        quotes.date as quote_date,
        quotes.close_price as base_close_price,
        row_number() over (
            partition by resolved_distributions.cnpj, resolved_distributions.base_date
            order by all_tickers.ticker_priority asc, quotes.date desc
        ) as quote_rank
    from resolved_distributions
    inner join all_tickers
        on resolved_distributions.cnpj = all_tickers.cnpj
    inner join {{ ref('stg_finance_base__quotes') }} as quotes
        on all_tickers.ticker = quotes.ticker
        and quotes.date <= resolved_distributions.base_date
        and quotes.close_price is not null

),

latest_quotes_at_base as (

    select
        cnpj,
        base_date,
        quote_date,
        base_close_price
    from best_quotes
    where quote_rank = 1

)

select
    resolved_distributions.distribution_key,
    resolved_distributions.distribution_id,
    resolved_distributions.doc_id,
    resolved_distributions.cnpj,
    resolved_distributions.ticker,
    resolved_distributions.current_ticker,
    resolved_distributions.fund_type,
    resolved_distributions.trading_code,
    resolved_distributions.base_date,
    resolved_distributions.payment_date,
    resolved_distributions.amount,
    {{ adjusted_by_split('resolved_distributions.amount', 'split_factors.cumulative_split_factor') }} as adjusted_amount,
    split_factors.cumulative_split_factor,
    latest_quotes_at_base.quote_date,
    latest_quotes_at_base.base_close_price,
    case
        when latest_quotes_at_base.base_close_price > 0
            and {{ adjusted_by_split('resolved_distributions.amount', 'split_factors.cumulative_split_factor') }} <= latest_quotes_at_base.base_close_price
            then {{ adjusted_by_split('resolved_distributions.amount', 'split_factors.cumulative_split_factor') }} / latest_quotes_at_base.base_close_price
    end as distribution_yield,
    resolved_distributions.distribution_type
from resolved_distributions
left join split_factors
    on resolved_distributions.cnpj = split_factors.cnpj
    and resolved_distributions.base_date = split_factors.base_date
left join latest_quotes_at_base
    on resolved_distributions.cnpj = latest_quotes_at_base.cnpj
    and resolved_distributions.base_date = latest_quotes_at_base.base_date
