with current_funds as (

    select *
    from {{ ref('int_finance_base__current_funds') }}

),

ranked as (

    select
        *,
        row_number() over (
            partition by ticker
            order by updated_at desc nulls last, created_at desc nulls last, cnpj
        ) as ticker_rank
    from current_funds

)

select
    cnpj,
    ticker,
    fund_name,
    fund_type,
    administrator,
    manager,
    segment,
    is_active,
    created_at,
    updated_at
from ranked
where ticker_rank = 1
