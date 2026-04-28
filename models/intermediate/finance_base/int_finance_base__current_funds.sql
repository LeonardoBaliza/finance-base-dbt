with funds as (

    select *
    from {{ ref('stg_finance_base__funds') }}
    where is_active

),

ranked as (

    select
        *,
        row_number() over (
            partition by cnpj
            order by updated_at desc nulls last, created_at desc nulls last, ticker
        ) as fund_rank
    from funds

)

select
    fund_id,
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
where fund_rank = 1
