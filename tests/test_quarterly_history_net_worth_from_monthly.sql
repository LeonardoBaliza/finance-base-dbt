with quarterly_history as (

    select
        cnpj,
        quarter_start,
        net_worth
    from {{ ref('mart_fund_quarterly_history') }}

),

monthly_metrics_in_quarter as (

    select
        quarterly_history.cnpj,
        quarterly_history.quarter_start,
        monthly_metrics.net_worth,
        row_number() over (
            partition by quarterly_history.cnpj, quarterly_history.quarter_start
            order by monthly_metrics.month_start desc
        ) as monthly_rank
    from quarterly_history
    inner join {{ ref('int_finance_base__monthly_metrics') }} as monthly_metrics
        on quarterly_history.cnpj = monthly_metrics.cnpj
        and monthly_metrics.month_start >= quarterly_history.quarter_start
        and monthly_metrics.month_start < dateadd(quarter, 1, quarterly_history.quarter_start)
        and monthly_metrics.net_worth is not null

)

select
    quarterly_history.cnpj,
    quarterly_history.quarter_start
from quarterly_history
inner join monthly_metrics_in_quarter
    on quarterly_history.cnpj = monthly_metrics_in_quarter.cnpj
    and quarterly_history.quarter_start = monthly_metrics_in_quarter.quarter_start
    and monthly_metrics_in_quarter.monthly_rank = 1
where quarterly_history.net_worth is null
