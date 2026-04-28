with quarterly_reports as (

    select *
    from {{ ref('stg_finance_base__quarterly_reports') }}
    qualify row_number() over (
        partition by cnpj, quarter_start
        order by updated_at desc nulls last, created_at desc nulls last, report_id desc
    ) = 1

),

quarterly_distributions as (

    select
        cnpj,
        date_trunc('quarter', base_date) as quarter_start,
        sum(adjusted_amount) as distributions_per_share
    from {{ ref('int_finance_base__distribution_yields') }}
    group by cnpj, date_trunc('quarter', base_date)

),

monthly_metrics_in_quarter as (

    select
        quarterly_reports.cnpj,
        quarterly_reports.quarter_start,
        monthly_metrics.net_worth,
        row_number() over (
            partition by quarterly_reports.cnpj, quarterly_reports.quarter_start
            order by monthly_metrics.month_start desc
        ) as monthly_rank
    from quarterly_reports
    inner join {{ ref('int_finance_base__monthly_metrics') }} as monthly_metrics
        on quarterly_reports.cnpj = monthly_metrics.cnpj
        and monthly_metrics.month_start >= quarterly_reports.quarter_start
        and monthly_metrics.month_start < dateadd(quarter, 1, quarterly_reports.quarter_start)
        and monthly_metrics.net_worth is not null

)

select
    quarterly_reports.report_id,
    quarterly_reports.cnpj,
    quarterly_reports.ticker,
    quarterly_reports.quarter_start,
    quarterly_reports.ffo,
    quarterly_reports.net_income,
    coalesce(quarterly_reports.net_worth, monthly_metrics_in_quarter.net_worth) as net_worth,
    quarterly_distributions.distributions_per_share,
    case
        when quarterly_reports.ffo > 0
            then quarterly_distributions.distributions_per_share / quarterly_reports.ffo
    end as distribution_to_ffo_ratio
from quarterly_reports
left join quarterly_distributions
    on quarterly_reports.cnpj = quarterly_distributions.cnpj
    and quarterly_reports.quarter_start = quarterly_distributions.quarter_start
left join monthly_metrics_in_quarter
    on quarterly_reports.cnpj = monthly_metrics_in_quarter.cnpj
    and quarterly_reports.quarter_start = monthly_metrics_in_quarter.quarter_start
    and monthly_metrics_in_quarter.monthly_rank = 1
