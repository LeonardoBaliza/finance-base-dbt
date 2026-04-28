with fii_reports as (

    select
        payload,
        'FII' as fund_type
    from {{ source('finance_base_raw', 'raw_fii_monthly_reports') }}

),

fiagro_reports as (

    select
        payload,
        'FIAGRO' as fund_type
    from {{ source('finance_base_raw', 'raw_fiagro_monthly_reports') }}

),

unioned as (

    select * from fii_reports
    union all
    select * from fiagro_reports

),

renamed as (

    select
        coalesce(payload:"id"::string, payload:"report_id"::string) as report_id,
        fund_type,
        regexp_replace(coalesce(payload:"cnpj"::string, payload:"fund_cnpj"::string), '[^0-9]', '') as cnpj,
        upper(coalesce(payload:"ticker"::string, payload:"codigo_negociacao"::string, payload:"symbol"::string)) as ticker,
        date_trunc(
            'month',
            coalesce(
                try_to_date(payload:"competencia"::string),
                try_to_date(payload:"reference_date"::string),
                try_to_date(payload:"data_referencia"::string)
            )
        ) as month_start,
        {{ safe_to_numeric('coalesce(payload:"patrimonio_liquido", payload:"net_worth", payload:"pl", payload:"data":"informe_mensal":"resumo":"patrimonio_liquido", payload:"data":"lista_inform":"vl_patrimonio_liquido")') }} as net_worth,
        {{ safe_to_numeric('coalesce(payload:"numero_cotistas", payload:"shareholder_count", payload:"cotistas", payload:"data":"informe_mensal":"cotistas":"_total", payload:"data":"lista_tp_cotst":"qtd_tot_cotst")') }} as shareholder_count,
        {{ safe_to_numeric('coalesce(payload:"numero_cotas", payload:"share_count", payload:"cotas_emitidas", payload:"quantidade_cotas", payload:"data":"informe_mensal":"resumo":"num_cotas_emitidas", payload:"data":"lista_inform":"nr_cot_emitidas")') }} as share_count,
        coalesce(
            {{ safe_to_numeric('coalesce(payload:"valor_patrimonial_por_cota", payload:"book_value_per_share", payload:"vp_cota")') }},
            {{ safe_to_numeric('coalesce(payload:"patrimonio_liquido", payload:"net_worth", payload:"pl", payload:"data":"informe_mensal":"resumo":"patrimonio_liquido", payload:"data":"lista_inform":"vl_patrimonio_liquido")') }}
                / nullif({{ safe_to_numeric('coalesce(payload:"numero_cotas", payload:"share_count", payload:"cotas_emitidas", payload:"quantidade_cotas", payload:"data":"informe_mensal":"resumo":"num_cotas_emitidas", payload:"data":"lista_inform":"nr_cot_emitidas")') }}, 0)
        ) as book_value_per_share,
        try_to_timestamp_ntz(coalesce(payload:"created_at"::string, payload:"createdAt"::string)) as created_at,
        try_to_timestamp_ntz(coalesce(payload:"updated_at"::string, payload:"updatedAt"::string)) as updated_at
    from unioned
    where coalesce(payload:"situacao_documento"::string, 'A') = 'A'
      and payload:"extraction_error"::string is null

)

select *
from renamed
where ticker is not null
  and month_start is not null
