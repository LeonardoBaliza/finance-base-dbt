with source as (

    select payload
    from {{ source('finance_base_raw', 'raw_fii_quarterly_reports') }}

),

renamed as (

    select
        coalesce(payload:"id"::string, payload:"report_id"::string) as report_id,
        regexp_replace(coalesce(payload:"cnpj"::string, payload:"fund_cnpj"::string), '[^0-9]', '') as cnpj,
        upper(coalesce(payload:"ticker"::string, payload:"codigo_negociacao"::string, payload:"symbol"::string)) as ticker,
        date_trunc(
            'quarter',
            coalesce(
                try_to_date(payload:"competencia"::string),
                try_to_date(payload:"reference_date"::string),
                try_to_date(payload:"data_referencia"::string)
            )
        ) as quarter_start,
        coalesce(
            {{ safe_to_numeric('coalesce(payload:"ffo", payload:"funds_from_operations", payload:"resultado_recorrente")') }},
            {{ safe_to_numeric('payload:"data":"demonstracoes_contabil_financ":"resultado_trimestral_liquido":"financeiro"') }}
              - coalesce({{ safe_to_numeric('payload:"data":"demonstracoes_contabil_financ":"ativos_imobiliarios":"propriedades_invest":"receitas_venda":"financeiro"') }}, 0)
              - coalesce({{ safe_to_numeric('payload:"data":"demonstracoes_contabil_financ":"ativos_imobiliarios":"propriedades_invest":"custo_propriedades_vendidas":"financeiro"') }}, 0)
              - coalesce({{ safe_to_numeric('payload:"data":"demonstracoes_contabil_financ":"ativos_imobiliarios":"tvm":"ajuste_valor":"financeiro"') }}, 0)
              - coalesce({{ safe_to_numeric('payload:"data":"demonstracoes_contabil_financ":"ativos_imobiliarios":"tvm":"resultado_venda":"financeiro"') }}, 0)
              - coalesce({{ safe_to_numeric('payload:"data":"demonstracoes_contabil_financ":"ativos_imobiliarios":"estoques":"receitas_venda":"financeiro"') }}, 0)
              - coalesce({{ safe_to_numeric('payload:"data":"demonstracoes_contabil_financ":"ativos_imobiliarios":"estoques":"custo_imov_estoque":"financeiro"') }}, 0)
              - coalesce({{ safe_to_numeric('payload:"data":"demonstracoes_contabil_financ":"recursos_liquidez":"ajuste_valor_aplicacoes":"financeiro"') }}, 0)
              - coalesce({{ safe_to_numeric('payload:"data":"demonstracoes_contabil_financ":"recursos_liquidez":"resultado_venda_aplicacoes":"financeiro"') }}, 0)
        ) as ffo,
        coalesce(
            {{ safe_to_numeric('coalesce(payload:"resultado", payload:"net_income", payload:"lucro_liquido")') }},
            {{ safe_to_numeric('payload:"data":"demonstracoes_contabil_financ":"resultado_trimestral_liquido":"financeiro"') }}
        ) as net_income,
        {{ safe_to_numeric('coalesce(payload:"patrimonio_liquido", payload:"net_worth", payload:"pl")') }} as net_worth,
        try_to_timestamp_ntz(coalesce(payload:"created_at"::string, payload:"createdAt"::string)) as created_at,
        try_to_timestamp_ntz(coalesce(payload:"updated_at"::string, payload:"updatedAt"::string)) as updated_at
    from source
    where coalesce(payload:"situacao_documento"::string, 'A') = 'A'
      and payload:"extraction_error"::string is null

)

select *
from renamed
where ticker is not null
  and quarter_start is not null
