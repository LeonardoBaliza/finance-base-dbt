with source as (

    select payload
    from {{ source('finance_base_raw', 'raw_fund_distributions') }}

),

renamed as (

    select
        coalesce(payload:"id"::string, payload:"distribution_id"::string) as distribution_id,
        coalesce(payload:"doc_id"::string, payload:"document_id"::string) as doc_id,
        regexp_replace(coalesce(payload:"cnpj"::string, payload:"fund_cnpj"::string), '[^0-9]', '') as cnpj,
        upper(coalesce(payload:"ticker"::string, payload:"codigo_negociacao"::string, payload:"symbol"::string)) as ticker,
        upper(coalesce(payload:"cod_negociacao"::string, payload:"codigo_negociacao"::string, payload:"ticker"::string, payload:"symbol"::string)) as trading_code,
        coalesce(
            try_to_date(payload:"data_base"::string),
            try_to_date(payload:"base_date"::string),
            try_to_date(payload:"data_com"::string)
        ) as base_date,
        coalesce(
            try_to_date(payload:"data_pagamento"::string),
            try_to_date(payload:"payment_date"::string)
        ) as payment_date,
        {{ safe_to_numeric('coalesce(payload:"valor", payload:"value", payload:"amount", payload:"valor_provento")') }} as amount,
        coalesce(payload:"tipo"::string, payload:"distribution_type"::string, payload:"event_type"::string) as distribution_type,
        coalesce(payload:"situacao_documento"::string, payload:"document_status"::string, 'A') as document_status,
        coalesce(try_to_boolean(payload:"is_active"::string), true) as is_active,
        try_to_timestamp_ntz(coalesce(payload:"created_at"::string, payload:"createdAt"::string)) as created_at,
        try_to_timestamp_ntz(coalesce(payload:"updated_at"::string, payload:"updatedAt"::string)) as updated_at
    from source

)

select *
from renamed
where ticker is not null
  and base_date is not null
  and amount is not null
  and amount >= 0
  and (payment_date is null or payment_date >= base_date)
  and document_status = 'A'
  and is_active
