with source as (

    select payload
    from {{ source('finance_base_raw', 'raw_fund_ticker_history') }}

),

renamed as (

    select
        coalesce(payload:"id"::string, payload:"ticker_history_id"::string) as ticker_history_id,
        regexp_replace(coalesce(payload:"cnpj"::string, payload:"fund_cnpj"::string), '[^0-9]', '') as cnpj,
        upper(coalesce(payload:"ticker"::string, payload:"codigo_negociacao"::string, payload:"symbol"::string)) as ticker,
        coalesce(
            try_to_date(payload:"valid_from"::string),
            try_to_date(payload:"start_date"::string),
            try_to_date(payload:"data_inicio"::string)
        ) as valid_from,
        coalesce(
            try_to_date(payload:"valid_to"::string),
            try_to_date(payload:"end_date"::string),
            try_to_date(payload:"data_fim"::string)
        ) as valid_to,
        try_to_timestamp_ntz(coalesce(payload:"created_at"::string, payload:"createdAt"::string)) as created_at,
        try_to_timestamp_ntz(coalesce(payload:"updated_at"::string, payload:"updatedAt"::string)) as updated_at
    from source

)

select *
from renamed
where cnpj is not null
  and ticker is not null
