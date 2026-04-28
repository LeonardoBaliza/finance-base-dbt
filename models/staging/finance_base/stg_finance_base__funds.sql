with source as (

    select payload
    from {{ source('finance_base_raw', 'raw_funds') }}

),

renamed as (

    select
        coalesce(payload:"id"::string, payload:"fund_id"::string, payload:"id_fnet"::string) as fund_id,
        regexp_replace(coalesce(payload:"cnpj"::string, payload:"fund_cnpj"::string), '[^0-9]', '') as cnpj,
        upper(coalesce(payload:"ticker"::string, payload:"codigo_negociacao"::string, payload:"symbol"::string)) as ticker,
        coalesce(payload:"name"::string, payload:"nome"::string, payload:"fund_name"::string, payload:"trading_name"::string) as fund_name,
        case
            when upper(coalesce(payload:"fund_type"::string, payload:"tipo_fundo"::string, payload:"type"::string)) like '%AGRO%' then 'FIAGRO'
            when upper(coalesce(payload:"fund_type"::string, payload:"tipo_fundo"::string, payload:"type"::string)) like '%FII%' then 'FII'
            else 'FII'
        end as fund_type,
        coalesce(payload:"administrator"::string, payload:"administrador"::string) as administrator,
        coalesce(payload:"manager"::string, payload:"gestor"::string, payload:"manager_name"::string) as manager,
        coalesce(payload:"segment"::string, payload:"segmento"::string, payload:"classification"::string) as segment,
        coalesce(try_to_boolean(payload:"is_active"::string), true) as is_active,
        try_to_timestamp_ntz(coalesce(payload:"created_at"::string, payload:"createdAt"::string)) as created_at,
        try_to_timestamp_ntz(coalesce(payload:"updated_at"::string, payload:"updatedAt"::string)) as updated_at
    from source

)

select *
from renamed
where cnpj is not null
  and ticker is not null
