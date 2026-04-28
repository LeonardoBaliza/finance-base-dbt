with source as (

    select payload
    from {{ source('finance_base_raw', 'raw_funds_splits') }}

),

renamed as (

    select
        coalesce(payload:"id"::string, payload:"split_id"::string) as split_id,
        regexp_replace(coalesce(payload:"cnpj"::string, payload:"fund_cnpj"::string), '[^0-9]', '') as cnpj,
        coalesce(
            try_to_date(payload:"last_date_prior"::string),
            try_to_date(payload:"split_date"::string),
            try_to_date(payload:"data_base"::string),
            try_to_date(payload:"data_referencia"::string)
        ) as split_date,
        coalesce(
            {{ safe_to_numeric('payload:"split_factor"') }},
            {{ safe_to_numeric('payload:"factor"') }},
            nullif({{ safe_to_numeric('payload:"new_quantity"') }}, 0) / nullif({{ safe_to_numeric('payload:"old_quantity"') }}, 0)
        ) as split_factor,
        try_to_timestamp_ntz(coalesce(payload:"created_at"::string, payload:"createdAt"::string)) as created_at,
        try_to_timestamp_ntz(coalesce(payload:"updated_at"::string, payload:"updatedAt"::string)) as updated_at
    from source

)

select *
from renamed
where cnpj is not null
  and split_date is not null
  and split_factor is not null
  and split_factor > 0
