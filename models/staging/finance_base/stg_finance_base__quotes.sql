with source as (

    select payload
    from {{ source('finance_base_raw', 'raw_quotes') }}

),

renamed as (

    select
        upper(coalesce(payload:"ticker"::string, payload:"codigo_negociacao"::string, payload:"symbol"::string)) as ticker,
        coalesce(
            try_to_date(payload:"date"::string),
            try_to_date(payload:"quote_date"::string),
            try_to_date(payload:"data_pregao"::string),
            try_to_date(payload:"data_referencia"::string)
        ) as date,
        {{ safe_to_numeric('coalesce(payload:"open", payload:"open_price", payload:"preco_abertura")') }} as open_price,
        {{ safe_to_numeric('coalesce(payload:"high", payload:"high_price", payload:"preco_maximo")') }} as high_price,
        {{ safe_to_numeric('coalesce(payload:"low", payload:"low_price", payload:"preco_minimo")') }} as low_price,
        {{ safe_to_numeric('coalesce(payload:"close", payload:"close_price", payload:"adj_close", payload:"preco_fechamento")') }} as close_price,
        {{ safe_to_numeric('coalesce(payload:"volume", payload:"financial_volume", payload:"volume_financeiro")') }} as financial_volume,
        {{ safe_to_numeric('coalesce(payload:"trades", payload:"trade_count", payload:"negocios")') }} as trade_count,
        try_to_timestamp_ntz(coalesce(payload:"created_at"::string, payload:"createdAt"::string)) as created_at,
        try_to_timestamp_ntz(coalesce(payload:"updated_at"::string, payload:"updatedAt"::string)) as updated_at
    from source

),

normalized as (

    select
        ticker,
        date,
        open_price,
        greatest_ignore_nulls(high_price, open_price, close_price) as high_price,
        least_ignore_nulls(low_price, open_price, close_price) as low_price,
        close_price,
        financial_volume,
        trade_count,
        created_at,
        updated_at
    from renamed
    where ticker is not null
      and date is not null

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by ticker, date
            order by
                updated_at desc nulls last,
                created_at desc nulls last,
                financial_volume desc nulls last,
                trade_count desc nulls last,
                close_price desc nulls last
        ) as quote_rank
    from normalized

)

select
    ticker,
    date,
    open_price,
    high_price,
    low_price,
    close_price,
    financial_volume,
    trade_count,
    created_at,
    updated_at
from deduplicated
where quote_rank = 1
