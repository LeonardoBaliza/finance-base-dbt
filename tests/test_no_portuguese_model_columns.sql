select
    table_schema,
    table_name,
    column_name
from {{ target.database }}.information_schema.columns
where table_schema in (
    upper('{{ target.schema }}'),
    upper('{{ target.schema }}_BRONZE'),
    upper('{{ target.schema }}_SILVER'),
    upper('{{ target.schema }}_GOLD'),
    'BRONZE',
    'SILVER',
    'GOLD'
)
and regexp_like(
    lower(column_name),
    '(^|_)(patrimonio|liquido|numero|cotistas|cotas|valor|data|pagamento|negociacao)($|_)'
)
