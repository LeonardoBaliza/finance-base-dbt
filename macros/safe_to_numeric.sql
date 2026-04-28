{% macro safe_to_numeric(expression, precision=38, scale=10) -%}
    try_to_decimal(nullif(replace(to_varchar({{ expression }}), ',', '.'), ''), {{ precision }}, {{ scale }})
{%- endmacro %}
