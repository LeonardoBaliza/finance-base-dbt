{% macro adjusted_by_split(amount_expression, cumulative_split_factor_expression) -%}
    {{ amount_expression }} * coalesce({{ cumulative_split_factor_expression }}, 1)
{%- endmacro %}
