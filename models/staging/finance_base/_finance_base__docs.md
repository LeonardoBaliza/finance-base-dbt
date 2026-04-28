{% docs finance_base_dbt_overview %}

Finance Base dbt transforms raw Brazilian fund data into BI-ready models.

Raw rows are preserved in Snowflake `RAW` tables as `PAYLOAD VARIANT`. Staging
models are the only layer that parses JSON, standardizes names, and casts data
types. Intermediate models handle reusable business logic. Marts expose the
Gold layer for dashboards and interview review.

{% enddocs %}

{% docs split_adjusted_distribution %}

Split-adjusted distribution values convert historical cash distributions into a
current-share equivalent using split events after the distribution base date.
This keeps distribution yield comparable across funds that changed share counts.

{% enddocs %}

{% docs dividend_yield_12m %}

Dividend yield 12m is the sum of split-adjusted distributions over the last
twelve months divided by the latest available closing price.

{% enddocs %}

{% docs price_to_book_ratio %}

P/VP is price divided by book value per share. Values are nullable when either
price or book value is missing or non-positive.

{% enddocs %}

{% docs ffo %}

FFO is a recurring operating cash-flow proxy reported quarterly. This project
uses the reported source value when available and pairs it with quarterly
distributions to estimate payout trends.

{% enddocs %}
