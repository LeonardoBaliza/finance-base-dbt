# Finance Base dbt Mini Project

This repository contains a small dbt project that models Finance Base data in
Snowflake.

For the reasoning behind the main modeling choices, see
[EXPLAIN.md](EXPLAIN.md).

## Architecture

The project uses a medallion layout mapped to Snowflake schemas:

| dbt folder | Snowflake schema | Purpose | Materialization |
| --- | --- | --- | --- |
| `models/staging` | `BRONZE` | source-conformed parsing from raw `PAYLOAD VARIANT` rows | view |
| `models/intermediate` | `SILVER` | reusable business logic and grain changes | view |
| `models/marts` | `GOLD` | BI-facing dimensions, facts, and marts | table |

Raw Snowflake tables are referenced only through `source()`. Downstream models
use `ref()` for lineage and testability.

## Snowflake Targets

- Account identifier: `scnsmfn-hhc26274`
- Database: `FINANCE_BASE_DBT`
- Warehouse: `FINANCE_BASE_WH`
- dbt role: `FINANCE_BASE_DBT_ROLE`
- read-only role: `FINANCE_BASE_READONLY_ROLE`
- Raw schema: `RAW`
- BI schema: `GOLD`

Admin setup for warehouses, roles, users, and grants requires Snowflake admin
roles and is not run by this repository.

## Local Setup

Install dependencies:

```bash
uv sync
```

Create a local dbt profile:

```bash
mkdir -p ~/.dbt
cp profiles.example.yml ~/.dbt/profiles.yml
set -a && source .env.local && set +a
```

Install dbt packages:

```bash
make deps
```

Validate the connection:

```bash
make debug
```

## Source Data

The staging layer expects these Snowflake raw tables in `FINANCE_BASE_DBT.RAW`:

- `FUNDS`
- `QUOTES`
- `FUND_DISTRIBUTIONS`
- `FUNDS_SPLITS`
- `FUND_TICKER_HISTORY`
- `FII_MONTHLY_REPORTS`
- `FIAGRO_MONTHLY_REPORTS`
- `FII_QUARTERLY_REPORTS`

Each raw table is expected to expose a `PAYLOAD VARIANT` column. The staging
models parse common English and Portuguese field names so the mini project can
work with representative extracts.

If using the source migration script from the reference project:

```bash
uv run python scripts/migrate_supabase_to_snowflake_raw.py \
  --target-database FINANCE_BASE_DBT \
  --target-schema RAW \
  --truncate
```

## Model Lineage

Core mart outputs:

- `dim_funds`: current fund dimension.
- `fct_daily_quotes`: daily OHLCV fact table.
- `fct_distributions`: split-adjusted distribution fact table.
- `mart_fund_indicators`: current price, P/VP, PL, cotistas, and DY 12m.
- `mart_fund_monthly_history`: monthly PL, cotistas, VP/cota, and P/VP trend.
- `mart_fund_quarterly_history`: quarterly FFO, net income, distributions, and payout trend.

Business logic highlights:

- current fund records are selected by CNPJ using latest update timestamps;
- historical distributions are adjusted by split factors after the base date;
- DY 12m uses the latest quote date per fund and the last twelve months of distributions;
- monthly P/VP uses the latest quote available in each reporting month.

## Commands

Build everything:

```bash
make build
```

Focused development:

```bash
set -a && source .env.local && set +a
uv run dbt build --profiles-dir . --select stg_finance_base__funds+
uv run dbt build --profiles-dir . --select int_finance_base__distribution_yields+
uv run dbt build --profiles-dir . --select mart_fund_indicators+
```

Generate docs:

```bash
make docs-generate
make docs-serve
```

## SQL Style

- lowercase SQL keywords and identifiers;
- descriptive CTE names;
- one model per transformation purpose;
- explicit column lists outside staging;
- comments only for non-obvious business rules;
- simple `ref()`, `source()`, and macro usage.

## Interview Walkthrough

1. Show `dbt_project.yml` and the `BRONZE`/`SILVER`/`GOLD` schema mapping.
2. Show `models/staging/finance_base/_finance_base__sources.yml`.
3. Walk through `stg_finance_base__fund_distributions.sql`.
4. Walk through `int_finance_base__distribution_yields.sql`.
5. Show `mart_fund_indicators.sql`.
6. Show tests in the folder-level YAML files.
7. Run or show `dbt docs`.
8. Query final `GOLD` models in Snowflake.

## Validation

Expected validation commands:

```bash
make deps
make parse
make run
make test
make docs-generate
```

`dbt run`, `dbt test`, and read-only validation require a configured Snowflake
account with the raw tables already loaded.
