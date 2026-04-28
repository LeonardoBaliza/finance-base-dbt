# Tradeoffs and Decisions

This document explains the main implementation choices behind the Finance Base
dbt mini project, including the tradeoffs that were accepted to keep the
project small, readable, and interview-friendly.

## Architecture

The project uses a medallion layout:

- `models/staging` maps raw Snowflake `PAYLOAD VARIANT` rows into typed,
  source-conformed views in `BRONZE`.
- `models/intermediate` keeps reusable business logic and grain changes in
  `SILVER`.
- `models/marts` publishes BI-facing tables in `GOLD`.

The main benefit is clear lineage: raw data is only referenced with `source()`,
and every downstream dependency uses `ref()`. That makes dbt docs, tests, and
focused builds useful during a walkthrough.

The tradeoff is extra model count for a small project. Some transformations
could be collapsed into marts, but separating layers makes assumptions easier
to review and reduces duplicated business logic.

## Materialization Strategy

Staging and intermediate models are views, while marts are tables.

Views keep development fast and avoid persisting transient parsing or helper
logic. Tables in the mart layer give BI consumers stable objects and avoid
recomputing common dashboard outputs.

The tradeoff is that complex intermediate views may be recalculated when marts
build. For this mini project, readability and low storage overhead were
prioritized over maximum runtime performance. If source volume grows, the first
candidates for table or incremental materialization would be distribution yield
logic and monthly or quarterly history models.

## Raw Payload Parsing

Staging models parse multiple English and Portuguese field names from the raw
`PAYLOAD VARIANT` column. This makes the project tolerant of representative
extracts from different source shapes.

The tradeoff is that staging SQL contains more `coalesce()` and type-casting
logic than a strict source contract would require. The choice keeps ingestion
flexible without spreading schema variation into downstream models.

## Business Keys

CNPJ is used as the stable fund identifier where available, while ticker is
treated as a market-facing attribute that can change over time. Current fund
records are selected by latest timestamps per CNPJ, and ticker history is used
when resolving distributions.

The tradeoff is extra joins and ranking logic, but it avoids treating ticker as
an immutable fund key. That matters for fund renames and ticker changes.

## Distribution and Split Logic

Distributions are deduplicated with a deterministic hash key and then adjusted
for splits that occur after the distribution base date. Yield calculations use
the latest available quote at or before the relevant base date.

The tradeoff is conservative metric calculation. The project avoids producing
yield values when prices are missing or invalid, and it caps distribution yield
tests to catch implausible results. This may leave some null metrics, but those
nulls are preferable to silently publishing misleading indicators.

## Mart Shape

The mart layer publishes separate dimensions, facts, current indicators, and
historical monthly or quarterly outputs instead of one wide table.

This supports common BI use cases:

- `dim_funds` for fund attributes.
- `fct_daily_quotes` for market history.
- `fct_distributions` for adjusted income events.
- `mart_fund_indicators` for current dashboard metrics.
- `mart_fund_monthly_history` and `mart_fund_quarterly_history` for trends.

The tradeoff is that consumers may need joins for ad hoc analysis. For the
intended dashboard and interview scope, the separation keeps each model's grain
clear and testable.

## Validation

The project leans on dbt tests for structural and business-rule validation:
not-null keys, uniqueness, ticker and CNPJ formats, non-negative financial
values, price consistency, and bounded yield ratios.

The tradeoff is that tests validate modeled data after it lands in Snowflake;
they do not replace upstream ingestion checks. This is intentional for a dbt
mini project: dbt owns transformation quality, while source loading remains an
external responsibility.

## What Was Left Out

Several production features were intentionally left out to keep the project
focused:

- incremental models and orchestration;
- snapshots for slowly changing attributes;
- CI/CD and deployment automation;
- full source freshness checks;
- row-level security and production grant management.

Those are reasonable next steps for a production deployment, but they would add
operational complexity that is outside the core modeling exercise.
