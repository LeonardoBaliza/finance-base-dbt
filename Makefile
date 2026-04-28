ENV_FILE ?= .env.local
PROFILES_DIR ?= .
DBT := uv run dbt
WITH_ENV = test -f "$(ENV_FILE)" && set -a && . "$(ENV_FILE)" && set +a &&

.PHONY: help deps debug parse compile run test build docs-generate docs-serve ls clean

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*## "; printf "Available commands:\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

deps: ## Install dbt packages
	$(DBT) deps --profiles-dir "$(PROFILES_DIR)"

debug: ## Validate dbt profile and Snowflake connection
	$(WITH_ENV) $(DBT) debug --profiles-dir "$(PROFILES_DIR)"

parse: ## Parse project files without running models
	$(WITH_ENV) $(DBT) parse --profiles-dir "$(PROFILES_DIR)" --no-partial-parse

compile: ## Compile SQL and Jinja
	$(WITH_ENV) $(DBT) compile --profiles-dir "$(PROFILES_DIR)"

run: ## Run all dbt models
	$(WITH_ENV) $(DBT) run --profiles-dir "$(PROFILES_DIR)"

test: ## Run dbt data tests
	$(WITH_ENV) $(DBT) test --profiles-dir "$(PROFILES_DIR)"

build: ## Run models and tests
	$(WITH_ENV) $(DBT) build --profiles-dir "$(PROFILES_DIR)"

docs-generate: ## Generate dbt documentation artifacts
	$(WITH_ENV) $(DBT) docs generate --profiles-dir "$(PROFILES_DIR)"

docs-serve: ## Serve dbt documentation locally
	$(WITH_ENV) $(DBT) docs serve --profiles-dir "$(PROFILES_DIR)"

ls: ## List dbt models
	$(WITH_ENV) $(DBT) ls --profiles-dir "$(PROFILES_DIR)" --resource-type model

clean: ## Remove dbt generated artifacts
	$(DBT) clean --profiles-dir "$(PROFILES_DIR)"
