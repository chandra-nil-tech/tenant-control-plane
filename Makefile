# Command runner. Every target only needs Docker (+ curl/sh for the demo scripts).
# `make help` lists everything.

COMPOSE      := docker compose
TEST_RUN     := $(COMPOSE) --profile test run --rm --build test
TEST_DEPS    := postgres-test rabbitmq-test

.DEFAULT_GOAL := help
.PHONY: help up down restart clean logs test check race demo queues \
        worker-fail worker-chaos worker-slow worker-reset fmt

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- lifecycle
up: ## Build and boot everything (db, broker, migrations, api, worker)
	$(COMPOSE) up --build -d
	@./scripts/wait-ready.sh
	@echo "API: http://localhost:8000/docs   RabbitMQ UI: http://localhost:15672 (guest/guest)"

down: ## Stop the stack (data is kept)
	$(COMPOSE) down

restart: ## Restart the stack; tenants persist
	$(COMPOSE) restart
	@./scripts/wait-ready.sh

clean: ## Stop and delete all data volumes
	$(COMPOSE) --profile test down -v --remove-orphans

logs: ## Follow api + worker logs
	$(COMPOSE) logs -f api worker

# ---------------------------------------------------------------- quality
test: ## Run the full test suite (in Docker, against its own Postgres + RabbitMQ)
	$(TEST_RUN) pytest --cov --cov-report=term; status=$$?; \
	$(COMPOSE) --profile test stop $(TEST_DEPS) >/dev/null 2>&1; exit $$status

check: ## All quality gates: format, lint, mypy, bandit, tests+coverage
	$(TEST_RUN) sh scripts/check.sh; status=$$?; \
	$(COMPOSE) --profile test stop $(TEST_DEPS) >/dev/null 2>&1; exit $$status

race: ## Fire concurrent creates/PATCHes/DELETEs at the running API
	$(COMPOSE) --profile test run --rm --no-deps --build test python scripts/race_test.py --base-url http://api:8000

# ---------------------------------------------------------------- scenarios
demo: ## Create a tenant and watch it reach `active`
	@./scripts/demo.sh

worker-fail: ## Restart the worker so every task fails
	WORKER_FAIL_RATE=1 $(COMPOSE) up -d --no-deps --force-recreate worker

worker-chaos: ## Restart the worker sending duplicates, terminal-before-in_progress
	WORKER_DUPLICATE_RATE=1 WORKER_OUT_OF_ORDER=true $(COMPOSE) up -d --no-deps --force-recreate worker

worker-slow: ## Restart the worker with 5-10s delays per step
	WORKER_MIN_DELAY_MS=5000 WORKER_MAX_DELAY_MS=10000 $(COMPOSE) up -d --no-deps --force-recreate worker

worker-reset: ## Restart the worker with default settings
	$(COMPOSE) up -d --no-deps --force-recreate worker

queues: ## Show queue depths, including the dead-letter queues
	$(COMPOSE) exec rabbitmq rabbitmqctl list_queues name type messages

# ---------------------------------------------------------------- local dev (needs uv)
fmt: ## Format and auto-fix locally
	uv run ruff format src tests migrations scripts && uv run ruff check --fix src tests migrations scripts
