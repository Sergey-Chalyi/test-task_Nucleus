.PHONY: help up down logs ps test lint fmt install load clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

install:  ## Create a virtualenv and install dev dependencies
	python3 -m venv .venv
	.venv/bin/pip install -r requirements-dev.txt

up:  ## Start postgres, redis, api and worker
	docker compose up --build -d

down:  ## Stop everything and drop the volumes
	docker compose down -v

logs:  ## Follow the api and worker logs
	docker compose logs -f api worker

ps:  ## Show container status
	docker compose ps

test:  ## Run the test suite (no Docker required)
	.venv/bin/python -m pytest

lint:  ## Lint with ruff
	.venv/bin/ruff check app tests scripts

fmt:  ## Format with ruff
	.venv/bin/ruff format app tests scripts

load:  ## Fire synthetic traffic at a running API
	.venv/bin/python scripts/load_test.py

clean:  ## Remove caches
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache
