.PHONY: install lock ci run analyze producer consumer test coverage test-coverage test-broker lint lint-fix format format-check type-check typecheck check docker-build docker-up docker-down

install:
	uv sync --locked

lock:
	uv lock

run:
	uv run python -m src.scripts.serve

analyze:
	uv run python -m src.scripts.analyze $(FILE)

producer:
	uv run python -m src.scripts.kafka_producer $(FILES)

consumer:
	uv run python -m src.scripts.kafka_consumer

test:
	uv run pytest src/tests -m "not broker"

coverage:
	uv run pytest src/tests -m "not broker" --cov=src --cov-report=term-missing --cov-report=xml

test-coverage: coverage

test-broker:
	uv run pytest src/tests -m broker

lint:
	uv run ruff check .

lint-fix:
	uv run ruff check --fix .

format:
	uv run ruff format .

format-check:
	uv run ruff format --check .

typecheck:
	uv run mypy src

type-check: typecheck

ci: lint format-check type-check test coverage

check: ci

docker-build:
	docker build --tag aips-car-counter:local .

docker-up:
	docker compose up --build

docker-down:
	docker compose down
