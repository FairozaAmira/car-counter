.PHONY: install lock run analyze producer consumer test test-broker lint format typecheck check docker-up docker-down

install:
	uv sync --locked

lock:
	uv lock

run:
	uv run uvicorn src.main:app --host 0.0.0.0 --port 8000 --workers 4 --timeout-graceful-shutdown 120 --timeout-worker-healthcheck 120

analyze:
	uv run python -m src.scripts.analyze $(FILE)

producer:
	uv run python -m src.scripts.kafka_producer $(FILES)

consumer:
	uv run python -m src.scripts.kafka_consumer

test:
	uv run pytest -m "not broker"

test-broker:
	uv run pytest -m broker

lint:
	uv run ruff check .

format:
	uv run ruff format .

typecheck:
	uv run mypy src

check: lint typecheck test

docker-up:
	docker compose up --build

docker-down:
	docker compose down
