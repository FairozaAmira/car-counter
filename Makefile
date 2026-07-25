IMAGE_NAME ?= aips-car-counter
IMAGE_TAG ?= local
IMAGE := $(IMAGE_NAME):$(IMAGE_TAG)
CONTAINER_NAME ?= aips-car-counter
DOCKER_PLATFORM ?= linux/amd64
DOCKER_DATABASE_HOST ?= host.docker.internal
ENV_FILE ?= .env
APP_PORT ?= 8000
POSTGRES_DB ?= application
POSTGRES_USER ?= application
POSTGRES_PASSWORD ?= application-local
POSTGRES_PORT ?= 5433
DOCKER_DATABASE_URL ?= postgresql+asyncpg://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(DOCKER_DATABASE_HOST):$(POSTGRES_PORT)/$(POSTGRES_DB)
KAFKA_BOOTSTRAP_SERVER ?= localhost:29092
KAFKA_REQUEST_TOPIC ?= traffic.analysis.requests
KAFKA_RESULT_TOPIC ?= traffic.analysis.results
ALEMBIC_CONFIG ?= src/migrations/alembic.ini
REVISION ?= -1

.PHONY: install lock lock-check ci run analyze producer consumer test coverage test-coverage test-broker lint lint-fix format format-check type-check typecheck check db-upgrade db-downgrade db-revision db-current db-history migration-check docker-build docker-verify docker-run docker-stop docker-up docker-down kafka-topics

install:
	uv sync --locked

lock:
	uv lock

lock-check:
	uv lock --check

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

db-upgrade:
	uv run --locked alembic --config $(ALEMBIC_CONFIG) upgrade head

db-downgrade:
	uv run --locked alembic --config $(ALEMBIC_CONFIG) downgrade $(REVISION)

db-revision:
	uv run --locked alembic --config $(ALEMBIC_CONFIG) revision --autogenerate --message "$(MESSAGE)"

db-current:
	uv run --locked alembic --config $(ALEMBIC_CONFIG) current

db-history:
	uv run --locked alembic --config $(ALEMBIC_CONFIG) history

migration-check: db-upgrade
	uv run --locked alembic --config $(ALEMBIC_CONFIG) check

ci: lock-check install lint format-check type-check test coverage migration-check

check: ci

docker-build:
	docker build --no-cache --pull --platform $(DOCKER_PLATFORM) --tag $(IMAGE) .

docker-verify:
	docker run --rm --platform $(DOCKER_PLATFORM) --entrypoint sh $(IMAGE) -c 'test -x /app/.venv/bin/python && test -f /app/src/main.py && python -c "from src.main import app; assert app.openapi()[\"info\"][\"title\"]"'

docker-run:
	docker run \
		--name $(CONTAINER_NAME) \
		--rm \
		--platform $(DOCKER_PLATFORM) \
		--add-host $(DOCKER_DATABASE_HOST):host-gateway \
		--env-file $(ENV_FILE) \
		--env APP_PORT=$(APP_PORT) \
		--env DATABASE_URL=$(DOCKER_DATABASE_URL) \
		--publish $(APP_PORT):$(APP_PORT) \
		$(IMAGE)

docker-stop:
	docker stop $(CONTAINER_NAME)

docker-up:
	docker compose up --build

docker-down:
	docker compose down

kafka-topics:
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic $(KAFKA_REQUEST_TOPIC) --bootstrap-server $(KAFKA_BOOTSTRAP_SERVER) --partitions 1 --replication-factor 1
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic $(KAFKA_RESULT_TOPIC) --bootstrap-server $(KAFKA_BOOTSTRAP_SERVER) --partitions 1 --replication-factor 1
