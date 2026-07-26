# AIPS Traffic Counter

[![Main CI](https://github.com/FairozaAmira/car-counter/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/FairozaAmira/car-counter/actions/workflows/ci.yaml?query=branch%3Amain)
[![Develop CI](https://github.com/FairozaAmira/car-counter/actions/workflows/ci.yaml/badge.svg?branch=develop)](https://github.com/FairozaAmira/car-counter/actions/workflows/ci.yaml?query=branch%3Adevelop)
[![Coverage](https://codecov.io/gh/FairozaAmira/car-counter/branch/main/graph/badge.svg)](https://codecov.io/gh/FairozaAmira/car-counter)
[![CodeQL](https://github.com/FairozaAmira/car-counter/actions/workflows/codeql.yaml/badge.svg?branch=main)](https://github.com/FairozaAmira/car-counter/actions/workflows/codeql.yaml?query=branch%3Amain)
[![Python](https://img.shields.io/badge/Python-3.12.13-3776AB?logo=python&logoColor=white)](https://docs.python.org/3.12/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.139.2-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0.51-D71F00?logo=sqlalchemy&logoColor=white)](https://docs.sqlalchemy.org/en/20/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17.5-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/docs/17/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-4.0.0-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/documentation/)
[![Redis](https://img.shields.io/badge/Redis-7.4-DC382D?logo=redis&logoColor=white)](https://redis.io/docs/)
[![uv](https://img.shields.io/badge/uv-0.11.2-DE5FE9?logo=astral&logoColor=white)](https://docs.astral.sh/uv/)

An asynchronous FastAPI service, CLI, and standalone Kafka worker for analyzing
machine-generated half-hour traffic counts. Python 3.12.13 is required and all
dependencies are managed and locked with `uv`.

The analyzer returns total cars, chronological daily totals, the three busiest
half-hours (earliest timestamp wins ties), and the quietest contiguous 90-minute
period containing observations at `T`, `T+30m`, and `T+60m`.

## Contents

- [Project status](#project-status)
- [Architecture](#architecture)
  - [Code flow](#code-flow)
- [Prerequisites and installation](#prerequisites-and-installation)
- [API overview](#api-overview)
- [Database and migrations](#database-and-migrations)
  - [Local database setup](#local-database-setup)
  - [Alembic commands](#alembic-commands)
  - [Add a database column](#add-a-database-column)
  - [Downgrade and remove a newly added column](#downgrade-and-remove-a-newly-added-column)
- [Quality checks](#quality-checks)
- [CLI and Kafka](#cli-and-kafka)
- [Run locally](#run-locally)
  - [Swagger UI](#swagger-ui)
  - [Postman](#postman)
- [Docker](#docker)
- [Releases](#releases)
- [Contributing](#contributing)
- [Authors](#authors)
- [License](#license)
- [Troubleshooting](#troubleshooting)

## Project status

The API, Kafka integration, local environment, automated tests, and delivery
workflows are implemented. Gateway integration and operational deployment remain
future work.

| Phase | Status |
| --- | --- |
| Project initiation | Completed |
| API development — Kafka setup | Completed |
| API development — controllers and routers | Completed |
| API development — environment setup | Completed |
| API development — database and migrations setup | Completed |
| API development — CI/CD setup | Completed |
| API development — API Gateway setup | Planned |
| Testing and validation | Completed |
| Production deployment | Planned |
| Monitoring and maintenance | Planned |

## Architecture

- FastAPI/Uvicorn provides typed upload endpoints and OpenAPI documentation.
- Controllers keep HTTP orchestration separate from parsing and analysis services.
- SQLAlchemy repositories store every successful POST response in PostgreSQL.
- Alembic owns PostgreSQL schema upgrades and downgrades.
- Uploads are size-, filename-, extension-, MIME-, NUL-, and UTF-8-validated.
- Optional API-key authentication is independent from Redis-backed rate limiting.
- Batch work uses bounded async concurrency, preserves order, and isolates failures.
- Kafka producer and consumer services reuse connections. The consumer runs as a
  standalone worker, never once per Uvicorn worker.

```mermaid
flowchart LR
    subgraph clients["Clients and local entry points"]
        apiClient["API client"]
        analysisCli["Analysis CLI"]
        producerCli["Kafka producer CLI"]
    end

    subgraph api["FastAPI process"]
        uvicorn["Uvicorn workers"]
        requestContext["Request context middleware"]
        trafficRouter["Traffic router"]
        security["API key and rate-limit dependencies"]
        controller["Traffic controller"]
        repository["Analysis result repository"]
        trafficService["Traffic analysis service"]
    end

    subgraph core["Core processing"]
        parser["Traffic text parser"]
        analyzer["Traffic analyzer"]
    end

    subgraph workers["Standalone Kafka processes"]
        producerService["Kafka producer service"]
        consumerWorker["Kafka consumer worker"]
    end

    subgraph infrastructure["Shared infrastructure"]
        redis[("Redis rate-limit backend")]
        postgres[("PostgreSQL")]
        requestTopic[["Kafka request topic"]]
        resultTopic[["Kafka result topic"]]
    end

    subgraph outputs["Outputs"]
        apiResponse["Typed JSON response"]
        cliResponse["CLI JSON output"]
        downstream["Downstream Kafka consumer"]
    end

    apiClient -->|"Multipart HTTP"| uvicorn
    uvicorn --> requestContext
    requestContext --> trafficRouter
    trafficRouter --> security
    security -.->|"Optional shared limit"| redis
    security --> controller
    controller --> repository
    repository --> postgres
    controller --> trafficService
    trafficService --> parser
    parser --> analyzer
    analyzer --> apiResponse

    analysisCli --> parser
    analyzer --> cliResponse

    producerCli --> producerService
    producerService -.->|"Publish request"| requestTopic
    requestTopic -.->|"Consume records"| consumerWorker
    consumerWorker --> analyzer
    consumerWorker -.->|"Publish result"| resultTopic
    resultTopic -.-> downstream
```

The synchronous API and CLI paths share the same parser and analyzer. Kafka
requests are handled by a separately scalable consumer process so multiple
Uvicorn workers do not create duplicate long-running consumers. Redis is used
only when shared rate limiting is enabled. Kong is planned and is therefore not
shown as an active gateway.

```text
src/
  config/       typed environment configuration
  controllers/  HTTP use-case orchestration
  db/           SQLAlchemy models, repositories, and async sessions
  dependencies/ authentication and rate-limit dependencies
  middleware/   request IDs and access logging
  migrations/   Alembic environment and versioned schema changes
  routers/      thin FastAPI routes
  schemas/      API and Kafka contracts
  scripts/      CLI and process entry points
  services/     analysis, parsing, rate limiting, Kafka packages
  utils/        errors, secure file handling, and response formatting
  tests/        unit, integration, e2e, and test data
docs/postman/   Postman collection and local environment
```

### Code flow

```mermaid
flowchart LR
    subgraph httpInput ["HTTP input"]
        httpClient(["API client"])
        requestContext["Request context middleware"]
        security["API-key authentication and Redis rate limit"]
    end

    subgraph apiFlow ["FastAPI flow"]
        trafficRouter["Traffic router"]
        trafficController["Traffic controller"]
        routeType{"Single or batch?"}
        batchRunner["Bounded concurrent batch processing"]
    end

    subgraph analysisFlow ["Analysis flow"]
        uploadValidation["Validate filename, type, size, and UTF-8"]
        parser["Parse timestamp and car-count records"]
        analyzer["Calculate totals, busiest periods, and quietest window"]
        safeError["Map safe structured error"]
    end

    subgraph kafkaFlow ["Kafka worker flow"]
        producerCli(["Producer CLI"])
        producerService["Parse files and publish requests"]
        requestTopic[("traffic.analysis.requests")]
        consumerService["Standalone consumer worker"]
        workerAnalyzer["Analyze request records"]
        resultPublisher["Publish result before offset commit"]
        resultTopic[("traffic.analysis.results")]
    end

    subgraph outputFlow ["Outputs"]
        apiResponse(["Typed API or batch response"])
        kafkaResult(["Kafka result event"])
    end

    httpClient --> requestContext --> security --> trafficRouter --> trafficController
    trafficController --> routeType
    routeType -->|"Single"| uploadValidation
    routeType -->|"Batch"| batchRunner --> uploadValidation
    uploadValidation --> parser --> analyzer --> apiResponse
    uploadValidation -.->|"Rejected"| safeError
    parser -.->|"Invalid records"| safeError
    analyzer -.->|"Analysis failure"| safeError
    safeError --> apiResponse

    producerCli --> producerService
    producerService -.->|"Produces"| requestTopic
    requestTopic -.->|"Consumes"| consumerService
    consumerService --> workerAnalyzer --> resultPublisher
    resultPublisher -.->|"Produces"| resultTopic
    resultTopic -.-> kafkaResult
```

## Prerequisites and installation

Install `uv`, Docker Desktop (for containers), and Python 3.12.13:

```bash
uv python install 3.12.13
make install
```

`make install` runs `uv sync --locked`. Copy safe local configuration if needed:

```bash
cp .env.example .env
```

Important variables include `APP_HOST`, `APP_PORT`, `APP_WORKERS`, `APP_RELOAD`,
`KEEP_ALIVE_TIMEOUT_SECONDS`, `GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS`,
`BATCH_CONCURRENCY`, `API_KEY`, `CORS_ORIGINS`, upload validation settings,
`DATABASE_*`, `RATE_LIMIT_*`, and `KAFKA_*`. `DATABASE_URL` must use the
`postgresql+asyncpg://` SQLAlchemy scheme. Empty `API_KEY` disables authentication.
`RATE_LIMIT_ENABLED=true` requires `RATE_LIMIT_REDIS_URL`. Only list trusted reverse
proxies in `TRUSTED_PROXY_HOSTS`; forwarding headers from other peers are ignored.

## API overview

| Method | Endpoint | Purpose | Authentication |
| --- | --- | --- | --- |
| `GET` | `/health/live` | Confirm that the API process is running | None |
| `GET` | `/health/ready` | Check PostgreSQL and configured Redis readiness | None |
| `POST` | `/api/v1/traffic/analyze` | Validate and analyze one traffic file | `X-API-Key` when configured |
| `POST` | `/api/v1/traffic/analyze/batch` | Analyze multiple files concurrently in input order | `X-API-Key` when configured |

Traffic files are UTF-8 text files with one timestamp and non-negative car count
per line:

```text
2021-12-01T05:00:00 5
2021-12-01T05:30:00 12
2021-12-01T06:00:00 8
```

Successful analysis responses include the overall total, daily totals, the three
busiest half-hours, and the quietest contiguous 90-minute period. Errors use a
structured `detail` object containing a stable `code` and safe `message`. Full
interactive contracts and schemas are available through Swagger UI, ReDoc, and
OpenAPI after starting the service.

Every successful POST response is committed to PostgreSQL before it is returned
and includes:

- `id`: UUID primary identifier for the API operation
- `createdAt`: UTC creation date formatted as `DD-MM-YYYY`
- `timeTaken`: processing duration in milliseconds, rounded to two decimal places

The response `id` is the primary key of `traffic_analysis_results`. The table
also stores the operation kind, a timezone-aware UTC creation timestamp, timing,
and response-aligned camelCase columns. Nested objects are unnested into scalar
columns, while arrays containing objects use PostgreSQL JSONB. A database write
failure is rolled back and returned as HTTP 503 with the safe
`ERR00020` code.

The current persisted columns are `id`, `"analysisKind"`, `"createdAt"`,
`"timeTaken"`, `"totalCars"`, `"dailyTotals"`, `"topHalfHours"`,
`"leastCarsPeriodStart"`, `"leastCarsPeriodEnd"`,
`"leastCarsPeriodTotalCars"`, `"leastCarsPeriodRecords"`, and `items`.
PostgreSQL converts unquoted identifiers to lowercase, so every camelCase column
must be enclosed in double quotes in SQL. For example, use `"createdAt"` rather
than `createdAt` or `created_at`.

All dates returned by the POST APIs use `DD-MM-YYYY`; traffic observations that
require a time use `DD-MM-YYYY HH:MM:SS`. Uploaded machine-generated traffic files retain
their existing `YYYY-MM-DDTHH:MM:SS` input format.

## Database and migrations

PostgreSQL is the persistent store. SQLAlchemy uses an asyncpg connection pool
configured by `DATABASE_URL`, `DATABASE_POOL_SIZE`, `DATABASE_MAX_OVERFLOW`,
`DATABASE_POOL_TIMEOUT_SECONDS`, `DATABASE_POOL_RECYCLE_SECONDS`,
`DATABASE_CONNECT_TIMEOUT_SECONDS`, and `DATABASE_COMMAND_TIMEOUT_SECONDS`.
Never use a production database for local development or tests.

### Local database setup

1. Create the local environment file:

   ```bash
   cp .env.example .env
   ```

2. Confirm that `.env` contains the local async SQLAlchemy URL:

   ```dotenv
   DATABASE_URL=postgresql+asyncpg://application:application-local@localhost:5433/application
   POSTGRES_PORT=5433
   ```

   The local Compose defaults use database `application`, user `application`,
   password `application-local`, and host port `5433`. PostgreSQL continues to
   use port `5432` inside the Compose network. Override
   `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, or `POSTGRES_PORT`
   when those defaults conflict with another local PostgreSQL instance. Keep
   `DATABASE_URL` consistent with the overridden values.

3. Start PostgreSQL and wait for its health check:

   ```bash
   docker compose up --detach postgres
   docker compose ps postgres
   ```

4. Install the locked dependencies and apply all migrations:

   ```bash
   make install
   make db-upgrade
   ```

5. Confirm that the database is at the latest revision:

   ```bash
   make db-current
   ```

   The current revision should include `20260726_0002 (head)`.

6. Start the API:

   ```bash
   APP_WORKERS=1 APP_RELOAD=true make run
   ```

   In another terminal, submit one of the POST requests from the API examples
   below. Successful single and batch responses are committed to
   `traffic_analysis_results` before the API returns them.

7. Inspect recently stored results:

   ```bash
   docker compose exec -T postgres psql \
     --username application \
     --dbname application \
     --command 'SELECT id, "analysisKind", "createdAt", "timeTaken", "totalCars", items FROM traffic_analysis_results ORDER BY "createdAt" DESC LIMIT 10;'
   ```

   Copy the command exactly as shown. Do not use `ORDER BY created_at`: that
   pre-migration column no longer exists. PostgreSQL requires the current
   camelCase column to be written as `ORDER BY "createdAt"`.

   Inspect the live table definition and exact column names:

   ```bash
   docker compose exec -T postgres psql \
     --username application \
     --dbname application \
     --command '\d traffic_analysis_results'
   ```

The PostgreSQL named volume preserves local data across `docker compose down`.
Use non-production credentials and databases for development and testing.

### Alembic commands

```bash
make db-upgrade
make db-downgrade
make db-revision MESSAGE="description"
make db-current
make db-history
make migration-check
```

`make db-downgrade` defaults to one revision; set `REVISION` to choose another
target. Review every generated revision before applying it. The initial migration
creates `traffic_analysis_results` with upgrade and downgrade logic, a UUID
primary key, constraints for result kind and non-negative timing, and an index on
kind plus creation time. Revision `20260726_0002` migrates stored responses to
camelCase, response-aligned columns and unnests the `leastCarsPeriod` object.
`make migration-check` applies pending migrations and fails when SQLAlchemy
metadata contains schema changes without a migration.

### Add a database column

Use Alembic for every schema change. Do not change PostgreSQL manually.

1. Add the camelCase mapped attribute and column definition to the appropriate
   SQLAlchemy model under `src/db/models/`.
2. Generate a migration with a concise description:

   ```bash
   make db-revision MESSAGE="add resource field"
   ```

3. Review the new file under `src/migrations/versions/`. Its `upgrade()` function
   must add the column and its `downgrade()` function must remove the same column.
   Review the type, nullability, default, indexes, constraints, and any data
   backfill. Do not accept generated migration code without reviewing it.
4. Apply the migration to a disposable local database:

   ```bash
   make db-upgrade
   make db-current
   make migration-check
   ```

5. Update repositories, schemas, API responses, tests, and documentation that use
   the column, then run `make ci`.

For a required column on a table that already contains rows, first add it as
nullable or with a safe server default, backfill existing rows, and only then
make it non-nullable. Test this process outside production before deployment.

### Downgrade and remove a newly added column

To reverse the latest migration, first confirm the current revision and inspect
the migration's `downgrade()` function. The default `REVISION=-1` moves back one
revision:

```bash
make db-current
make db-downgrade
make db-current
```

To downgrade to a specific revision:

```bash
make db-history
make db-downgrade REVISION=<target-revision>
make db-current
```

The column is dropped only when the selected migration's `downgrade()` function
contains the corresponding `op.drop_column(...)`. Dropping a column permanently
from the current schema requires a new migration: remove the mapped attribute
from the SQLAlchemy model, run `make db-revision MESSAGE="drop resource field"`,
and review that the new `upgrade()` drops the column while `downgrade()` restores
it. Then apply it with `make db-upgrade`.

Downgrading or dropping a column can permanently delete its data. Use a
disposable database for validation, back up required data, never test against
production, and coordinate application rollback compatibility before executing
the change in staging or production.

When starting the complete stack with `docker compose up --build`, the one-off
`migrate` service applies Alembic migrations before the API is allowed to start.

## Quality checks

```bash
make lint
make lint-fix
make format
make format-check
make type-check
make test
make test-database
make coverage
make migration-check
make ci
```

`make test`, `make coverage`, and `make ci` use the pytest expression
`-m "not broker"`. The real-broker integration test is intentionally deselected
from the standard suite because it requires a running Kafka service. A result such
as `94 passed, 1 skipped, 1 deselected` therefore means the application suite
passed while external database and Kafka tests were not executed.

Run the PostgreSQL persistence integration test with the dedicated target:

```bash
# Correct local replacement for the stale RUN_DATABASE_TESTS command.
make test-database
```

This target starts the local Compose PostgreSQL service, recreates only the
validated `application_test` database, applies all migrations, runs
`test_postgres_persistence.py`, and drops the test database afterward. It uses
the local defaults: user `application`, password `application-local`, and host
port `5433`. Never point it at production.

To run the same workflow manually, create and migrate the disposable database
before setting `RUN_DATABASE_TESTS=1`:

```bash
docker compose up --detach --wait postgres

docker compose exec -T postgres psql \
  --username application \
  --dbname postgres \
  --command 'DROP DATABASE IF EXISTS application_test WITH (FORCE);'

docker compose exec -T postgres psql \
  --username application \
  --dbname postgres \
  --command 'CREATE DATABASE application_test OWNER application;'

DATABASE_URL=postgresql+asyncpg://application:application-local@localhost:5433/application_test \
make db-upgrade

RUN_DATABASE_TESTS=1 \
DATABASE_URL=postgresql+asyncpg://application:application-local@localhost:5433/application_test \
make test

docker compose exec -T postgres psql \
  --username application \
  --dbname postgres \
  --command 'DROP DATABASE IF EXISTS application_test WITH (FORCE);'
```

The cleanup command permanently removes only `application_test` and its data.
Confirm the `_test` suffix before running it.

If `POSTGRES_USER`, `POSTGRES_PASSWORD`, or `POSTGRES_PORT` was overridden when
the Compose volume was first created, pass those same values to the target:

```bash
make test-database \
  POSTGRES_USER=<test-user> \
  POSTGRES_PASSWORD=<test-password> \
  POSTGRES_PORT=<test-port>
```

`TEST_POSTGRES_DB` must use only letters, numbers, and underscores and must end
in `_test`. Changing Compose environment variables does not change credentials
already stored in an existing PostgreSQL volume. Do not run the CI-only
`application:application@localhost:5432` URL against the local Compose service.

All GitHub Actions quality jobs execute `make ci`, which checks and installs
locked dependencies before running the same ordered local quality gate. This
keeps local, pull-request, staging, and production-tag validation aligned.

Run the broker integration test separately:

```bash
docker compose up --detach kafka
make kafka-topics
RUN_KAFKA_TESTS=1 make test-broker
```

Coverage measures `src` application code, excludes tests and migration version
files, writes
`coverage.xml`, and enforces 100% statement and branch coverage.

## CLI and Kafka

Analyze without the API:

```bash
uv run python -m src.scripts.analyze src/tests/data/sample_traffic.txt
```

Start PostgreSQL, run migrations, and start Kafka, Redis, the API, and the
standalone consumer:

```bash
docker compose up --build
docker compose down
```

Compose runs Alembic in a one-off `migrate` service before starting the API.

Or run the worker and producer from the locked environment:

```bash
make consumer
make producer FILES="src/tests/data/sample_traffic.txt"
```

Requests use `traffic.analysis.requests`; results use `traffic.analysis.results`.
The consumer processes partitions concurrently, preserves ordering within each
partition, publishes before manually committing, and therefore provides at-least-once
delivery. Downstream systems should de-duplicate by request ID.

## Run locally

Start supporting services, apply migrations, and run the aligned quality gate:

```bash
docker compose up --detach postgres redis kafka
make db-upgrade
make ci
make kafka-topics
```

Development:

```bash
APP_WORKERS=1 APP_RELOAD=true make run
```

The underlying command is:

```bash
uv run python -m src.scripts.serve
```

Production-style local run:

```bash
APP_HOST=0.0.0.0 \
APP_PORT=8000 \
APP_WORKERS=4 \
APP_RELOAD=false \
KEEP_ALIVE_TIMEOUT_SECONDS=5 \
GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS=30 \
uv run python -m src.scripts.serve
```

API URLs:

- Base URL: `http://localhost:8000`
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- OpenAPI JSON: `http://localhost:8000/openapi.json`

Health checks:

```bash
curl --request GET \
  --url http://localhost:8000/health/live \
  --header "Accept: application/json"

curl --request GET \
  --url http://localhost:8000/health/ready \
  --header "Accept: application/json"
```

Analyze one file:

```bash
curl --request POST \
  --url http://localhost:8000/api/v1/traffic/analyze \
  --header "Accept: application/json" \
  --header "X-API-Key: <api-key>" \
  --form "file=@src/tests/data/sample_traffic.txt;type=text/plain"
```

Analyze files concurrently:

```bash
curl --request POST \
  --url http://localhost:8000/api/v1/traffic/analyze/batch \
  --header "Accept: application/json" \
  --header "X-API-Key: <api-key>" \
  --form "files=@src/tests/data/sample_traffic.txt;type=text/plain" \
  --form "files=@src/tests/data/sample_traffic.txt;type=text/plain"
```
Successful single-file response:

```json
{
  "totalCars": 398,
  "dailyTotals": [
    {
      "date": "01-12-2021",
      "carCount": 179
    },
    {
      "date": "05-12-2021",
      "carCount": 81
    },
    {
      "date": "08-12-2021",
      "carCount": 134
    },
    {
      "date": "09-12-2021",
      "carCount": 4
    }
  ],
  "topHalfHours": [
    {
      "timestamp": "01-12-2021 07:30:00",
      "carCount": 46
    },
    {
      "timestamp": "01-12-2021 08:00:00",
      "carCount": 42
    },
    {
      "timestamp": "08-12-2021 18:00:00",
      "carCount": 33
    }
  ],
  "leastCarsPeriod": {
    "start": "01-12-2021 05:00:00",
    "end": "01-12-2021 06:30:00",
    "totalCars": 31,
    "records": [
      {
        "timestamp": "01-12-2021 05:00:00",
        "carCount": 5
      },
      {
        "timestamp": "01-12-2021 05:30:00",
        "carCount": 12
      },
      {
        "timestamp": "01-12-2021 06:00:00",
        "carCount": 14
      }
    ]
  },
  "id": "6c023e9c-f9d6-4348-8299-48667795ade4",
  "createdAt": "25-07-2026",
  "timeTaken": 4.32
}
```

The batch endpoint returns the same `id`, `createdAt`, and `timeTaken` metadata
with an ordered `items` array containing an independent result or error for each
uploaded file.

Success is HTTP 200. Common responses are 401 (invalid key), 413 (too large),
415 (unsupported file), 422 (invalid traffic data), and 429 (limit exceeded).
HTTP 429 includes `Retry-After`. Batch item failures remain inside a 200 response.

All application failures use a stable code with an explicit HTTP status:

| Code | HTTP status | Meaning |
| --- | ---: | --- |
| `ERR00010` | 500 | Kafka producer initialization failure |
| `ERR00011` | 500 | Kafka consumer initialization failure |
| `ERR00012` | 500 | Kafka consumer action failure |
| `ERR00013` | 500 | Kafka publish failure |
| `ERR00020` | 503 | Database persistence failure |
| `ERR00021` | 401 | Authentication failure |
| `ERR00022` | 429 | Rate limit exceeded |
| `ERR00030` | 400 | Invalid or missing JSON request body |
| `ERR00031` | 422 | Invalid request body |
| `ERR00032` | 422 | Invalid traffic record |
| `ERR00033` | 422 | Invalid timestamp |
| `ERR00034` | 422 | Invalid car count |
| `ERR00035` | 422 | Duplicate timestamp |
| `ERR00036` | 422 | Empty input |
| `ERR00037` | 422 | Missing contiguous traffic period |
| `ERR00040` | 400 | Invalid filename |
| `ERR00041` | 415 | Unsupported file extension |
| `ERR00042` | 415 | Unsupported media type |
| `ERR00043` | 413 | File too large |
| `ERR00044` | 415 | Invalid file content |
| `ERR00045` | 415 | Invalid file encoding |
| `ERR00046` | 500 | File read failure |

Check the database after either cURL request. Copy the `id` from the API response
and replace `<response-id>`:

```bash
docker compose exec -T postgres psql \
  --username application \
  --dbname application \
  --command 'SELECT id, "analysisKind", "createdAt", "timeTaken", "totalCars", "dailyTotals", "topHalfHours", "leastCarsPeriodStart", "leastCarsPeriodEnd", "leastCarsPeriodTotalCars", "leastCarsPeriodRecords", items FROM traffic_analysis_results WHERE id = $$<response-id>$$;'
```

The query should return exactly one row. If the local PostgreSQL database or user
was overridden, replace the `--dbname` and `--username` values accordingly.

Check the five most recently stored results:

```bash
docker compose exec -T postgres psql \
  --username application \
  --dbname application \
  --command 'SELECT id, "analysisKind", "createdAt", "timeTaken", "totalCars", items FROM traffic_analysis_results ORDER BY "createdAt" DESC LIMIT 5;'
```

Example scalar result columns (JSONB list columns are omitted for readability):

```text
                  id                  | analysisKind |          createdAt           | timeTaken | totalCars
--------------------------------------+--------------+------------------------------+-----------+----------
 7d963c7d-71a8-40a7-a433-57aac4676f25 | batch        | 2026-07-26 06:15:21.42031+00 |      3.87 | null
 9d3f51e8-7397-4a11-b9bb-97f44d0d821e | single       | 2026-07-26 06:14:02.91854+00 |      4.32 |       398
(2 rows)
```

PostgreSQL stores `createdAt` as timezone-aware UTC. The API formats the
user-facing `createdAt` value as `DD-MM-YYYY`.



### Swagger UI

Start the API, open `/docs`, select an endpoint, click **Try it out**, provide the
file and `X-API-Key` when configured, click **Execute**, then review the generated
request, response status, headers, and body.

### Postman

Import [collection.json](docs/postman/collection.json) and
[local-environment.json](docs/postman/local-environment.json). The environment uses:

```text
base_url=http://localhost:8000
api_key=<api-key>
```

Select the local environment, attach `src/tests/data/sample_traffic.txt` to upload
requests, and leave `api_key` empty only when authentication is disabled.

## Docker

```bash
make docker-build
make docker-verify
make docker-run
```

`make docker-build` pulls the latest base images, disables the layer cache, and
builds `aips-car-counter:local` for `linux/amd64`. On Apple Silicon, build and run
an ARM image explicitly:

```bash
make docker-build DOCKER_PLATFORM=linux/arm64
make docker-verify DOCKER_PLATFORM=linux/arm64
make docker-run DOCKER_PLATFORM=linux/arm64
```

Override `IMAGE_TAG` for immutable releases, for example
`make docker-build IMAGE_TAG=v1.4.0`. `make docker-run` uses `.env`, publishes
port 8000, and runs the named `aips-car-counter` container. It replaces the
host-side `localhost` database address with `host.docker.internal`, because
`localhost` inside the API container refers to that container rather than the
Mac. The target also configures Docker's host-gateway mapping for Linux.

Start PostgreSQL and apply migrations before running the standalone API image:

```bash
docker compose up --detach --wait postgres
make db-upgrade
make docker-run DOCKER_PLATFORM=linux/arm64
```

From another terminal, `make docker-stop` stops the API container. Override
`ENV_FILE`, `APP_PORT`, `IMAGE_NAME`, `IMAGE_TAG`, `CONTAINER_NAME`,
`DOCKER_DATABASE_HOST`, `POSTGRES_PORT`, or `DOCKER_DATABASE_URL` when needed.

The multi-stage image installs locked production dependencies, excludes tests and
environment files, uses a non-root user, and starts Uvicorn through the typed
environment-driven entry point.

For local Kafka, start Compose and explicitly ensure both application topics exist:

```bash
docker compose up --detach kafka
make kafka-topics
```

The target safely uses Kafka's `--if-not-exists` option for
`traffic.analysis.requests` and `traffic.analysis.results`. Compose also enables
automatic topic creation for local development.

No Kubernetes manifests are maintained in this repository. If this image is
deployed to Kubernetes, use a new immutable `IMAGE_TAG` for every release. If a
mutable tag is unavoidable, set `imagePullPolicy: Always` and trigger a rollout
restart so nodes do not retain an older cached image.

## Releases

GitHub Actions validates pull requests and pushes to `develop`. Immutable staging
images are published from tags such as `staging-v1.4.0` using the protected
`staging` environment. Production images are published from tags such as `v1.4.0`
using the protected `production` environment, where approval should be configured.
The workflows publish only the exact tag and do not publish `latest`.

## Contributing

This is proprietary software, not an open-source project. Contributions are
accepted only from authorized collaborators and remain subject to
[the proprietary license](license.md). Contact the repository owner before
starting substantial work; acceptance is discretionary and may require a separate
written contributor agreement.

For an authorized contribution:

1. Create a focused branch from `develop`.
2. Install the locked development environment with `make install`.
3. Follow the existing architecture, typing, documentation, security, and testing
   conventions.
4. Add or update tests for every behavior change.
5. Run `make ci` and relevant Docker or broker tests before opening a pull request.
6. Open the pull request against `develop` with a concise summary, validation
   evidence, operational impact, and any remaining risks.

Do not submit secrets, production data, generated caches, unrelated refactors, or
third-party material that you are not authorized to contribute.

## Authors

- [FairozaAmira](https://github.com/FairozaAmira) — creator and maintainer

## License

Copyright © 2026 FairozaAmira. All rights reserved. This project is proprietary
software and is not licensed for public use, modification, or distribution.
See [license.md](license.md) for the complete terms.

## Troubleshooting

- If `uv` cannot write its cache, use
  `UV_CACHE_DIR=/private/tmp/car-counter-uv-cache`.
- If Docker commands cannot connect, start Docker Desktop before rebuilding.
- If readiness returns 503, check Redis and `RATE_LIMIT_REDIS_URL`.
- If readiness returns 503 with PostgreSQL configured, check `DATABASE_URL`,
  database health, and `make db-current`.
- If PostgreSQL reports `InvalidPasswordError`, verify that `DATABASE_URL` uses
  the credentials and published host port of the running database. The local
  Compose defaults are `application:application-local@localhost:5433`; the
  `application:application@localhost:5432` URL is reserved for CI services.
- If Alembic reports unapplied changes, run `make db-upgrade`; create a new
  revision for schema changes rather than editing an applied migration.
- If startup rejects settings, ensure reload uses one worker, CORS has no wildcard,
  PostgreSQL uses the asyncpg URL scheme, and Redis is configured whenever rate
  limiting is enabled.
