# AIPS Traffic Counter

An asynchronous FastAPI service, CLI, and standalone Kafka worker for analyzing
machine-generated half-hour traffic counts. Python 3.12 is required and all
dependencies are managed and locked with `uv`.

The analyzer returns total cars, chronological daily totals, the three busiest
half-hours (earliest timestamp wins ties), and the quietest contiguous 90-minute
period containing observations at `T`, `T+30m`, and `T+60m`.

## Architecture

- FastAPI/Uvicorn provides typed upload endpoints and OpenAPI documentation.
- Controllers keep HTTP orchestration separate from parsing and analysis services.
- Uploads are size-, filename-, extension-, MIME-, NUL-, and UTF-8-validated.
- Optional API-key authentication is independent from Redis-backed rate limiting.
- Batch work uses bounded async concurrency, preserves order, and isolates failures.
- Kafka producer and consumer services reuse connections. The consumer runs as a
  standalone worker, never once per Uvicorn worker.

```text
src/
  config/       typed environment configuration
  controllers/  HTTP use-case orchestration
  dependencies/ authentication and rate-limit dependencies
  middleware/   request IDs and access logging
  routers/      thin FastAPI routes
  schemas/      API and Kafka contracts
  scripts/      CLI and process entry points
  services/     analysis, parsing, rate limiting, Kafka packages
  tests/        unit, integration, e2e, and test data
docs/postman/   Postman collection and local environment
```

## Prerequisites and installation

Install `uv`, Docker Desktop (for containers), and Python 3.12:

```bash
uv python install 3.12
make install
```

`make install` runs `uv sync --locked`. Copy safe local configuration if needed:

```bash
cp .env.example .env
```

Important variables include `APP_HOST`, `APP_PORT`, `APP_WORKERS`, `APP_RELOAD`,
`KEEP_ALIVE_TIMEOUT_SECONDS`, `GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS`,
`BATCH_CONCURRENCY`, `API_KEY`, `CORS_ORIGINS`, upload validation settings,
`RATE_LIMIT_*`, and `KAFKA_*`. Empty `API_KEY` disables authentication.
`RATE_LIMIT_ENABLED=true` requires `RATE_LIMIT_REDIS_URL`. Only list trusted reverse
proxies in `TRUSTED_PROXY_HOSTS`; forwarding headers from other peers are ignored.

## Run locally

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

Success is HTTP 200. Common responses are 401 (invalid key), 413 (too large),
415 (unsupported file), 422 (invalid traffic data), and 429 (limit exceeded).
HTTP 429 includes `Retry-After`. Batch item failures remain inside a 200 response.

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

## CLI and Kafka

Analyze without the API:

```bash
uv run python -m src.scripts.analyze src/tests/data/sample_traffic.txt
```

Start Kafka, Redis, the API, and the standalone consumer:

```bash
docker compose up --build
docker compose down
```

Or run the worker and producer from the locked environment:

```bash
make consumer
make producer FILES="src/tests/data/sample_traffic.txt"
```

Requests use `traffic.analysis.requests`; results use `traffic.analysis.results`.
The consumer processes partitions concurrently, preserves ordering within each
partition, publishes before manually committing, and therefore provides at-least-once
delivery. Downstream systems should de-duplicate by request ID.

## Docker

```bash
make docker-build
docker run --rm \
  --env-file .env \
  --publish 8000:8000 \
  aips-car-counter:local
```

The multi-stage image installs locked production dependencies, excludes tests and
environment files, uses a non-root user, and starts Uvicorn through the typed
environment-driven entry point.

## Quality checks

```bash
make lint
make lint-fix
make format
make format-check
make type-check
make test
make coverage
make ci
```

`make test` excludes broker-marked tests. Run `make test-broker` with Kafka available.
Coverage measures `src` application code, excludes `src/tests`, writes
`coverage.xml`, and enforces 100% statement and branch coverage.

## Releases

GitHub Actions validates pull requests and pushes to `develop`. Immutable staging
images are published from tags such as `staging-v1.4.0` using the protected
`staging` environment. Production images are published from tags such as `v1.4.0`
using the protected `production` environment, where approval should be configured.
The workflows publish only the exact tag and do not publish `latest`.

## Troubleshooting

- If `uv` cannot write its cache, use
  `UV_CACHE_DIR=/private/tmp/car-counter-uv-cache`.
- If Docker commands cannot connect, start Docker Desktop before rebuilding.
- If readiness returns 503, check Redis and `RATE_LIMIT_REDIS_URL`.
- If startup rejects settings, ensure reload uses one worker, CORS has no wildcard,
  and Redis is configured whenever rate limiting is enabled.
