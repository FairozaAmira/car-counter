# AIPS Traffic Counter

A Python 3.12 solution to the AIPS coding challenge. It analyzes machine-generated
half-hour traffic counts through a synchronous CLI, async FastAPI endpoints, or an
async Kafka producer/consumer pipeline. The implementation uses the standard library
for all calculations and deliberately avoids Pandas and Polars.

## What it calculates

- Total cars across the input.
- Per-day totals in chronological order.
- The three busiest half-hours, with earlier timestamps winning ties.
- The quietest contiguous 1.5-hour period. A valid period contains records at
  `T`, `T+30m`, and `T+60m` and ends at `T+90m`.

Input is UTF-8 text with one record per line:

```text
2021-12-01T05:00:00 5
2021-12-01T05:30:00 12
2021-12-01T06:00:00 14
```

Records are sorted before analysis. Empty input, negative counts, malformed records,
duplicate timestamps, and data without a contiguous period are rejected.

## Setup and local use

Install the locked environment:

```bash
uv sync --locked
```

Analyze one file from the command line:

```bash
uv run python -m src.scripts.analyze src/tests/data/sample_traffic.txt
```

Start the API with four Uvicorn workers:

```bash
make run
```

OpenAPI documentation is available at <http://localhost:8000/docs>.

Analyze one file:

```bash
curl -F "file=@src/tests/data/sample_traffic.txt" \
  http://localhost:8000/api/v1/traffic/analyze
```

Analyze multiple files concurrently:

```bash
curl \
  -F "files=@src/tests/data/sample_traffic.txt" \
  -F "files=@src/tests/data/sample_traffic.txt" \
  http://localhost:8000/api/v1/traffic/analyze/batch
```

Batch results preserve input order. Each item has an independent `completed` or
`failed` status so one malformed file does not discard successful siblings.

## Kafka workflow

Start the API, Kafka broker, and standalone consumer:

```bash
docker compose up --build
```

With a local broker running, publish one or more files:

```bash
uv run python -m src.scripts.kafka_producer \
  src/tests/data/sample_traffic.txt \
  src/tests/data/sample_traffic.txt
```

Requests are published to `traffic.analysis.requests`; results and stable failure
events are published to `traffic.analysis.results`. Both use versioned JSON schemas
and the UUID request ID as the Kafka key.

The consumer uses the `traffic-analysis-workers` group. It processes partitions
concurrently, keeps records within a partition sequential, publishes a result before
manually committing the request offset, and therefore provides at-least-once
delivery. Downstream consumers should de-duplicate by request ID.

Kafka is intentionally independent of FastAPI lifecycle. Starting four web workers
does not create four hidden Kafka consumers, and HTTP/CLI analysis works without a
broker.

## Development

```bash
make lint
make typecheck
make test
make check
```

Broker-backed tests are opt-in:

```bash
make test-broker
```

Configuration is loaded from environment variables or `.env`. The most commonly
changed values are `BATCH_CONCURRENCY`, `KAFKA_BOOTSTRAP_SERVERS`,
`KAFKA_REQUEST_TOPIC`, `KAFKA_RESULT_TOPIC`, and `KAFKA_CONSUMER_GROUP`.

## Project structure

```text
src/
  config/       environment configuration
  controllers/  HTTP orchestration
  routers/      FastAPI routes
  schemas/      API and Kafka contracts
  scripts/      thin CLI entry points
  services/     parsing, analysis, Kafka producer, Kafka consumer
  tests/
    data/
    e2e/
    integration/
    units/
    utils/
  main.py       FastAPI application
```
