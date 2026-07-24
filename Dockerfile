FROM ghcr.io/astral-sh/uv:0.11.2 AS uv

FROM python:3.12-slim AS builder
COPY --from=uv /uv /uvx /bin/
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev

FROM python:3.12-slim AS runtime
WORKDIR /app/src
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app" \
    PYTHONUNBUFFERED=1
COPY --from=builder /app/.venv /app/.venv
COPY src /app/src
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4", "--timeout-graceful-shutdown", "120", "--timeout-worker-healthcheck", "120"]
