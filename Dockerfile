FROM ghcr.io/astral-sh/uv:0.11.2 AS uv

FROM python:3.12.13-slim AS builder
COPY --from=uv /uv /uvx /bin/
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev

FROM python:3.12.13-slim AS runtime
RUN groupadd --system app && useradd --system --gid app --home /app app
WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app" \
    PYTHONUNBUFFERED=1
COPY --from=builder /app/.venv /app/.venv
COPY src /app/src
RUN chown -R app:app /app
USER app
EXPOSE 8000
CMD ["python", "-m", "src.scripts.serve"]
