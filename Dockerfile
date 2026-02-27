# syntax=docker/dockerfile:1
FROM python:3.12-slim AS builder
WORKDIR /app
COPY pyproject.toml README.md /app/
RUN pip install --upgrade pip && pip install build
COPY src /app/src
RUN python -m build --wheel

FROM python:3.12-slim AS runtime
RUN useradd -m appuser
WORKDIR /app
COPY --from=builder /app/dist/*.whl /app/
RUN pip install --no-cache-dir /app/*.whl && rm /app/*.whl
USER appuser
ENV XDG_CONFIG_HOME=/config
VOLUME ["/config", "/data", "/output"]
ENTRYPOINT ["bsky_noise"]
