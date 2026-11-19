FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# Install the locked project dependencies before copying the application source.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    pip install --no-cache-dir uv \
    && uv sync --frozen --no-dev --no-install-project

COPY main.py config.py threshold.py ./
COPY jev/ ./jev/
COPY laya_predict/ ./laya_predict/
COPY kafka_stream/ ./kafka_stream/

ENV PATH="/app/.venv/bin:${PATH}"

CMD ["python", "main.py"]
