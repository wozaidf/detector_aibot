FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --disable-pip-version-check --retries 10 --timeout 120 -r requirements.txt

COPY config.py main.py iforest_tool.py ./
COPY inference/ ./inference/
COPY kafka_stream/ ./kafka_stream/
COPY models/ ./models/

CMD ["python", "main.py"]
