FROM python:3.12-slim
WORKDIR /app
COPY l1_stream_simulator/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY l1_stream_simulator ./l1_stream_simulator
CMD ["python", "-m", "l1_stream_simulator.service"]
