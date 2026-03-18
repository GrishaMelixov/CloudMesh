FROM python:3.11-slim

WORKDIR /app

# Build context is repo root: docker build -f services/log-aggregator/Dockerfile .
COPY services/log-aggregator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/log-aggregator/ .

CMD ["python", "main.py"]
