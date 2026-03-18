FROM python:3.11-slim

WORKDIR /app

# Build context is repo root: docker build -f services/metrics-api/Dockerfile .
COPY services/metrics-api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/metrics-api/ .

EXPOSE 8000

CMD ["python", "main.py"]
