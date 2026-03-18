FROM python:3.11-slim

WORKDIR /app

# Build context is repo root: docker build -f services/log-producer/Dockerfile .
COPY services/log-producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Generate gRPC stubs from proto
COPY proto/logs.proto /proto/logs.proto
RUN python -m grpc_tools.protoc \
    -I /proto \
    --python_out=/app \
    --grpc_python_out=/app \
    /proto/logs.proto

COPY services/log-producer/ .

CMD ["python", "main.py"]
