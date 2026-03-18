FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching)
# Build context is the repo root: docker build -f services/log-collector/Dockerfile .
COPY services/log-collector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Generate gRPC stubs from proto
COPY proto/logs.proto /proto/logs.proto
RUN python -m grpc_tools.protoc \
    -I /proto \
    --python_out=/app \
    --grpc_python_out=/app \
    /proto/logs.proto

# Copy service source
COPY services/log-collector/ .

EXPOSE 50051 50052

CMD ["python", "main.py"]
