# FastBI Data Platform - Docker Image
# Python 3.14 slim base for minimal attack surface

FROM python:3.14-slim AS base

# System dependencies for pyarrow, pandas, and AWS SDK
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd -r appgroup && useradd -r -g appgroup -m -d /home/appuser appuser

# Set working directory
WORKDIR /app

# Copy requirements first for better cache layering
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY --chown=appuser:appgroup . .

# Create necessary directories
RUN mkdir -p /app/logs && chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 7000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:7000/ || exit 1

# Default environment variables (non-sensitive defaults only)
# Sensitive values must be provided via .env file at runtime:
# docker run --env-file .env fastbi-platform:latest
# or via docker-compose which auto-loads .env
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \ 
    LOCALSTACK_ENDPOINT="" \
    AWS_ENDPOINT_URL="" \
    AWS_ACCESS_KEY_ID="" \
    AWS_SECRET_ACCESS_KEY="" \
    AWS_DEFAULT_REGION="" \
    AWS_REGION="" \
    S3_DATALAKE_BUCKET="" \
    DATA_LAKE_BUCKET="" \
    DYNAMO_CATALOG_TABLE="" \
    DYNAMODB_TABLE_EMR_CLUSTERS="" \
    DYNAMODB_TABLE_EMR_STEPS="" \
    MAX_PARALLEL_SCANS=5 \
    MAX_CONCURRENT_TASKS=2 \
    EMR_DEFAULT_EXECUTOR_CORES=2 \
    EMR_DEFAULT_EXECUTOR_MEMORY=2g \
    EMR_DEFAULT_DRIVER_MEMORY=1g \
    EMR_DEFAULT_NUM_EXECUTORS=4 \
    EMR_DEFAULT_SHUFFLE_PARTITIONS=200 \
    EMR_DEFAULT_YARN_MEMORY_OVERHEAD=512m \
    EMR_ENABLE_SPARK_EVENT_LOGS="" \
    EMR_SPARK_EVENT_LOGS_DIR="" \
    EMR_TRANSIENT_AUTO_TERMINATE="" \
    EMR_TRANSIENT_TERMINATION_DELAY_SECONDS=5 \
    SPARK_SUBMIT_PATH=""

# Run application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7000"]