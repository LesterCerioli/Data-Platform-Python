# FastBI Data Platform — Glue + EMR (Python)

A self-hosted data platform implementing **Glue-like catalog/crawler** and **EMR-like Spark compute** using Python 3.14, FastAPI, and LocalStack (S3 + DynamoDB).

## Architecture

| Component | AWS Equivalent | Implementation |
|-----------|----------------|----------------|
| **Catalog** | AWS Glue Data Catalog | `app/catalog/` — DynamoDB-backed table registry |
| **Crawler** | AWS Glue Crawler | `app/catalog/crawler.py` — Async S3 scanner with schema inference |
| **Compute** | AWS EMR | `app/services/emr/` — Cluster + Step lifecycle, `spark-submit` backend |
| **Data Lake** | S3 | LocalStack S3 bucket (`data-lake`) with layered structure |
| **State** | DynamoDB | LocalStack DynamoDB tables (`emr_clusters`, `emr_steps`, `DataCatalog`) |

## Project Structure

```
FastBI-EMR-Glue-Platform/
├── .env                    # Environment configuration
├── config.py               # Root settings (pydantic-settings)
├── main.py                 # FastAPI app + lifespan + routes
├── app/
│   ├── core/config.py      # Re-export of root settings
│   ├── catalog/            # Glue-like catalog & crawler
│   │   ├── models.py       # TableInfo, ColumnInfo, ScanTask
│   │   ├── dynamo_store.py # DynamoDB persistence
│   │   ├── schema_infer.py # Parquet/CSV schema inference
│   │   └── crawler.py      # Async folder scanner
│   ├── api/v1/routes/
│   │   └── emr.py          # EMR REST endpoints
│   ├── bootstrap/
│   │   └── emr_bootstrap.py # S3 bucket + DynamoDB tables creation
│   ├── domain/emr/enums.py # Cluster/Step status enums
│   ├── infrastructure/
│   │   ├── aws/            # S3 & DynamoDB clients + repositories
│   │   └── emr/            # SparkExecutor interface + subprocess impl
│   ├── schemas/emr.py      # Pydantic request/response models
│   ├── services/emr/       # ClusterManager, StepExecutor, tasks
│   └── utils/time_utils.py
└── venv/                   # Python virtual environment
```

## Prerequisites

- Python 3.14+
- LocalStack (for S3 + DynamoDB) **or** real AWS credentials
- `spark-submit` available in PATH (for EMR step execution)

## Quick Start

### 1. Start LocalStack (S3 + DynamoDB)

**Option A: Docker (recommended)**
```bash
docker run -d --name localstack \
  -p 4566:4566 -p 4510-4559:4510-4559 \
  -e SERVICES=s3,dynamodb \
  localstack/localstack:latest

# Wait for services to be ready
sleep 10
curl http://localhost:4566/_localstack/health
```

**Option B: LocalStack via pip (no Docker)**
```bash
pip install localstack
localstack start -d
sleep 10
```

**Option C: Real AWS**
Edit `.env` with your AWS credentials and remove `AWS_ENDPOINT_URL`.

### 2. Create and activate virtual environment
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt  # or install manually below
```

### 3. Install dependencies
```bash
pip install fastapi uvicorn pydantic pydantic-settings boto3 botocore s3fs pyarrow pandas
```

### 4. Run the application
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at `http://localhost:8000`.

## API Endpoints

### Catalog (Glue-like)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Health check |
| `POST` | `/catalog/scan` | Start async crawler scan (prefix: `curated/`) |
| `GET` | `/catalog/scan/{task_id}` | Get scan task status |
| `GET` | `/catalog/tables` | List all tables (supports `?search=`) |
| `GET` | `/catalog/tables/{table_name}` | Get table details |

### EMR (Compute)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/emr/clusters` | Create cluster (TRANSIENT/PERSISTENT) |
| `GET` | `/api/v1/emr/clusters` | List clusters (filter: `?status=RUNNING`) |
| `GET` | `/api/v1/emr/clusters/{cluster_id}` | Describe cluster + steps |
| `POST` | `/api/v1/emr/clusters/{cluster_id}/terminate` | Terminate cluster |
| `POST` | `/api/v1/emr/clusters/{cluster_id}/steps` | Add Spark step |
| `GET` | `/api/v1/emr/clusters/{cluster_id}/steps` | List steps |
| `GET` | `/api/v1/emr/steps/{step_id}` | Describe step (logs, duration, status) |
| `POST` | `/api/v1/emr/steps/{step_id}/cancel` | Cancel running step |

## Example Usage

### Create a transient EMR cluster
```bash
curl -X POST http://localhost:8000/api/v1/emr/clusters \
  -H "Content-Type: application/json" \
  -d '{"cluster_name": "etl-cluster", "cluster_type": "TRANSIENT"}'
```

### Submit a PySpark step (script must exist in S3 at `scripts/clean.py`)
```bash
curl -X POST http://localhost:8000/api/v1/emr/clusters/j-xxxxxxxxxxxx/steps \
  -H "Content-Type: application/json" \
  -d '{
    "step_type": "SPARK_SUBMIT",
    "spark_submit": {
      "name": "clean-data",
      "entrypoint": "scripts/clean.py",
      "app_args": ["--input=raw/events", "--output=processed/events"],
      "spark_conf_override": {"spark.executor.memory": "4g"}
    }
  }'
```

### Start a catalog scan
```bash
curl -X POST "http://localhost:8000/catalog/scan?prefix=curated/"
```

### List discovered tables
```bash
curl http://localhost:8000/catalog/tables
```

## Data Lake Structure (S3)

```
s3://data-lake/
├── raw/              # Raw ingested data
├── staging/          # Intermediate processing
├── processed/        # Cleaned, typed data (curated)
├── curated/          # Business-ready datasets
├── scripts/          # PySpark/JAR files for EMR steps
├── temp/             # Spark temp/checkpoint data
└── emr/logs/         # Spark event logs + step stdout/stderr
```

## Configuration (`.env`)

```env
# LocalStack / AWS
LOCALSTACK_ENDPOINT=http://localhost:4566
AWS_ENDPOINT_URL=http://localhost:4566
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=us-east-1
AWS_REGION=us-east-1

# Data Lake
S3_DATALAKE_BUCKET=data-lake
DATA_LAKE_BUCKET=data-lake

# DynamoDB
DYNAMO_CATALOG_TABLE=DataCatalog
DYNAMODB_TABLE_EMR_CLUSTERS=emr_clusters
DYNAMODB_TABLE_EMR_STEPS=emr_steps

# EMR Runtime
EMR_DEFAULT_EXECUTOR_CORES=2
EMR_DEFAULT_EXECUTOR_MEMORY=2g
EMR_DEFAULT_DRIVER_MEMORY=1g
EMR_DEFAULT_NUM_EXECUTORS=4
EMR_DEFAULT_SHUFFLE_PARTITIONS=200
EMR_DEFAULT_YARN_MEMORY_OVERHEAD=512m
EMR_ENABLE_SPARK_EVENT_LOGS=true
EMR_SPARK_EVENT_LOGS_DIR=emr/logs
EMR_TRANSIENT_AUTO_TERMINATE=true
EMR_TRANSIENT_TERMINATION_DELAY_SECONDS=5
SPARK_SUBMIT_PATH=spark-submit
```

## Cluster Types

| Type | Behavior |
|------|----------|
| `TRANSIENT` | Auto-terminates after all steps complete (default) |
| `PERSISTENT` | Stays running until explicit `terminate` call |

## Extending Spark Backend

The `SparkExecutor` interface (`app/infrastructure/emr/interfaces.py`) allows swapping execution backends:

- **Current**: `SubprocessSparkExecutor` — runs `spark-submit` locally
- **Future**: `LivySparkExecutor`, `KubernetesSparkExecutor` — implement the same interface

## License

Internal use only.