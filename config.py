from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitine=True,
    )
    LOCALSTACK_ENDPOINT: str
    AWS_ENDPOINT_URL: str
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_SESSION_TOKEN: str | None = None
    AWS_DEFAULT_REGION: str
    AWS_REGION: str

    S3_DATALAKE_BUCKET: str
    DATA_LAKE_BUCKET: str

    DYNAMO_CATALOG_TABLE: str
    DYNAMODB_TABLE_EMR_CLUSTERS: str
    DYNAMODB_TABLE_EMR_STEPS: str
    DYNAMODB_TABLE_PIPELINES: str
    DYNAMODB_TABLE_PIPELINE_STEPS: str

    MAX_PARALLEL_SCANS: int
    MAX_CONCURRENT_TASKS: int

    EMR_DEFAULT_EXECUTOR_CORES: int
    EMR_DEFAULT_EXECUTOR_MEMORY: str
    EMR_DEFAULT_DRIVER_MEMORY: str
    EMR_DEFAULT_NUM_EXECUTORS: int | None = None
    EMR_DEFAULT_SHUFFLE_PARTITIONS: int
    EMR_DEFAULT_YARN_MEMORY_OVERHEAD: str | None = None
    EMR_ENABLE_SPARK_EVENT_LOGS: bool
    EMR_SPARK_EVENT_LOGS_DIR: str
    EMR_TRANSIENT_AUTO_TERMINATE: bool
    EMR_TRANSIENT_TERMINATION_DELAY_SECONDS: int
    SPARK_SUBMIT_PATH: str

    # Authentication settings
    AUTH_CLIENT_ID: str
    AUTH_CLIENT_SECRET: str
    AUTH_PRIVATE_KEY_BASE64: str
    AUTH_PUBLIC_KEY_BASE64: str
    JWT_ISSUER: str
    JWT_AUDIENCE: str
    TOKEN_EXPIRATION_SECONDS: int
    DYNAMODB_TABLE_AUTH: str


settings = Settings()