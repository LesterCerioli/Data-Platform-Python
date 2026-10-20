import json
from typing import Any

from botocore.exceptions import ClientError

from app.core.config import settings
from app.infrastructure.aws.s3_client import get_s3_client, get_s3_resource


class DataLakeS3Client:
    def __init__(self) -> None:
        self.s3_resource = get_s3_resource()
        self.s3_client = get_s3_client()
        self.bucket_name = settings.S3_DATALAKE_BUCKET

    def ensure_bucket_exists(self) -> None:
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchBucket", "NotFound"):
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    **(
                        {"CreateBucketConfiguration": {"LocationConstraint": settings.AWS_REGION}}
                        if settings.AWS_REGION not in ("us-east-1", None)
                        else {}
                    ),
                )
            else:
                raise

    def put_text(self, key: str, body: str, content_type: str = "text/plain") -> None:
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key.lstrip("/"),
            Body=body.encode("utf-8"),
            ContentType=content_type,
        )

    def append_text(self, key: str, body: str, content_type: str = "text/plain") -> None:
        existing = ""
        try:
            resp = self.s3_client.get_object(Bucket=self.bucket_name, Key=key.lstrip("/"))
            existing = resp["Body"].read().decode("utf-8", errors="replace")
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") not in ("NoSuchKey", "404"):
                raise
        new_body = (existing + body) if existing else body
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key.lstrip("/"),
            Body=new_body.encode("utf-8"),
            ContentType=content_type,
        )

    def put_json(self, key: str, data: dict[str, Any]) -> None:
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key.lstrip("/"),
            Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

    def upload_file(self, local_path: str, key: str) -> None:
        self.s3_client.upload_file(local_path, self.bucket_name, key.lstrip("/"))

    def generate_presigned_url(self, key: str, expires_in: int = 3600) -> str:
        return self.s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket_name, "Key": key.lstrip("/")},
            ExpiresIn=expires_in,
        )

    @staticmethod
    def build_log_key(cluster_id: str, step_id: str, filename: str) -> str:
        return f"{settings.EMR_SPARK_EVENT_LOGS_DIR}/{cluster_id}/{step_id}/{filename}"

    @staticmethod
    def build_script_key(filename: str) -> str:
        return f"scripts/{filename.lstrip('/')}"

    @staticmethod
    def build_temp_key(prefix: str, filename: str) -> str:
        return f"temp/{prefix.strip('/')}/{filename.lstrip('/')}"


def get_data_lake_client() -> DataLakeS3Client:
    return DataLakeS3Client()