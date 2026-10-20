import asyncio
import uuid
from datetime import datetime
from typing import Dict, List
import boto3
from botocore.config import Config
from app.catalog.models import TableInfo, ScanTask, ColumnInfo
from app.catalog.schema_infer import infer_from_parquet, infer_from_csv
from app.catalog.dynamo_store import AsyncDynamoCatalog
from config import settings


class AsyncCrawler:
    def __init__(self):
        self.catalog = AsyncDynamoCatalog()
        self.active_tasks: Dict[str, ScanTask] = {}
        self.semaphore = asyncio.Semaphore(settings.MAX_PARALLEL_SCANS)
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=settings.LOCALSTACK_ENDPOINT,
            region_name=settings.AWS_DEFAULT_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            config=Config(retries={"max_attempts": 3, "mode": "standard"}),
        )
        self.s3_resource = boto3.resource(
            "s3",
            endpoint_url=settings.LOCALSTACK_ENDPOINT,
            region_name=settings.AWS_DEFAULT_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            config=Config(retries={"max_attempts": 3, "mode": "standard"}),
        )
        self.bucket_name = settings.S3_DATALAKE_BUCKET

    def _detect_format(self, files: List[str]) -> str:
        for ext in [".parquet", ".csv", ".json"]:
            if any(f.endswith(ext) for f in files):
                return ext.lstrip(".")
        return "unknown"

    def _extract_table_name(self, prefix: str) -> str:
        parts = [p for p in prefix.split("/") if p and p not in ["raw", "scripts", "temp", "emr"]]
        return "_".join(parts) or "default_table"

    def _list_folder_objects(self, folder_path: str) -> list:
        
        objects = []
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=folder_path):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    
                    if key != folder_path.rstrip("/"):
                        objects.append(key)
        except Exception:
            return []
        return objects

    def _is_folder(self, key: str, folder_path: str) -> bool:
        
        return key.startswith(folder_path.rstrip("/") + "/") if folder_path else False

    async def scan_single_folder(self, folder_path: str):
        async with self.semaphore:
            await asyncio.sleep(0)
            try:
                files = self._list_folder_objects(folder_path)
                if not files:
                    return None
                fmt = self._detect_format(files)
                if fmt == "unknown":
                    return None
                samples = [f for f in files if f.endswith(f".{fmt}")]
                if not samples:
                    return None

                loop = asyncio.get_event_loop()
                if fmt == "parquet":
                    cols = await loop.run_in_executor(
                        None, infer_from_parquet, f"s3://{self.bucket_name}/{samples[0]}"
                    )
                elif fmt == "csv":
                    cols = await loop.run_in_executor(
                        None, infer_from_csv, f"s3://{self.bucket_name}/{samples[0]}"
                    )
                else:
                    cols = []

                table_name = self._extract_table_name(folder_path)
                existing = await self.catalog.get_table_by_name(table_name)
                now = datetime.utcnow().isoformat()

                table = TableInfo(
                    table_name=table_name,
                    location=f"s3://{folder_path}",
                    format=fmt,
                    columns=cols,
                    created_at=existing.created_at if existing else now,
                    updated_at=now,
                )
                await self.catalog.save_table(table)
                return {"name": table_name, "is_new": existing is None}
            except Exception as e:
                print(f"Erro em {folder_path}: {e}")
                return None

    async def start_scan(self, prefix: str = "curated/") -> ScanTask:
        task_id = str(uuid.uuid4())
        full_prefix = f"{settings.S3_DATALAKE_BUCKET}/{prefix}"

        task = ScanTask(
            task_id=task_id,
            status="running",
            prefix=prefix,
            started_at=datetime.utcnow().isoformat(),
        )
        self.active_tasks[task_id] = task
        await self.catalog.save_task(task)

        try:
            entries = self._list_folder_objects(full_prefix)
            folders = [p for p in entries if self._is_folder(p, full_prefix)]
        except Exception:
            folders = []

        async def process_all():
            return await asyncio.gather(
                *[self.scan_single_folder(f) for f in folders],
                return_exceptions=True,
            )

        asyncio.create_task(self._finalize(task_id, process_all()))
        return task

    async def _finalize(self, task_id: str, coro):
        try:
            results = await coro
            task = self.active_tasks.get(task_id)
            if not task:
                return
            new_t = []
            upd_t = []
            for r in results:
                if isinstance(r, dict) and "name" in r:
                    (new_t if r["is_new"] else upd_t).append(r["name"])
            task.status = "completed"
            task.new_tables = new_t
            task.updated_tables = upd_t
            task.total_discovered = len(new_t) + len(upd_t)
            task.finished_at = datetime.utcnow().isoformat()
            await self.catalog.save_task(task)
        except Exception as e:
            task = self.active_tasks.get(task_id)
            if task:
                task.status = "failed"
                task.error = str(e)
                task.finished_at = datetime.utcnow().isoformat()
                await self.catalog.save_task(task)
        asyncio.create_task(self._cleanup(task_id))

    async def _cleanup(self, task_id: str):
        await asyncio.sleep(3600)
        self.active_tasks.pop(task_id, None)

    def get_task(self, task_id: str) -> ScanTask | None:
        return self.active_tasks.get(task_id)