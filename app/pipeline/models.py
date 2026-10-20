from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel

from app.core.config import settings
from app.infrastructure.aws.dynamodb_client import get_dynamodb_resource


def _to_decimal(v: Any) -> Any:
    if isinstance(v, int | float) and not isinstance(v, bool):
        return str(v)
    if isinstance(v, dict):
        return {k: _to_decimal(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_to_decimal(x) for x in v]
    return v


def _from_decimal(v: Any) -> Any:
    if isinstance(v, str) and v.isdigit():
        return int(v)
    if isinstance(v, dict):
        return {k: _from_decimal(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_from_decimal(x) for x in v]
    return v


class StepDef(BaseModel):
    name: str
    step_type: str  
    entrypoint: str
    app_args: list[str] | None = None
    main_class: str | None = None
    jars: list[str] | None = None
    packages: list[str] | None = None
    py_files: list[str] | None = None
    files: list[str] | None = None
    command_args: list[str] | None = None
    driver_class_path: str | None = None
    executor_class_path: str | None = None
    master: str | None = None
    deploy_mode: str | None = None
    queue: str | None = None
    verbose: bool = False
    spark_conf_override: dict[str, str] | None = None
    retry_count: int = 0
    retry_delay_seconds: int = 0
    depends_on: list[str] | None = None  


class PipelineDef(BaseModel):
    name: str
    description: str | None = None
    steps: list[StepDef]
    idempotent: bool = False
    tags: dict[str, str] | None = None


class PipelineRun(BaseModel):
    pipeline_id: str
    pipeline_name: str
    status: str = "PENDING"
    steps: dict[str, dict[str, Any]] = {}
    current_step: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    failed_step: str | None = None
    tags: dict[str, str] | None = None


class PipelineRepository:
    
    def __init__(self) -> None:
        self.resource = get_dynamodb_resource()
        self.pipelines_table = self.resource.Table(
            settings.DYNAMODB_TABLE_PIPELINES
        )
        self.steps_table = self.resource.Table(
            settings.DYNAMODB_TABLE_PIPELINE_STEPS
        )

    def ensure_tables_exist(self) -> None:
        client = self.resource.meta.client
        existing = client.list_tables().get("TableNames", [])

        if settings.DYNAMODB_TABLE_PIPELINES not in existing:
            self.resource.create_table(
                TableName=settings.DYNAMODB_TABLE_PIPELINES,
                KeySchema=[{"AttributeName": "pipeline_id", "KeyType": "HASH"}],
                AttributeDefinitions=[
                    {"AttributeName": "pipeline_id", "AttributeType": "S"}
                ],
                BillingMode="PAY_PER_REQUEST",
            )

        if settings.DYNAMODB_TABLE_PIPELINE_STEPS not in existing:
            self.resource.create_table(
                TableName=settings.DYNAMODB_TABLE_PIPELINE_STEPS,
                KeySchema=[
                    {"AttributeName": "run_id", "KeyType": "HASH"},
                    {"AttributeName": "step_id", "KeyType": "Range"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "run_id", "AttributeType": "S"},
                    {"AttributeName": "step_id", "AttributeType": "S"},
                ],
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": "pipeline_id-index",
                        "KeySchema": [{"AttributeName": "pipeline_id", "KeyType": "HASH"}],
                        "Projection": {"ProjectionType": "ALL"},
                    }
                ],
                BillingMode="PAY_PER_REQUEST",
            )

        try:
            self.pipelines_table.wait_until_exists()
            self.steps_table.wait_until_exists()
        except Exception:
            pass

    def create_run(self, pipeline_id: str, pipeline_name: str) -> PipelineRun:
        run = PipelineRun(
            pipeline_id=pipeline_id,
            pipeline_name=pipeline_name,
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        self.pipelines_table.put_item(Item=_to_decimal(run.model_dump()))
        return run

    def get_run(self, pipeline_id: str) -> PipelineRun | None:
        try:
            resp = self.pipelines_table.get_item(Key={"pipeline_id": pipeline_id})
        except Exception:
            return None
        item = resp.get("Item")
        if not item:
            return None
        data = _from_decimal(item)
        return PipelineRun(**data)

    def update_step(
        self,
        pipeline_id: str,
        step_id: str,
        status: str,
        stdout_path_s3_key: str | None = None,
        stderr_path_s3_key: str | None = None,
        stdout_lines: int = 0,
        stderr_lines: int = 0,
        return_code: int | None = None,
        failure_reason: str | None = None,
    ) -> None:
        run = self.get_run(pipeline_id)
        if not run:
            return

        step_data = {
            "step_id": step_id,
            "status": status,
            "stdout_path_s3_key": stdout_path_s3_key,
            "stderr_path_s3_key": stderr_path_s3_key,
            "stdout_lines": stdout_lines,
            "stderr_lines": stderr_lines,
            "return_code": return_code,
            "failure_reason": failure_reason,
        }
        run.steps[step_id] = step_data

        
        all_completed = all(
            s.get("status") in ("COMPLETED", "FAILED", "CANCELLED")
            for s in run.steps.values()
        )
        any_failed = any(s.get("status") == "FAILED" for s in run.steps.values())
        any_running = any(s.get("status") == "RUNNING" for s in run.steps.values())

        if any_failed:
            run.failed_step = step_id
            run.status = "FAILED"
        elif all_completed:
            run.status = "COMPLETED"
            run.completed_at = datetime.now(timezone.utc).isoformat()
        elif any_running:
            run.status = "RUNNING"

        
        self.pipelines_table.put_item(Item=_to_decimal(run.model_dump()))

        
        self.steps_table.put_item(
            Item=_to_decimal(
                {
                    "run_id": pipeline_id,
                    "step_id": step_id,
                    "status": status,
                    "updated_at": run.completed_at or run.started_at or "",
                    "failure_reason": failure_reason,
                }
            )
        )

    def get_step_status(self, pipeline_id: str, step_id: str) -> dict[str, Any] | None:
        run = self.get_run(pipeline_id)
        if not run:
            return None
        return run.steps.get(step_id)

    def list_steps_by_run(
        self, pipeline_id: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        run = self.get_run(pipeline_id)
        if not run:
            return []
        return [
            {"step_id": sid, **data}
            for sid, data in run.steps.items()
        ][:limit]