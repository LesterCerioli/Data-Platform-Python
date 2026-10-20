from decimal import Decimal
from typing import Any

from botocore.exceptions import ClientError

from app.core.config import settings
from app.domain.emr.enums import ClusterStatus, StepStatus
from app.infrastructure.aws.dynamodb_client import get_dynamodb_resource


def _to_decimal(v: Any) -> Any:
    if isinstance(v, int | float) and not isinstance(v, bool):
        return Decimal(str(v))
    if isinstance(v, dict):
        return {k: _to_decimal(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_to_decimal(x) for x in v]
    return v


def _from_decimal(v: Any) -> Any:
    if isinstance(v, Decimal):
        if v == v.to_integral_value():
            return int(v)
        return float(v)
    if isinstance(v, dict):
        return {k: _from_decimal(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_from_decimal(x) for x in v]
    return v


class EmrDynamoDBRepository:
    def __init__(self) -> None:
        self.resource = get_dynamodb_resource()
        self.clusters_table = self.resource.Table(settings.DYNAMODB_TABLE_EMR_CLUSTERS)
        self.steps_table = self.resource.Table(settings.DYNAMODB_TABLE_EMR_STEPS)

    def ensure_tables_exist(self) -> None:
        client = self.resource.meta.client
        existing = client.list_tables().get("TableNames", [])

        if settings.DYNAMODB_TABLE_EMR_CLUSTERS not in existing:
            self.resource.create_table(
                TableName=settings.DYNAMODB_TABLE_EMR_CLUSTERS,
                KeySchema=[{"AttributeName": "cluster_id", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "cluster_id", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )

        if settings.DYNAMODB_TABLE_EMR_STEPS not in existing:
            self.resource.create_table(
                TableName=settings.DYNAMODB_TABLE_EMR_STEPS,
                KeySchema=[{"AttributeName": "step_id", "KeyType": "HASH"}],
                AttributeDefinitions=[
                    {"AttributeName": "step_id", "AttributeType": "S"},
                    {"AttributeName": "cluster_id", "AttributeType": "S"},
                ],
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": "cluster_id-index",
                        "KeySchema": [{"AttributeName": "cluster_id", "KeyType": "HASH"}],
                        "Projection": {"ProjectionType": "ALL"},
                    }
                ],
                BillingMode="PAY_PER_REQUEST",
            )

        try:
            self.clusters_table.wait_until_exists()
            self.steps_table.wait_until_exists()
        except Exception:
            pass

    def create_cluster(self, item: dict[str, Any]) -> dict[str, Any]:
        self.clusters_table.put_item(Item=_to_decimal(item))
        return item

    def get_cluster(self, cluster_id: str) -> dict[str, Any] | None:
        try:
            resp = self.clusters_table.get_item(Key={"cluster_id": cluster_id})
        except ClientError:
            return None
        item = resp.get("Item")
        return _from_decimal(item) if item else None

    def list_clusters(self, limit: int = 50, status: ClusterStatus | None = None) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        scan_kwargs: dict[str, Any] = {"Limit": limit}

        if status:
            scan_kwargs["FilterExpression"] = "cluster_status = :s"
            scan_kwargs["ExpressionAttributeValues"] = {":s": status.value}

        resp = self.clusters_table.scan(**scan_kwargs)
        items.extend(_from_decimal(i) for i in resp.get("Items", []))

        while "LastEvaluatedKey" in resp and len(items) < limit:
            scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
            resp = self.clusters_table.scan(**scan_kwargs)
            items.extend(_from_decimal(i) for i in resp.get("Items", []))

        return items[:limit]

    def update_cluster_status(
        self,
        cluster_id: str,
        status: ClusterStatus,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        extra = extra or {}
        update_expr = ["cluster_status = :s", "updated_at = :u"]
        values: dict[str, Any] = {":s": status.value, ":u": extra.pop("updated_at", None)}

        if status == ClusterStatus.RUNNING and "started_at" not in extra:
            extra["started_at"] = extra.get("updated_at")

        if status in (ClusterStatus.TERMINATED, ClusterStatus.FAILED, ClusterStatus.TERMINATING):
            if "terminated_at" not in extra and status != ClusterStatus.TERMINATING:
                extra["terminated_at"] = extra.get("updated_at")

        for k, v in extra.items():
            update_expr.append(f"{k} = :{k}")
            values[f":{k}"] = v

        try:
            resp = self.clusters_table.update_item(
                Key={"cluster_id": cluster_id},
                UpdateExpression="SET " + ", ".join(update_expr),
                ExpressionAttributeValues=_to_decimal(values),
                ReturnValues="ALL_NEW",
            )
        except ClientError:
            return None
        return _from_decimal(resp.get("Attributes"))

    def delete_cluster(self, cluster_id: str) -> None:
        try:
            self.clusters_table.delete_item(Key={"cluster_id": cluster_id})
        except ClientError:
            pass

    def create_step(self, item: dict[str, Any]) -> dict[str, Any]:
        self.steps_table.put_item(Item=_to_decimal(item))
        return item

    def get_step(self, step_id: str) -> dict[str, Any] | None:
        try:
            resp = self.steps_table.get_item(Key={"step_id": step_id})
        except ClientError:
            return None
        item = resp.get("Item")
        return _from_decimal(item) if item else None

    def list_steps_by_cluster(
        self,
        cluster_id: str,
        limit: int = 100,
        status: StepStatus | None = None,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        query_kwargs: dict[str, Any] = {
            "IndexName": "cluster_id-index",
            "KeyConditionExpression": "cluster_id = :cid",
            "ExpressionAttributeValues": {":cid": cluster_id},
            "Limit": limit,
        }

        if status:
            query_kwargs["FilterExpression"] = "step_status = :s"
            query_kwargs["ExpressionAttributeValues"] = {":cid": cluster_id, ":s": status.value}

        resp = self.steps_table.query(**query_kwargs)
        items.extend(_from_decimal(i) for i in resp.get("Items", []))

        while "LastEvaluatedKey" in resp and len(items) < limit:
            query_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
            resp = self.steps_table.query(**query_kwargs)
            items.extend(_from_decimal(i) for i in resp.get("Items", []))

        items.sort(key=lambda x: x.get("created_at") or "")
        return items[:limit]

    def count_running_steps(self, cluster_id: str) -> int:
        steps = self.list_steps_by_cluster(cluster_id, limit=500)
        return sum(1 for s in steps if s.get("step_status") == StepStatus.RUNNING.value)

    def count_terminal_steps(self, cluster_id: str) -> int:
        steps = self.list_steps_by_cluster(cluster_id, limit=500)
        terminal = {StepStatus.COMPLETED.value, StepStatus.FAILED.value, StepStatus.CANCELLED.value}
        return sum(1 for s in steps if s.get("step_status") in terminal)

    def count_steps(self, cluster_id: str) -> int:
        steps = self.list_steps_by_cluster(cluster_id, limit=1000)
        return len(steps)

    def update_step_status(
        self,
        step_id: str,
        status: StepStatus,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        extra = extra or {}
        update_expr = ["step_status = :s", "updated_at = :u"]
        values: dict[str, Any] = {":s": status.value, ":u": extra.pop("updated_at", None)}

        if status == StepStatus.RUNNING and "started_at" not in extra:
            extra["started_at"] = extra.get("updated_at")

        if status in (StepStatus.COMPLETED, StepStatus.FAILED, StepStatus.CANCELLED):
            if "ended_at" not in extra:
                extra["ended_at"] = extra.get("updated_at")

        for k, v in extra.items():
            update_expr.append(f"{k} = :{k}")
            values[f":{k}"] = v

        try:
            resp = self.steps_table.update_item(
                Key={"step_id": step_id},
                UpdateExpression="SET " + ", ".join(update_expr),
                ExpressionAttributeValues=_to_decimal(values),
                ReturnValues="ALL_NEW",
            )
        except ClientError:
            return None
        return _from_decimal(resp.get("Attributes"))

    def update_step_partial(self, step_id: str, extra: dict[str, Any]) -> dict[str, Any] | None:
        if not extra:
            return self.get_step(step_id)

        update_expr_list: list[str] = ["updated_at = :u"]
        values: dict[str, Any] = {":u": extra.pop("updated_at", None)}

        for k, v in extra.items():
            update_expr_list.append(f"{k} = :{k}")
            values[f":{k}"] = v

        try:
            resp = self.steps_table.update_item(
                Key={"step_id": step_id},
                UpdateExpression="SET " + ", ".join(update_expr_list),
                ExpressionAttributeValues=_to_decimal(values),
                ReturnValues="ALL_NEW",
            )
        except ClientError:
            return None
        return _from_decimal(resp.get("Attributes"))


def get_emr_repository() -> EmrDynamoDBRepository:
    return EmrDynamoDBRepository()