from typing import List, Optional
import boto3
from botocore.config import Config
from config import settings
from app.catalog.models import TableInfo, ScanTask, ColumnInfo


class AsyncDynamoCatalog:
    def __init__(self):
        self.client = boto3.client(
            "dynamodb",
            endpoint_url=settings.LOCALSTACK_ENDPOINT,
            region_name=settings.AWS_DEFAULT_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            config=Config(retries={"max_attempts": 3, "mode": "standard"}),
        )
        self.table_name = settings.DYNAMO_CATALOG_TABLE
        self._ensure_table()

    def _ensure_table(self):
        try:
            self.client.describe_table(TableName=self.table_name)
        except self.client.exceptions.ResourceNotFoundException:
            self.client.create_table(
                TableName=self.table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                    {"AttributeName": "table_name", "AttributeType": "S"},
                ],
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": "table_name-index",
                        "KeySchema": [{"AttributeName": "table_name", "KeyType": "HASH"}],
                        "Projection": {"ProjectionType": "ALL"},
                    }
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            self.client.get_waiter("table_exists").wait(TableName=self.table_name)

    def _table_to_item(self, table: TableInfo) -> dict:
        return {
            "pk": {"S": "TABLE"},
            "sk": {"S": table.table_name},
            "table_name": {"S": table.table_name},
            "location": {"S": table.location},
            "format": {"S": table.format},
            "columns": {"S": table.model_dump_json()},
            "partitions": {"S": str(table.partitions) if table.partitions else "[]"},
            "row_count": {"N": str(table.row_count)} if table.row_count else {"NULL": True},
            "created_at": {"S": table.created_at},
            "updated_at": {"S": table.updated_at},
            "source": {"S": table.source} if table.source else {"NULL": True},
        }

    def _item_to_table(self, item: dict) -> TableInfo:
        import json
        columns_data = json.loads(item.get("columns", {}).get("S", "[]"))
        columns = [ColumnInfo(**c) for c in columns_data.get("columns", [])]
        import ast
        partitions = ast.literal_eval(item.get("partitions", {}).get("S", "[]"))
        return TableInfo(
            table_name=item["table_name"]["S"],
            location=item["location"]["S"],
            format=item["format"]["S"],
            columns=columns,
            partitions=partitions,
            row_count=int(item.get("row_count", {}).get("N", 0)) if item.get("row_count", {}).get("N") else None,
            created_at=item["created_at"]["S"],
            updated_at=item["updated_at"]["S"],
            source=item.get("source", {}).get("S"),
        )

    async def save_table(self, table: TableInfo) -> None:
        item = self._table_to_item(table)
        self.client.put_item(TableName=self.table_name, Item=item)

    async def get_table_by_name(self, table_name: str) -> Optional[TableInfo]:
        try:
            resp = self.client.query(
                TableName=self.table_name,
                IndexName="table_name-index",
                KeyConditionExpression="table_name = :tn",
                ExpressionAttributeValues={":tn": {"S": table_name}},
            )
            items = resp.get("Items", [])
            if not items:
                return None
            return self._item_to_table(items[0])
        except Exception:
            return None

    async def list_all_tables(self) -> List[TableInfo]:
        resp = self.client.query(
            TableName=self.table_name,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "TABLE"}},
        )
        return [self._item_to_table(item) for item in resp.get("Items", [])]

    async def search_tables(self, query: str) -> List[TableInfo]:
        all_tables = await self.list_all_tables()
        return [t for t in all_tables if query.lower() in t.table_name.lower()]

    # ScanTask methods
    async def save_task(self, task: ScanTask) -> None:
        item = {
            "pk": {"S": "SCAN_TASK"},
            "sk": {"S": task.task_id},
            "task_id": {"S": task.task_id},
            "status": {"S": task.status},
            "prefix": {"S": task.prefix},
            "new_tables": {"S": str(task.new_tables)},
            "updated_tables": {"S": str(task.updated_tables)},
            "total_discovered": {"N": str(task.total_discovered)},
            "error": {"S": task.error} if task.error else {"NULL": True},
            "started_at": {"S": task.started_at},
            "finished_at": {"S": task.finished_at} if task.finished_at else {"NULL": True},
        }
        self.client.put_item(TableName=self.table_name, Item=item)

    async def get_task(self, task_id: str) -> Optional[ScanTask]:
        try:
            resp = self.client.get_item(TableName=self.table_name, Key={"pk": {"S": "SCAN_TASK"}, "sk": {"S": task_id}})
            item = resp.get("Item")
            if not item:
                return None
            import ast
            return ScanTask(
                task_id=item["task_id"]["S"],
                status=item["status"]["S"],
                prefix=item["prefix"]["S"],
                new_tables=ast.literal_eval(item["new_tables"]["S"]),
                updated_tables=ast.literal_eval(item["updated_tables"]["S"]),
                total_discovered=int(item["total_discovered"]["N"]),
                error=item.get("error", {}).get("S"),
                started_at=item["started_at"]["S"],
                finished_at=item.get("finished_at", {}).get("S"),
            )
        except Exception:
            return None