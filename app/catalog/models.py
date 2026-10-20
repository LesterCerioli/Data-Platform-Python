from pydantic import BaseModel
from typing import List, Optional, Literal

class ColumnInfo(BaseModel):
    name: str
    data_type: str
    is_partition: bool = False

class TableInfo(BaseModel):
    table_name: str
    location: str
    format: Literal["parquet", "csv", "json"]
    columns: List[ColumnInfo]
    partitions: Optional[List[str]] = None
    row_count: Optional[int] = None
    created_at: str
    updated_at: str
    source: Optional[str] = None

class ScanTask(BaseModel):
    task_id: str
    status: Literal["pending", "running", "completed", "failed"]
    prefix: str
    new_tables: List[str] = []
    updated_tables: List[str] = []
    total_discovered: int = 0
    error: Optional[str] = None
    started_at: str
    finished_at: Optional[str] = None