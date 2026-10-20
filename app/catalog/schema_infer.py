import pyarrow.parquet as pq
import pandas as pd
from typing import List
from app.catalog.models import ColumnInfo


def infer_from_parquet(s3_path: str, fs) -> List[ColumnInfo]:
    with fs.open(s3_path, "rb") as f:
        schema = pq.read_schema(f)

    columns = []
    for name in schema.names:
        t = str(schema.field(name).type).lower()
        if "int" in t:
            dtype = "integer"
        elif "float" in t or "decimal" in t:
            dtype = "float"
        elif "bool" in t:
            dtype = "boolean"
        elif "date" in t or "time" in t:
            dtype = "datetime"
        else:
            dtype = "string"
        columns.append(ColumnInfo(name=name, data_type=dtype))
    return columns


def infer_from_csv(s3_path: str, fs) -> List[ColumnInfo]:
    with fs.open(s3_path, "rb") as f:
        df = pd.read_csv(f, nrows=100)

    columns = []
    for name, dtype in df.dtypes.items():
        t = str(dtype)
        if "int" in t:
            col_type = "integer"
        elif "float" in t:
            col_type = "float"
        elif "bool" in t:
            col_type = "boolean"
        elif "datetime" in t:
            col_type = "datetime"
        else:
            col_type = "string"
        columns.append(ColumnInfo(name=name, data_type=col_type))
    return columns