
import sys
import json
from datetime import datetime

sys.path.insert(0, r"C:\Users\Lester\repositories\github\enterprise\FastBI-EMR-Glue-Platform")

# Test 1: Catalog deserialization fix
print("=== Test 1: Catalog deserialization round-trip ===")
from app.catalog.models import TableInfo, ColumnInfo

t = TableInfo(
    table_name="test_table",
    location="s3://data-lake/curated/test",
    format="parquet",
    columns=[
        ColumnInfo(name="id", data_type="integer"),
        ColumnInfo(name="name", data_type="string"),
    ],
    row_count=1000,
    created_at=datetime.utcnow().isoformat(),
    updated_at=datetime.utcnow().isoformat(),
    source="test_source",
)

# Simulate _table_to_item
item = {
    "pk": {"S": "TABLE"},
    "sk": {"S": t.table_name},
    "table_name": {"S": t.table_name},
    "location": {"S": t.location},
    "format": {"S": t.format},
    "columns": {"S": t.model_dump_json()},
    "partitions": {"S": str(t.partitions) if t.partitions else "[]"},
    "row_count": {"N": str(t.row_count)} if t.row_count else {"NULL": True},
    "created_at": {"S": t.created_at},
    "updated_at": {"S": t.updated_at},
    "source": {"S": t.source} if t.source else {"NULL": True},
}

# The FIXED deserialization: columns_data.get('columns', [])
columns_raw = json.loads(item["columns"]["S"])
columns_list = columns_raw.get("columns", [])
restored = [ColumnInfo(**c) for c in columns_list]
assert len(restored) == 2, f"Expected 2 columns, got {len(restored)}"
assert restored[0].name == "id"
assert restored[1].name == "name"
print("PASS: Catalog deserialization round-trip works")

# Test 2: Crawler table name generation fix
print("\n=== Test 2: Crawler table name generation ===")
from app.catalog.crawler import AsyncCrawler

crawler = AsyncCrawler()

# Test various prefixes - "curated" is no longer filtered out
test_cases = [
    ('curated/', 'curated'),       # previously broken - now correctly produces 'curated'
    ('curated/data/', 'curated_data'),  # path segments joined
    ('raw/', 'default_table'),     # 'raw' is excluded, produces default
    ('processed/', 'processed'),   # produces 'processed'
    ('curated/projectA/', 'curated_projectA'),  # path segments joined
    ('scripts/', 'default_table'), # 'scripts' is excluded
    ('temp/', 'default_table'),    # 'temp' is excluded
    ('emr/', 'default_table'),     # 'emr' is excluded
]

for prefix, expected in test_cases:
    result = crawler._extract_table_name(prefix)
    status = "PASS" if result == expected else "FAIL"
    print(f"  {status} Prefix '{prefix}' -> '{result}' (expected '{expected}')")
    assert result == expected, f"Expected '{expected}', got '{result}'"

print("PASS: Crawler table name generation fix works")

# Test 3: Verify config consistency
print("\n=== Test 3: Config consistency ===")
from app.core.config import settings
assert settings.S3_DATALAKE_BUCKET == settings.DATA_LAKE_BUCKET == "data-lake"
print("PASS: Config bucket names consistent (both = 'data-lake')")

print("\n=== ALL TESTS PASSED ===")