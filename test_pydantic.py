import sys
sys.path.insert(0, r'C:\Users\Lester\repositories\github\enterprise\FastBI-EMR-Glue-Platform')
from app.schemas.emr import ClusterDescriptionResponse
from pydantic import TypeAdapter
import typing

# Try to get the type info
ta = TypeAdapter(ClusterDescriptionResponse)
print(f'TypeAdapter created: {ta}')

# Try to generate schema
try:
    schema = ta.json_schema()
    print('JSON schema generated')
except Exception as e:
    print(f'JSON schema error: {e}')