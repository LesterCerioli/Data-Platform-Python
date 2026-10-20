import sys
sys.path.insert(0, r'C:\Users\Lester\repositories\github\enterprise\FastBI-EMR-Glue-Platform')
from typing import Any
from app.schemas.emr import ClusterDescriptionResponse, StepResponse
StepResponse.model_rebuild()
ClusterDescriptionResponse.model_rebuild()
print('Both models rebuilt successfully')

from fastapi.openapi.utils import get_openapi
from app.main import app
schema = get_openapi(title=app.title, version=app.version, openapi_version=app.openapi_version, routes=app.routes)
print('Schema generated successfully')
print(f'Paths: {list(schema.paths.keys())}')