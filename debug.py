import sys
sys.path.insert(0, r'C:\Users\Lester\repositories\github\enterprise\FastBI-EMR-Glue-Platform')
from app.main import app
print(f'API version: {app.version}')
print(f'OpenAPI version: {app.openapi_version}')