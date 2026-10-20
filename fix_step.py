import sys
sys.path.insert(0, r'C:\Users\Lester\repositories\github\enterprise\FastBI-EMR-Glue-Platform')
from typing import Any
from app.schemas.emr import StepResponse

# Try to rebuild with Any imported
StepResponse.model_rebuild()
print('StepResponse rebuilt successfully')