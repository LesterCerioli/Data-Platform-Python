import sys
sys.path.insert(0, r'C:\Users\Lester\repositories\github\enterprise\FastBI-EMR-Glue-Platform')
from app.schemas.emr import StepResponse
field = StepResponse.model_fields['step_params']
print(f'step_params annotation: {field.annotation}')
print(f'step_params default: {field.default}')