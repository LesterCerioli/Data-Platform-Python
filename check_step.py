import sys
sys.path.insert(0, r'C:\Users\Lester\repositories\github\enterprise\FastBI-EMR-Glue-Platform')
from app.schemas.emr import StepResponse

# Check the step_params field
field_info = StepResponse.model_fields['step_params']
print(f'step_params annotation: {field_info.annotation}')
print(f'step_params default: {field_info.default}')
print(f'step_params is_required: {field_info.is_required()}')