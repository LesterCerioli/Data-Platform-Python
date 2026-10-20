import sys
sys.path.insert(0, r'C:\Users\Lester\repositories\github\enterprise\FastBI-EMR-Glue-Platform')

from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

# Test with JSON body in request body - this should work now
response = client.post("/auth/token", json={"client_id": "jholgJ1hEcb5Fty2", "client_secret": "w1VvvSax1vUcED6s"})
print(f"JSON body status: {response.status_code}")
print(f"JSON body response: {response.json()}")