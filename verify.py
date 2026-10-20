from app.services.auth_service import authenticate_client, issue_jwt, get_current_user, validate_jwt, is_token_revoked, revoke_token, ensure_auth_table_exists
print('auth_service imports OK')

from app.config import settings
print('config imports OK')

from app.main import app
print('main app import OK')

from fastapi.testclient import TestClient
client = TestClient(app)
print('TestClient created OK')

# Test the token endpoint
response = client.post(
    "/auth/token",
    data={"client_id": "fastbi-client", "client_secret": "fastbi-secret-2024"}
)
print(f"Token endpoint status: {response.status_code}")
print(f"Token endpoint response: {response.json()}")

# Test that the token can be used to access a protected endpoint
if "access_token" in response.json():
    token = response.json()["access_token"]
    response2 = client.get(
        "/api/v1/emr/clusters",
        headers={"Authorization": f"Bearer {token}"}
    )
    print(f"Protected endpoint status: {response2.status_code}")
    print(f"Protected endpoint response: {response2.json()}")