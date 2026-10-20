import os
import base64
import hmac
import jwt as pyjwt
from datetime import datetime, timedelta, timezone
import boto3
from botocore.exceptions import ClientError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi import Depends, Header, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional, Dict, Any, List

from config import settings



_PRIVATE_KEY_CACHE: Optional[bytes] = None
_PUBLIC_KEY_CACHE: Optional[bytes] = None


def _get_private_key() -> bytes:
    global _PRIVATE_KEY_CACHE
    if _PRIVATE_KEY_CACHE is None:
        private_key_b64 = os.environ.get("AUTH_PRIVATE_KEY_BASE64", "")
        if private_key_b64:
            key_bytes = base64.b64decode(private_key_b64)
            private_key = Ed25519PrivateKey.from_private_bytes(key_bytes)
            _PRIVATE_KEY_CACHE = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        else:
            private_key = Ed25519PrivateKey.generate()
            _PRIVATE_KEY_CACHE = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            os.environ["AUTH_PRIVATE_KEY_BASE64"] = base64.b64encode(
                private_key.private_bytes(
                    encoding=serialization.Encoding.Raw,
                    format=serialization.PrivateFormat.Raw,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            ).decode()
            os.environ["AUTH_PUBLIC_KEY_BASE64"] = base64.b64encode(
                private_key.public_key().public_bytes(
                    encoding=serialization.Encoding.Raw,
                    format=serialization.PublicFormat.Raw,
                )
            ).decode()
    return _PRIVATE_KEY_CACHE


def _get_public_key() -> bytes:
    global _PUBLIC_KEY_CACHE
    if _PUBLIC_KEY_CACHE is None:
        public_key_b64 = os.environ.get("AUTH_PUBLIC_KEY_BASE64", "")
        if public_key_b64:
            _PUBLIC_KEY_CACHE = base64.b64decode(public_key_b64)
        else:
            _PUBLIC_KEY_CACHE = _get_private_key()
    return _PUBLIC_KEY_CACHE


CLIENT_ID_LENGTH = 16
CLIENT_SECRET_LENGTH = 16

ALLOWED_CLIENT_ID = settings.AUTH_CLIENT_ID
ALLOWED_CLIENT_SECRET = settings.AUTH_CLIENT_SECRET


if len(ALLOWED_CLIENT_ID) != CLIENT_ID_LENGTH:
    raise ValueError(
        f"AUTH_CLIENT_ID should have exactly {CLIENT_ID_LENGTH} characteres. "
        f"Received: {len(ALLOWED_CLIENT_ID)} characteres."
    )
if len(ALLOWED_CLIENT_SECRET) != CLIENT_SECRET_LENGTH:
    raise ValueError(
        f"AUTH_CLIENT_SECRET should have exactly {CLIENT_SECRET_LENGTH} characteres. "
        f"Received: {len(ALLOWED_CLIENT_SECRET)} characteres."
    )

JWT_ISSUER = os.environ.get("JWT_ISSUER", "fastbi-api")
JWT_AUDIENCE = os.environ.get("JWT_AUDIENCE", "fastbi-client")
TOKEN_EXPIRATION_SECONDS = int(os.environ.get("TOKEN_EXPIRATION_SECONDS", "120"))

DYNAMO_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_AUTH", "FASTI_auth_tokens")



_dynamo_client = None


def _get_dynamo_client():
    global _dynamo_client
    if _dynamo_client is None:
        _dynamo_client = boto3.client(
            "dynamodb",
            endpoint_url=settings.LOCALSTACK_ENDPOINT,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION,
        )
    return _dynamo_client



def authenticate_client(client_id: str, client_secret: str) -> bool:
    
    if not client_id or not client_secret:
        return False

    
    if len(client_id) != CLIENT_ID_LENGTH:
        return False
    if len(client_secret) != CLIENT_SECRET_LENGTH:
        return False

    
    return hmac.compare_digest(client_id, ALLOWED_CLIENT_ID) and \
           hmac.compare_digest(client_secret, ALLOWED_CLIENT_SECRET)


def issue_jwt(subject: str = "fastbi-client") -> str:
    
    now = datetime.now(timezone.utc)
    exp = now + timedelta(seconds=TOKEN_EXPIRATION_SECONDS)

    payload = {
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "sub": subject,
        "iat": now,
        "exp": exp,
        "jti": f"{subject}-{int(now.timestamp())}",
    }

    private_key = _get_private_key()
    token = pyjwt.encode(
        payload,
        private_key,
        algorithm="EdDSA",
    )
    return token


bearer_scheme = HTTPBearer(auto_error=False)


def validate_jwt(token: str) -> Dict[str, Any]:
    
    public_key = _get_public_key()

    payload = pyjwt.decode(
        token,
        public_key,
        algorithms=["EdDSA"],
        issuer=JWT_ISSUER,
        audience=JWT_AUDIENCE,
    )
    
    jti = payload.get("jti")
    if jti:
        from app.services.auth_service import is_token_revoked
        if is_token_revoked(jti):
            raise pyjwt.InvalidTokenError("Token has been revoked")

    return payload



def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> Dict[str, Any]:
    
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authorization token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials

    try:
        payload = validate_jwt(token)
    except pyjwt.InvalidTokenError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        )

    
    subject = payload.get("sub")
    if not subject:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing required 'sub' claim",
        )

    return payload


def is_token_revoked(jti: str) -> bool:
    
    try:
        dynamo = _get_dynamo_client()
        response = dynamo.get_item(
            TableName=DYNAMO_TABLE_NAME,
            Key={"token_id": {"S": jti}},
        )
        if "Item" in response:
            is_active = response["Item"].get("is_active", {"BOOL": True})["BOOL"]
            return is_active == False
        
        return False
    except ClientError:
        
        return False


def revoke_token(jti: str, active: bool = False) -> None:
    
    try:
        dynamo = _get_dynamo_client()
        dynamo.put_item(
            TableName=DYNAMO_TABLE_NAME,
            Item={
                "token_id": {"S": jti},
                "is_active": {"BOOL": active},
                "revoked_at": {"S": datetime.utcnow().isoformat()},
            },
        )
    except ClientError as e:
        raise RuntimeError(f"Failed to revoke token in DynamoDB: {e}")



def ensure_auth_table_exists() -> None:
    
    dynamo = _get_dynamo_client()
    try:
        dynamo.describe_table(TableName=DYNAMO_TABLE_NAME)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            dynamo.create_table(
                TableName=DYNAMO_TABLE_NAME,
                KeySchema=[{"AttributeName": "token_id", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "token_id", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            dynamo.get_waiter("table_exists").wait(TableName=DYNAMO_TABLE_NAME)
        else:
            raise