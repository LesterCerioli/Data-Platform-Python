from functools import lru_cache

import boto3
from botocore.config import Config

from app.core.config import settings


@lru_cache
def get_dynamodb_resource():
    session = boto3.session.Session(
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        aws_session_token=settings.AWS_SESSION_TOKEN,
        region_name=settings.AWS_REGION,
    )
    return session.resource(
        "dynamodb",
        endpoint_url=settings.AWS_ENDPOINT_URL,
        config=Config(retries={"max_attempts": 3, "mode": "standard"}),
    )


@lru_cache
def get_dynamodb_client():
    session = boto3.session.Session(
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        aws_session_token=settings.AWS_SESSION_TOKEN,
        region_name=settings.AWS_REGION,
    )
    return session.client(
        "dynamodb",
        endpoint_url=settings.AWS_ENDPOINT_URL,
        config=Config(retries={"max_attempts": 3, "mode": "standard"}),
    )