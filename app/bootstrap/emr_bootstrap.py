from app.infrastructure.aws.dynamodb_emr_repository import get_emr_repository
from app.infrastructure.aws.s3_data_lake import get_data_lake_client
from app.services.auth_service import ensure_auth_table_exists


def bootstrap_emr_infrastructure() -> dict[str, str]:
    results: dict[str, str] = {}

    dl = get_data_lake_client()
    dl.ensure_bucket_exists()
    results["data_lake_bucket"] = "ok"

    repo = get_emr_repository()
    repo.ensure_tables_exist()
    results["dynamodb_tables"] = "ok"

    ensure_auth_table_exists()
    results["auth_dynamodb_table"] = "ok"

    return results