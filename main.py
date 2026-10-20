from contextlib import asynccontextmanager
from fastapi import FastAPI, Body, Header, HTTPException, status
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

from app.catalog.crawler import AsyncCrawler
from app.catalog.dynamo_store import AsyncDynamoCatalog
from app.bootstrap.emr_bootstrap import bootstrap_emr_infrastructure
from app.api.v1.routes import emr
from app.services.auth_service import authenticate_client, issue_jwt
from config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize catalog (S3 + DynamoDB) and auth table
    bootstrap_emr_infrastructure()
    from app.services.auth_service import ensure_auth_table_exists
    ensure_auth_table_exists()
    yield


app = FastAPI(title="FastBI Data Platform — Glue + EMR Próprio", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include EMR routes
app.include_router(emr.router, prefix="/api/v1")


class TokenRequest(BaseModel):
    client_id: str
    client_secret: str


@app.post("/auth/token", summary="Obter token JWT")
async def authenticate_for_token(
    credentials: TokenRequest = Body(...),
):
    """Authenticate client credentials and issue a JWT token.

    Requires `client_id` and `client_secret` in request body as JSON.
    Returns a signed JWT valid for 120 seconds.
    """
    if not authenticate_client(credentials.client_id, credentials.client_secret):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid client credentials",
        )

    token = issue_jwt(subject=credentials.client_id)
    return {"access_token": token, "token_type": "bearer"}


@app.get("/")
async def root():
    return {
        "service": "FastBI Data Platform",
        "version": "1.0.0",
        "description": "Nosso Glue (catalog) + EMR (compute) em Python — independente do FastBI (Go)",
        "timestamp": datetime.utcnow().isoformat(),
    }


# Catalog endpoints (Glue-like)
@app.post("/catalog/scan", summary="Inicia escaneamento do Data Lake")
async def start_scan(prefix: str = "curated/"):
    """Dispara escaneamento — retorna imediatamente, roda em background"""
    return await crawler.start_scan(prefix)


@app.get("/catalog/scan/{task_id}", summary="Status do escaneamento")
async def scan_status(task_id: str):
    task = crawler.get_task(task_id)
    if not task:
        # Try from DynamoDB
        task = await catalog.get_task(task_id)
    if not task:
        return {"error": "Tarefa não encontrada"}
    return task


@app.get("/catalog/tables", summary="Lista todas as tabelas")
async def list_tables(search: str = None):
    return await (catalog.search_tables(search) if search else catalog.list_all_tables())


@app.get("/catalog/tables/{table_name}", summary="Detalhes da tabela")
async def get_table(table_name: str):
    t = await catalog.get_table_by_name(table_name)
    return t or {"error": "Não encontrada"}


# EMR endpoints are at /api/v1/emr/*
# POST   /api/v1/emr/clusters              - Create cluster
# GET    /api/v1/emr/clusters              - List clusters
# GET    /api/v1/emr/clusters/{id}         - Describe cluster
# POST   /api/v1/emr/clusters/{id}/terminate - Terminate cluster
# POST   /api/v1/emr/clusters/{id}/steps   - Add Spark step
# GET    /api/v1/emr/clusters/{id}/steps   - List steps
# GET    /api/v1/emr/steps/{id}            - Describe step
# POST   /api/v1/emr/steps/{id}/cancel     - Cancel step