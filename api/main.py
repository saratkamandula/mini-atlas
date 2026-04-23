# ─────────────────────────────────────────────────────────────────
# api/main.py
#
# Reads from:   Gold Lakehouse via SQL analytics endpoint
# Exposes:      REST API + GraphQL with API key authentication
# Endpoints:    /health
#               /projects/performance
#               /projects/performance/{project_id}
#               /employees/utilisation
#               /revenue/summary
#               /graphql
# Runs on:      Your laptop via uvicorn
# Written in:   Python — FastAPI + Strawberry GraphQL
# Credentials:  Loaded from .env file (never hardcoded)
# Auth method:  Service Principal — ActiveDirectoryServicePrincipal
# ─────────────────────────────────────────────────────────────────

from strawberry.fastapi import GraphQLRouter
from fastapi import FastAPI, HTTPException, Security
from fastapi.security import APIKeyHeader
import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="mini-ATLAS API",
    description="REST API exposing Gold layer marts from mini-ATLAS",
    version="1.0.0"
)

# ── CONFIG — loaded from .env ─────────────────────────────────────
SERVER        = os.getenv("FABRIC_SERVER")
DATABASE      = os.getenv("FABRIC_DATABASE")
TENANT_ID     = os.getenv("FABRIC_TENANT_ID")
CLIENT_ID     = os.getenv("FABRIC_CLIENT_ID")
CLIENT_SECRET = os.getenv("FABRIC_CLIENT_SECRET")
API_KEY       = os.getenv("API_KEY")
DRIVER        = "ODBC Driver 18 for SQL Server"

# ── AUTH ──────────────────────────────────────────────────────────
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verify_api_key(api_key: str = Security(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key. Pass X-API-Key header."
        )
    return api_key

# ── DATABASE CONNECTION ───────────────────────────────────────────
def get_connection():
    service_principal_id = f"{CLIENT_ID}@{TENANT_ID}"
    conn_str = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER},1433;"
        f"DATABASE={DATABASE};"
        f"UID={service_principal_id};"
        f"PWD={CLIENT_SECRET};"
        "Authentication=ActiveDirectoryServicePrincipal;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "timeout=120;"
    )
    return pyodbc.connect(conn_str)

def query_to_json(sql: str) -> list:
    conn = get_connection()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df.to_dict(orient="records")

# ── ENDPOINTS ─────────────────────────────────────────────────────

@app.get("/health")
def health_check():
    return {"status": "ok", "api": "mini-ATLAS", "version": "1.0.0"}

@app.get("/projects/performance")
def get_project_performance(api_key: str = Security(verify_api_key)):
    """Returns profitability metrics for all projects."""
    sql = """
        SELECT
            project_id,
            project_name,
            client,
            project_type,
            status,
            total_hours,
            total_labour_cost,
            total_revenue,
            profit,
            profit_margin_pct
        FROM mart_project_perf
        ORDER BY profit DESC
    """
    results = query_to_json(sql)
    return {"count": len(results), "data": results}

@app.get("/projects/performance/{project_id}")
def get_project_by_id(
    project_id: str,
    api_key: str = Security(verify_api_key)
):
    """Returns profitability metrics for a single project."""
    sql = f"""
        SELECT *
        FROM mart_project_perf
        WHERE project_id = '{project_id}'
    """
    results = query_to_json(sql)
    if not results:
        raise HTTPException(
            status_code=404,
            detail=f"Project {project_id} not found"
        )
    return results[0]

@app.get("/employees/utilisation")
def get_employee_utilisation(api_key: str = Security(verify_api_key)):
    """Returns utilisation metrics for all employees."""
    sql = """
        SELECT
            employee_id,
            first_name,
            last_name,
            role,
            department,
            total_hours,
            billable_hours,
            non_billable_hours,
            utilisation_pct,
            total_revenue_generated
        FROM mart_utilisation
        ORDER BY utilisation_pct DESC
    """
    results = query_to_json(sql)
    return {"count": len(results), "data": results}

@app.get("/revenue/summary")
def get_revenue_summary(api_key: str = Security(verify_api_key)):
    """Returns invoice and revenue summary per project."""
    sql = """
        SELECT
            project_id,
            project_name,
            project_client,
            total_invoices,
            total_paid,
            total_unpaid,
            total_overdue,
            total_invoiced,
            ISNULL(avg_days_overdue, 0) AS avg_days_overdue
        FROM mart_revenue_summary
        ORDER BY total_overdue DESC
    """
    results = query_to_json(sql)
    return {"count": len(results), "data": results}

# ── GRAPHQL ENDPOINT ─────────────────────────────────────────────
from api.schema import schema
graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql")