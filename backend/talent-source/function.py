"""
talent-source: backend service that owns ACME's HR source tables in Aurora PostgreSQL.

Why a backend service?
    In the workshop VPC only resources inside Aurora's security group can reach it.
    Backend Lambdas run in that group (infra/lambda.tf); EKS nodes do not. So this
    service is the network path between Aurora and the data/talent-etl EKS job.

Routes (through CloudFront: https://<distribution>/api/talent-source):
    GET  ?action=health   -> row count per table
    POST ?action=seed     -> (re)create the 7 tables and load the bundled seed CSVs
    GET  ?table=<name>    -> the whole table as CSV (used by Bronze ingestion)

Every request needs the header  X-Api-Key: sha256(POSTGRES_PASS)
(the Lambda and the EKS job both already receive POSTGRES_PASS, so no new secret).
"""

import hashlib
import hmac
import json
import logging
import os
import time
from pathlib import Path

from psycopg import OperationalError, connect

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Load order matters: parents before children (foreign keys)
TABLES = ["teams", "competencies", "employees", "projects",
          "performance_reviews", "competency_assessments", "talent_review"]
SEED_DIR = Path(__file__).parent / "seed"


def _conninfo() -> str:
    """Connection string from the injected environment variables. Aurora needs SSL."""
    ssl = "" if os.getenv("IS_LOCAL", "false") == "true" else " sslmode=require"
    return (f"host={os.getenv('POSTGRES_HOST', 'localhost')} port={os.getenv('POSTGRES_PORT', '5432')} "
            f"dbname={os.getenv('POSTGRES_NAME', 'postgres')} user={os.getenv('POSTGRES_USER', 'postgres')} "
            f"password={os.getenv('POSTGRES_PASS', '')} connect_timeout=60{ssl}")


def _connect(attempts: int = 3):
    """Connect with retries: Aurora Serverless v2 may be auto-paused and needs ~15s to resume."""
    for attempt in range(1, attempts + 1):
        try:
            return connect(_conninfo())
        except OperationalError as exc:
            if attempt == attempts:
                raise
            logger.warning("Aurora connect failed (attempt %d/%d): %s", attempt, attempts, exc)
            time.sleep(2 ** attempt)
    return None


def _expected_key() -> str:
    password = os.getenv("POSTGRES_PASS", "")
    return hashlib.sha256(password.encode()).hexdigest() if password else ""


def _response(status: int, body, content_type: str = "application/json") -> dict:
    if not isinstance(body, str):
        body = json.dumps(body)
    return {"statusCode": status, "headers": {"Content-Type": content_type}, "body": body}


def seed() -> dict:
    """Drop and recreate the source tables, then load the bundled CSVs (idempotent)."""
    counts = {}
    with _connect() as conn, conn.cursor() as cur:
        for table in reversed(TABLES):
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")  # nosec B608 - table from fixed list
        cur.execute((SEED_DIR / "schema.sql").read_text())
        for table in TABLES:
            with cur.copy(f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER true)") as copy:
                copy.write((SEED_DIR / f"{table}.csv").read_bytes())
            cur.execute(f"SELECT count(*) FROM {table}")  # nosec B608 - table from fixed list
            counts[table] = cur.fetchone()[0]
            logger.info("Seeded %s: %d rows", table, counts[table])
    return counts


def health() -> dict:
    """Row count per table (or the error if a table is missing)."""
    counts = {}
    with _connect() as conn, conn.cursor() as cur:
        for table in TABLES:
            try:
                cur.execute(f"SELECT count(*) FROM {table}")  # nosec B608 - table from fixed list
                counts[table] = cur.fetchone()[0]
            except Exception as exc:  # table not seeded yet
                conn.rollback()
                counts[table] = f"missing ({type(exc).__name__})"
    return counts


def export_table(table: str) -> str:
    """Whole table as CSV with header, ordered by primary key."""
    with _connect() as conn, conn.cursor() as cur:
        chunks = []
        with cur.copy(f"COPY (SELECT * FROM {table} ORDER BY 1) TO STDOUT WITH (FORMAT csv, HEADER true)") as copy:
            for data in copy:
                chunks.append(bytes(data))
    return b"".join(chunks).decode("utf-8")


def handler(event=None, context=None):
    """Lambda entry point (Lambda Function URL, payload v2)."""
    event = event or {}
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    params = event.get("queryStringParameters") or {}
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")

    expected = _expected_key()
    if not expected or not hmac.compare_digest(headers.get("x-api-key", ""), expected):
        return _response(401, {"error": "missing or invalid X-Api-Key"})

    try:
        action = params.get("action")
        table = params.get("table")
        if action == "seed" and method == "POST":
            return _response(200, {"seeded": seed()})
        if action == "health":
            return _response(200, {"tables": health()})
        if table:
            if table not in TABLES:
                return _response(400, {"error": f"unknown table '{table}'", "allowed": TABLES})
            csv_text = export_table(table)
            logger.info("Exported %s: %d bytes", table, len(csv_text))
            return _response(200, csv_text, "text/csv")
        return _response(400, {"error": "use ?action=health, POST ?action=seed, or ?table=<name>", "tables": TABLES})
    except Exception as exc:
        logger.exception("talent-source error")
        return _response(500, {"error": type(exc).__name__, "message": str(exc)})


if __name__ == "__main__":
    print(handler({"queryStringParameters": {"action": "health"},
                   "headers": {"X-Api-Key": _expected_key()}}))
