"""
ACME Talent ETL job.

Sources : PostgreSQL (7 HR tables) + S3 landing zone (learning_activity JSONL events)
Target  : S3 data lake with a medallion layout

    bronze/      raw, immutable copies of the sources
    silver/      cleaned, typed, validated, de-duplicated (Parquet)
    quarantine/  rows that failed validation, with the reason
    gold/        business-ready tables, one per business question (Parquet)
    _lineage/    one manifest per run: counts, timings, status

The structure follows the workshop skeleton (data/_examples/python-job/job.py):
    ingest_into_bronze()  ->  transform_to_silver()  ->  aggregate_to_gold()

Everything is configured with environment variables, so the same file runs
locally (LocalStack + local PostgreSQL) and in the cloud (EKS + Aurora + S3).
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from functools import wraps
from typing import Any, Callable, Literal, Optional

import boto3
import pandas as pd
import psycopg
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field, ValidationError

# =============================================================================
# 1. LOGGING
# =============================================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("talent-etl")


# =============================================================================
# 2. CONFIGURATION (environment variables only, nothing hardcoded)
# =============================================================================

@dataclass(frozen=True)
class Config:
    """All runtime settings, read from environment variables."""

    is_local: bool
    pg_host: str
    pg_port: int
    pg_name: str
    pg_user: str
    pg_pass: str
    bucket: str
    landing_prefix: str
    ingest_date: str
    retry_attempts: int
    retry_base_delay: float

    @classmethod
    def from_env(cls) -> "Config":
        """Build the config from environment variables (workshop standard names)."""
        return cls(
            is_local=os.getenv("IS_LOCAL", "true").lower() == "true",
            pg_host=os.getenv("POSTGRES_HOST", "localhost"),
            pg_port=int(os.getenv("POSTGRES_PORT", "5432")),
            pg_name=os.getenv("POSTGRES_NAME", "postgres"),
            pg_user=os.getenv("POSTGRES_USER", "postgres"),
            pg_pass=os.getenv("POSTGRES_PASS", ""),
            bucket=os.getenv("DATALAKE_BUCKET", "acme-datalake"),
            landing_prefix=os.getenv("LANDING_PREFIX", "landing/learning_activity").strip("/"),
            ingest_date=os.getenv("INGEST_DATE", date.today().isoformat()),
            retry_attempts=int(os.getenv("RETRY_ATTEMPTS", "3")),
            retry_base_delay=float(os.getenv("RETRY_BASE_DELAY", "1.0")),
        )

    def pg_conninfo(self) -> str:
        """PostgreSQL connection string. Aurora (cloud) requires SSL, local does not."""
        ssl = "" if self.is_local else " sslmode=require"
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_name} "
            f"user={self.pg_user} password={self.pg_pass} connect_timeout=15{ssl}"
        )


# =============================================================================
# 3. RETRY HELPER (exponential backoff for transient S3 / PostgreSQL failures)
# =============================================================================

def with_retry(func: Callable) -> Callable:
    """Retry an I/O function with exponential backoff: 1s, 2s, 4s, ..."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        attempts = int(os.getenv("RETRY_ATTEMPTS", "3"))
        base_delay = float(os.getenv("RETRY_BASE_DELAY", "1.0"))
        for attempt in range(1, attempts + 1):
            try:
                return func(*args, **kwargs)
            except (ClientError, psycopg.OperationalError, ConnectionError, TimeoutError) as exc:
                if attempt == attempts:
                    log.error("%s failed after %d attempts: %s", func.__name__, attempts, exc)
                    raise
                delay = base_delay * (2 ** (attempt - 1))
                log.warning("%s failed (attempt %d/%d): %s - retrying in %.1fs",
                            func.__name__, attempt, attempts, exc, delay)
                time.sleep(delay)
        return None  # unreachable

    return wrapper


# =============================================================================
# 4. I/O: S3 DATA LAKE + POSTGRESQL SOURCE
# =============================================================================

class DataLake:
    """Small wrapper around one S3 bucket. Uses AWS_ENDPOINT_URL for LocalStack."""

    def __init__(self, bucket: str, s3_client: Any = None) -> None:
        self.bucket = bucket
        self.s3 = s3_client or boto3.client(
            "s3",
            endpoint_url=os.getenv("AWS_ENDPOINT_URL") or None,
            config=BotoConfig(s3={"addressing_style": "path"}),
        )

    @with_retry
    def ensure_bucket(self) -> None:
        """Create the bucket if it does not exist yet."""
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except ClientError:
            log.info("Bucket %s not found - creating it", self.bucket)
            self.s3.create_bucket(Bucket=self.bucket)

    @with_retry
    def list_keys(self, prefix: str) -> list[str]:
        """List every object key under a prefix."""
        keys: list[str] = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            keys += [obj["Key"] for obj in page.get("Contents", [])]
        return sorted(keys)

    @with_retry
    def size(self, key: str) -> Optional[int]:
        """Object size in bytes, or None if the object does not exist."""
        try:
            return self.s3.head_object(Bucket=self.bucket, Key=key)["ContentLength"]
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return None
            raise

    @with_retry
    def get_bytes(self, key: str) -> bytes:
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    @with_retry
    def put_bytes(self, key: str, data: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)

    @with_retry
    def copy(self, src_key: str, dst_key: str) -> None:
        self.s3.copy_object(Bucket=self.bucket, Key=dst_key,
                            CopySource={"Bucket": self.bucket, "Key": src_key})

    def delete_prefix(self, prefix: str) -> None:
        """Delete everything under a prefix (used to overwrite a dataset = idempotent)."""
        for key in self.list_keys(prefix):
            self.s3.delete_object(Bucket=self.bucket, Key=key)

    def write_parquet(self, df: pd.DataFrame, key: str) -> None:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        self.put_bytes(key, buf.getvalue())

    def read_parquet(self, prefix: str) -> pd.DataFrame:
        """Read every .parquet file under a prefix into one DataFrame."""
        keys = [k for k in self.list_keys(prefix) if k.endswith(".parquet")]
        if not keys:
            return pd.DataFrame()
        return pd.concat([pd.read_parquet(io.BytesIO(self.get_bytes(k))) for k in keys], ignore_index=True)

    def write_dataset(self, df: pd.DataFrame, prefix: str) -> None:
        """Overwrite a whole dataset folder with one Parquet file."""
        self.delete_prefix(prefix + "/")
        self.write_parquet(df, f"{prefix}/data.parquet")


@with_retry
def read_postgres_table(cfg: Config, table: str) -> pd.DataFrame:
    """Read a full PostgreSQL table into a DataFrame (all values as text, like a CSV export)."""
    with psycopg.connect(cfg.pg_conninfo()) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table} ORDER BY 1")  # table names come from our own constant list
        columns = [c.name for c in cur.description]
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=columns)
    return df.astype(object).where(df.notna(), None).astype(str).replace("None", "")


# =============================================================================
# 5. SCHEMA CONTRACTS (Pydantic) - one model per Silver table
# =============================================================================

PeriodStr = Field(pattern=r"^\d{4}-H[12]$")


class Team(BaseModel):
    team_id: int = Field(gt=0)
    name: str = Field(min_length=1)
    department: str = Field(min_length=1)


class Competency(BaseModel):
    competency_id: int = Field(gt=0)
    name: str = Field(min_length=1)
    category: Literal["Technical", "Leadership", "Domain"]


class Employee(BaseModel):
    employee_id: int = Field(gt=0)
    first_name: str = Field(min_length=1)
    last_name: str = Field(min_length=1)
    email: str = Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    team_id: int = Field(gt=0)
    job_level: str = Field(min_length=1)
    hire_date: date


class Project(BaseModel):
    project_id: int = Field(gt=0)
    name: str = Field(min_length=1)
    team_id: int = Field(gt=0)
    outcome_status: Literal["on-time", "delayed", "at-risk", "cancelled"]
    outcome_score: float = Field(ge=0, le=100)


class PerformanceReview(BaseModel):
    review_id: int = Field(gt=0)
    employee_id: int = Field(gt=0)
    review_period: str = PeriodStr
    rating: int = Field(ge=1, le=5)
    manager_comment: Optional[str] = None
    submitted_date: date


class CompetencyAssessment(BaseModel):
    assessment_id: int = Field(gt=0)
    employee_id: int = Field(gt=0)
    competency_id: int = Field(gt=0)
    proficiency_level: int = Field(ge=1, le=5)
    required_level: int = Field(ge=1, le=5)
    assessed_date: date


class TalentReview(BaseModel):
    employee_id: int = Field(gt=0)
    review_cycle: str = PeriodStr
    potential_rating: Literal["Low", "Medium", "High"]
    promotion_readiness: Literal["Ready Now", "1-2 Years", "Not Ready"]
    attrition_risk: Literal["Low", "Medium", "High"]


class LearningEvent(BaseModel):
    event_id: str = Field(pattern=r"^EV\d+$")
    employee_id: int = Field(gt=0)
    event_date: datetime
    event_type: Literal["enrollment", "module_started", "module_completed", "quiz_attempted",
                        "quiz_passed", "course_completed", "certification_earned"]
    course_name: str = Field(min_length=1)
    text: Optional[str] = None
    score: Optional[float] = Field(default=None, ge=0, le=100)


# table name -> (schema model, primary key, {foreign key column: parent table})
POSTGRES_TABLES: dict[str, tuple[type[BaseModel], str, dict[str, str]]] = {
    "teams": (Team, "team_id", {}),
    "competencies": (Competency, "competency_id", {}),
    "employees": (Employee, "employee_id", {"team_id": "teams"}),
    "projects": (Project, "project_id", {"team_id": "teams"}),
    "performance_reviews": (PerformanceReview, "review_id", {"employee_id": "employees"}),
    "competency_assessments": (CompetencyAssessment, "assessment_id",
                               {"employee_id": "employees", "competency_id": "competencies"}),
    "talent_review": (TalentReview, "employee_id", {"employee_id": "employees"}),
}


def validate_records(records: list[dict], model: type[BaseModel]) -> tuple[list[dict], list[dict]]:
    """Validate each row against a schema. Returns (valid rows, rejected rows + reason)."""
    valid, rejected = [], []
    for rec in records:
        clean = {k: (None if v == "" or (isinstance(v, float) and pd.isna(v)) else v) for k, v in rec.items()}
        try:
            valid.append(model.model_validate(clean).model_dump())
        except ValidationError as exc:
            reason = "; ".join(f"{'.'.join(map(str, e['loc'])) or 'row'}: {e['msg']}" for e in exc.errors())
            rejected.append({"record": rec, "reject_reason": f"schema: {reason}"})
    return valid, rejected


def parse_learning_line(line: str) -> tuple[Optional[dict], Optional[str], bool]:
    """
    Parse one JSONL line.
    Returns (record or None, error reason or None, schema_drift flag).
    Schema drift: some files use 'notes' instead of 'text' - we map it back to 'text'.
    """
    try:
        rec = json.loads(line)
    except json.JSONDecodeError as exc:
        return None, f"malformed_json: {exc.msg}", False
    if not isinstance(rec, dict):
        return None, "malformed_json: not an object", False
    drift = "notes" in rec
    if drift:
        rec["text"] = rec.get("text") or rec.pop("notes")
        rec.pop("notes", None)
    return rec, None, drift


# =============================================================================
# 6. RUN CONTEXT (lineage + metrics)
# =============================================================================

@dataclass
class RunContext:
    """Everything one run needs, plus the lineage metrics it collects."""

    cfg: Config
    lake: DataLake
    pg_reader: Callable[[Config, str], pd.DataFrame] = read_postgres_table
    run_id: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6])
    metrics: list[dict] = field(default_factory=list)
    checks: list[dict] = field(default_factory=list)

    def record(self, layer: str, dataset: str, **counts: Any) -> None:
        """Store and log the row counts for one dataset in one layer."""
        self.metrics.append({"layer": layer, "dataset": dataset, **counts})
        log.info("[%s] %-24s %s", layer.upper(), dataset, " ".join(f"{k}={v}" for k, v in counts.items()))

    def metric(self, layer: str, dataset: str) -> dict:
        return next(m for m in self.metrics if m["layer"] == layer and m["dataset"] == dataset)


def bronze_pg_key(cfg: Config, table: str) -> str:
    return f"bronze/postgres/{table}/ingest_date={cfg.ingest_date}/{table}.csv"


def quarantine_key(cfg: Config, table: str) -> str:
    return f"quarantine/{table}/ingest_date={cfg.ingest_date}/rejected.jsonl"


# =============================================================================
# 7. BRONZE: copy sources as-is into the lake
# =============================================================================

def ingest_into_bronze(ctx: RunContext) -> None:
    """Ingest raw data into the bronze layer (PostgreSQL tables + S3 landing files)."""
    cfg, lake = ctx.cfg, ctx.lake
    lake.ensure_bucket()

    # 7a. PostgreSQL -> CSV snapshot per ingest date (rerun same day = overwrite = no duplicates)
    for table in POSTGRES_TABLES:
        df = ctx.pg_reader(cfg, table)
        if df.empty:
            log.warning("Source table %s returned 0 rows", table)
        key = bronze_pg_key(cfg, table)
        lake.put_bytes(key, df.to_csv(index=False).encode("utf-8"))
        ctx.record("bronze", table, source_rows=len(df), written_rows=len(df), path=key)

    # 7b. S3 landing JSONL -> bronze, keeping the year=/month=/day= partitions.
    #     Bronze is immutable: an existing file is never overwritten.
    landing_keys = [k for k in lake.list_keys(cfg.landing_prefix + "/") if k.endswith(".jsonl")]
    if not landing_keys:
        log.warning("No landing files found under s3://%s/%s/", cfg.bucket, cfg.landing_prefix)
    copied = skipped = size_mismatch = 0
    for src in landing_keys:
        dst = "bronze/learning_activity/" + src[len(cfg.landing_prefix) + 1:]
        src_size, dst_size = lake.size(src), lake.size(dst)
        if dst_size is None:
            lake.copy(src, dst)
            copied += 1
            dst_size = lake.size(dst)
        else:
            skipped += 1
        if dst_size != src_size:
            size_mismatch += 1
            log.error("Bronze file %s size %s differs from source %s (%s)", dst, dst_size, src, src_size)
    ctx.record("bronze", "learning_activity", source_files=len(landing_keys), copied=copied,
               already_present=skipped, size_mismatches=size_mismatch)
    ctx.checks.append({"check": "bronze learning_activity file sizes match landing",
                       "expected": 0, "actual": size_mismatch, "passed": size_mismatch == 0})


# =============================================================================
# 8. SILVER: validate, cast, de-duplicate, quarantine
# =============================================================================

def _write_quarantine(ctx: RunContext, table: str, rejected: list[dict]) -> None:
    key = quarantine_key(ctx.cfg, table)
    ctx.lake.delete_prefix(key)
    if rejected:
        body = "\n".join(json.dumps(r, default=str) for r in rejected)
        ctx.lake.put_bytes(key, body.encode("utf-8"))
        log.warning("%d rejected %s rows quarantined to s3://%s/%s", len(rejected), table, ctx.cfg.bucket, key)


def _apply_foreign_keys(df: pd.DataFrame, fks: dict[str, str], parents: dict[str, pd.DataFrame],
                        rejected: list[dict]) -> pd.DataFrame:
    """Move rows whose foreign key has no parent row into the rejected list."""
    for col, parent in fks.items():
        parent_pk = POSTGRES_TABLES[parent][1]
        orphan = ~df[col].isin(parents[parent][parent_pk])
        for rec in df[orphan].to_dict("records"):
            rejected.append({"record": rec, "reject_reason": f"referential: {col} not found in {parent}"})
        df = df[~orphan]
    return df


def transform_to_silver(ctx: RunContext) -> dict[str, pd.DataFrame]:
    """Transform data from the bronze layer to the silver layer."""
    cfg, lake = ctx.cfg, ctx.lake
    silver: dict[str, pd.DataFrame] = {}

    # 8a. PostgreSQL tables (dict order = parents before children, so FK checks work)
    for table, (model, pk, fks) in POSTGRES_TABLES.items():
        raw = pd.read_csv(io.BytesIO(lake.get_bytes(bronze_pg_key(cfg, table))), dtype=str, keep_default_na=False)
        valid, rejected = validate_records(raw.to_dict("records"), model)
        df = pd.DataFrame(valid, columns=list(model.model_fields))
        before = len(df)
        df = df.drop_duplicates(subset=[pk], keep="last")
        duplicates = before - len(df)
        df = _apply_foreign_keys(df, fks, silver, rejected)
        lake.write_dataset(df, f"silver/{table}")
        _write_quarantine(ctx, table, rejected)
        silver[table] = df
        ctx.record("silver", table, read=len(raw), valid=len(df), rejected=len(rejected),
                   duplicates_removed=duplicates)

    # 8b. Learning activity JSONL (the dirty source)
    records, rejected, drift, lines = [], [], 0, 0
    for key in [k for k in lake.list_keys("bronze/learning_activity/") if k.endswith(".jsonl")]:
        for line in lake.get_bytes(key).decode("utf-8").splitlines():
            if not line.strip():
                continue
            lines += 1
            rec, error, was_drift = parse_learning_line(line)
            if error:
                rejected.append({"record": line, "source_file": key, "reject_reason": error})
                continue
            drift += was_drift
            records.append(rec)
    if drift:
        log.warning("Schema drift: %d learning events used 'notes' instead of 'text' (mapped to 'text')", drift)

    valid, schema_rejected = validate_records(records, LearningEvent)
    rejected += schema_rejected
    events = pd.DataFrame(valid, columns=list(LearningEvent.model_fields))
    before = len(events)
    events = events.sort_values("event_id").drop_duplicates(subset=["event_id"], keep="first")
    duplicates = before - len(events)
    events = _apply_foreign_keys(events, {"employee_id": "employees"}, silver, rejected)
    events["event_date"] = pd.to_datetime(events["event_date"])
    events["event_year"] = events["event_date"].dt.year

    # partitioned by year for cheaper downstream reads
    lake.delete_prefix("silver/learning_activity/")
    for year, part in events.groupby("event_year"):
        lake.write_parquet(part.drop(columns="event_year"), f"silver/learning_activity/event_year={year}/data.parquet")
    _write_quarantine(ctx, "learning_activity", rejected)
    silver["learning_activity"] = events.drop(columns="event_year").reset_index(drop=True)
    ctx.record("silver", "learning_activity", read=lines, valid=len(events), rejected=len(rejected),
               duplicates_removed=duplicates, schema_drift_mapped=drift)

    # reconciliation: every bronze row is either valid, rejected, or a removed duplicate
    for table in list(POSTGRES_TABLES) + ["learning_activity"]:
        m = ctx.metric("silver", table)
        accounted = m["valid"] + m["rejected"] + m["duplicates_removed"]
        ctx.checks.append({"check": f"silver {table}: read = valid + rejected + duplicates",
                           "expected": m["read"], "actual": accounted, "passed": m["read"] == accounted})
    return silver


# =============================================================================
# 9. GOLD: one table per business question (pure functions = easy to unit test)
# =============================================================================

NEGATIVE_COMMENT_PATTERN = r"trended down|Missed|Struggled|needs support"
RISK_POINTS = {"Low": 0, "Medium": 1, "High": 2}


def employee_rating_summary(reviews: pd.DataFrame) -> pd.DataFrame:
    """One row per employee: latest rating, previous rating, trend, average."""
    r = reviews.sort_values(["employee_id", "review_period"])
    grouped = r.groupby("employee_id")
    out = pd.DataFrame({
        "latest_period": grouped["review_period"].last(),
        "latest_rating": grouped["rating"].last(),
        "previous_rating": grouped["rating"].apply(lambda s: s.iloc[-2] if len(s) > 1 else pd.NA),
        "avg_rating": grouped["rating"].mean().round(2),
        "review_count": grouped["rating"].size(),
        "latest_comment": grouped["manager_comment"].last(),
    }).reset_index()
    out["previous_rating"] = pd.to_numeric(out["previous_rating"], errors="coerce")
    out["rating_trend"] = out["latest_rating"] - out["previous_rating"]
    return out


def _employee_base(emp: pd.DataFrame, teams: pd.DataFrame) -> pd.DataFrame:
    base = emp.merge(teams.rename(columns={"name": "team_name"}), on="team_id", how="left")
    base["employee_name"] = base["first_name"] + " " + base["last_name"]
    return base[["employee_id", "employee_name", "job_level", "hire_date", "team_id", "team_name", "department"]]


def gold_employee_review_history(emp, teams, reviews) -> pd.DataFrame:
    """Q1 - rating and review history. Grain: employee x review_period."""
    df = reviews.merge(_employee_base(emp, teams), on="employee_id", how="left")
    df = df.sort_values(["employee_id", "review_period"])
    df["previous_rating"] = df.groupby("employee_id")["rating"].shift(1)
    df["rating_change"] = df["rating"] - df["previous_rating"]
    return df[["employee_id", "employee_name", "team_name", "department", "job_level", "review_period",
               "rating", "previous_rating", "rating_change", "manager_comment", "submitted_date"]].reset_index(drop=True)


def gold_skill_gaps(emp, teams, comps, assessments) -> pd.DataFrame:
    """Q2 - skill gaps. Grain: employee x competency assessment. gap = required - proficiency."""
    df = assessments.merge(comps.rename(columns={"name": "competency_name"}), on="competency_id", how="left")
    df = df.merge(_employee_base(emp, teams), on="employee_id", how="left")
    df["gap"] = df["required_level"] - df["proficiency_level"]
    df["has_gap"] = df["gap"] > 0
    df["is_critical_gap"] = df["gap"] >= 2
    return df[["employee_id", "employee_name", "team_name", "department", "competency_id", "competency_name",
               "category", "proficiency_level", "required_level", "gap", "has_gap", "is_critical_gap",
               "assessed_date"]].reset_index(drop=True)


def gold_promotion_pipeline(emp, teams, reviews, talent) -> pd.DataFrame:
    """Q3 - high-potential employees ready for promotion. Grain: employee."""
    df = talent.merge(employee_rating_summary(reviews), on="employee_id", how="left")
    df = df.merge(_employee_base(emp, teams), on="employee_id", how="left")
    df["is_high_potential"] = df["potential_rating"].eq("High")
    df["is_promotion_ready"] = (df["is_high_potential"] & df["promotion_readiness"].eq("Ready Now")
                                & (df["latest_rating"] >= 4))
    return df[["employee_id", "employee_name", "team_name", "department", "job_level", "potential_rating",
               "promotion_readiness", "latest_rating", "avg_rating", "rating_trend", "is_high_potential",
               "is_promotion_ready"]].sort_values("employee_id").reset_index(drop=True)


def gold_training_summary(emp, teams, events) -> pd.DataFrame:
    """Q4 - training completed per employee. Grain: employee (employees with no activity get zeros)."""
    e = events.copy()
    quiz = e["event_type"].isin(["quiz_attempted", "quiz_passed"])
    agg = e.groupby("employee_id").agg(
        total_events=("event_id", "count"),
        enrollments=("event_type", lambda s: int((s == "enrollment").sum())),
        modules_completed=("event_type", lambda s: int((s == "module_completed").sum())),
        quizzes_passed=("event_type", lambda s: int((s == "quiz_passed").sum())),
        certifications_earned=("event_type", lambda s: int((s == "certification_earned").sum())),
        first_activity=("event_date", "min"),
        last_activity=("event_date", "max"),
    )
    completed = (e[e["event_type"] == "course_completed"].groupby("employee_id")["course_name"]
                 .agg(courses_completed="nunique", completed_courses=lambda s: ", ".join(sorted(set(s)))))
    avg_quiz = e[quiz].groupby("employee_id")["score"].mean().round(1).rename("avg_quiz_score")
    df = _employee_base(emp, teams).merge(agg.join(completed, how="left").join(avg_quiz, how="left"),
                                          left_on="employee_id", right_index=True, how="left")
    for col in ["total_events", "enrollments", "modules_completed", "quizzes_passed",
                "certifications_earned", "courses_completed"]:
        df[col] = df[col].fillna(0).astype(int)
    df["completed_courses"] = df["completed_courses"].fillna("")
    return df.sort_values("employee_id").reset_index(drop=True)


def gold_attrition_risk(emp, teams, reviews, talent) -> pd.DataFrame:
    """
    Q5 - attrition risk from performance trends. Grain: employee.
    risk_score = HR attrition_risk (Low 0 / Medium 1 / High 2)
               + 1 if rating went down + 1 if latest rating <= 2 + 1 if latest comment is negative
    risk_band  = High (>=4), Medium (2-3), Low (0-1)
    """
    df = _employee_base(emp, teams).merge(employee_rating_summary(reviews), on="employee_id", how="left")
    df = df.merge(talent[["employee_id", "attrition_risk", "potential_rating"]], on="employee_id", how="left")
    df["declining_performance"] = df["rating_trend"].fillna(0) < 0
    df["low_latest_rating"] = df["latest_rating"].fillna(5) <= 2
    df["negative_manager_comment"] = df["latest_comment"].fillna("").str.contains(NEGATIVE_COMMENT_PATTERN, regex=True)
    df["risk_score"] = (df["attrition_risk"].map(RISK_POINTS).fillna(0).astype(int)
                        + df["declining_performance"].astype(int)
                        + df["low_latest_rating"].astype(int)
                        + df["negative_manager_comment"].astype(int))
    df["risk_band"] = pd.cut(df["risk_score"], bins=[-1, 1, 3, 10], labels=["Low", "Medium", "High"]).astype(str)
    return df[["employee_id", "employee_name", "team_name", "department", "job_level", "attrition_risk",
               "potential_rating", "previous_rating", "latest_rating", "rating_trend", "declining_performance",
               "low_latest_rating", "negative_manager_comment", "latest_comment", "risk_score",
               "risk_band"]].sort_values("employee_id").reset_index(drop=True)


def gold_team_performance(emp, teams, reviews, talent, projects) -> pd.DataFrame:
    """Q6 - employee performance vs team & project outcomes. Grain: team."""
    people = emp[["employee_id", "team_id"]].merge(employee_rating_summary(reviews), on="employee_id", how="left")
    people = people.merge(talent[["employee_id", "attrition_risk"]], on="employee_id", how="left")
    team_people = people.groupby("team_id").agg(
        headcount=("employee_id", "count"),
        avg_latest_rating=("latest_rating", "mean"),
        pct_high_performers=("latest_rating", lambda s: (s >= 4).mean() * 100),
        pct_high_attrition_risk=("attrition_risk", lambda s: (s == "High").mean() * 100),
    )
    team_projects = projects.groupby("team_id").agg(
        project_count=("project_id", "count"),
        avg_project_score=("outcome_score", "mean"),
        pct_projects_on_time=("outcome_status", lambda s: (s == "on-time").mean() * 100),
    )
    df = teams.rename(columns={"name": "team_name"}).merge(team_people, left_on="team_id", right_index=True, how="left")
    df = df.merge(team_projects, left_on="team_id", right_index=True, how="left")
    df["headcount"] = df["headcount"].fillna(0).astype(int)
    df["project_count"] = df["project_count"].fillna(0).astype(int)
    return df.round(2).sort_values("team_id").reset_index(drop=True)


def gold_skill_distribution(comps, assessments) -> pd.DataFrame:
    """Q7 - distribution of skills. Grain: competency x proficiency_level."""
    df = assessments.merge(comps.rename(columns={"name": "competency_name"}), on="competency_id", how="left")
    out = (df.groupby(["competency_id", "competency_name", "category", "proficiency_level"])
             .size().rename("employee_count").reset_index())
    out["pct_of_competency"] = (out["employee_count"]
                                / out.groupby("competency_id")["employee_count"].transform("sum") * 100).round(1)
    return out


def aggregate_to_gold(ctx: RunContext, silver: Optional[dict[str, pd.DataFrame]] = None) -> dict[str, pd.DataFrame]:
    """Aggregate data from the silver layer to the gold layer."""
    if silver is None:  # gold can also run on its own, reading Silver from S3
        silver = {t: ctx.lake.read_parquet(f"silver/{t}/") for t in list(POSTGRES_TABLES) + ["learning_activity"]}
    emp, teams, comps = silver["employees"], silver["teams"], silver["competencies"]
    reviews, assess, talent = silver["performance_reviews"], silver["competency_assessments"], silver["talent_review"]
    projects, events = silver["projects"], silver["learning_activity"]

    gold = {
        "employee_review_history": gold_employee_review_history(emp, teams, reviews),
        "skill_gaps": gold_skill_gaps(emp, teams, comps, assess),
        "promotion_pipeline": gold_promotion_pipeline(emp, teams, reviews, talent),
        "training_summary": gold_training_summary(emp, teams, events),
        "attrition_risk": gold_attrition_risk(emp, teams, reviews, talent),
        "team_performance": gold_team_performance(emp, teams, reviews, talent, projects),
        "skill_distribution": gold_skill_distribution(comps, assess),
    }
    for name, df in gold.items():
        ctx.lake.write_dataset(df, f"gold/{name}")
        ctx.record("gold", name, rows=len(df))

    # reconciliation: Gold grain and totals must match Silver
    expected = {
        "employee_review_history": len(reviews),
        "skill_gaps": len(assess),
        "promotion_pipeline": len(talent),
        "training_summary": len(emp),
        "attrition_risk": len(emp),
        "team_performance": len(teams),
    }
    for name, exp in expected.items():
        ctx.checks.append({"check": f"gold {name} rows match silver", "expected": exp,
                           "actual": len(gold[name]), "passed": exp == len(gold[name])})
    total = int(gold["skill_distribution"]["employee_count"].sum())
    ctx.checks.append({"check": "gold skill_distribution total = silver assessments", "expected": len(assess),
                       "actual": total, "passed": total == len(assess)})
    return gold


# =============================================================================
# 10. MAIN: run the steps in order, write the lineage manifest, set exit code
# =============================================================================

def write_manifest(ctx: RunContext, status: str, started: datetime, error: Optional[str] = None) -> None:
    ended = datetime.now(timezone.utc)
    manifest = {
        "run_id": ctx.run_id, "status": status, "error": error,
        "started_at": started.isoformat(), "ended_at": ended.isoformat(),
        "duration_seconds": round((ended - started).total_seconds(), 2),
        "ingest_date": ctx.cfg.ingest_date, "bucket": ctx.cfg.bucket,
        "metrics": ctx.metrics, "reconciliation_checks": ctx.checks,
    }
    try:
        ctx.lake.put_bytes(f"_lineage/run_id={ctx.run_id}/manifest.json",
                           json.dumps(manifest, indent=2, default=str).encode("utf-8"))
    except Exception as exc:  # the manifest must never hide the real error
        log.error("Could not write lineage manifest: %s", exc)
    log.info("Run %s finished: status=%s duration=%.2fs", ctx.run_id, status, manifest["duration_seconds"])


def run(ctx: RunContext) -> int:
    """Run the full pipeline. Returns 0 on success, 1 on failure."""
    started = datetime.now(timezone.utc)
    log.info("Run %s started | bucket=%s ingest_date=%s is_local=%s",
             ctx.run_id, ctx.cfg.bucket, ctx.cfg.ingest_date, ctx.cfg.is_local)
    try:
        ingest_into_bronze(ctx)
        silver = transform_to_silver(ctx)
        aggregate_to_gold(ctx, silver)
        failed = [c for c in ctx.checks if not c["passed"]]
        for c in ctx.checks:
            log.log(logging.INFO if c["passed"] else logging.ERROR, "CHECK %-55s expected=%s actual=%s %s",
                    c["check"], c["expected"], c["actual"], "PASS" if c["passed"] else "FAIL")
        if failed:
            write_manifest(ctx, "FAILED", started, f"{len(failed)} reconciliation check(s) failed")
            return 1
        write_manifest(ctx, "SUCCESS", started)
        return 0
    except Exception as exc:
        log.exception("Pipeline failed: %s", exc)
        write_manifest(ctx, "FAILED", started, str(exc))
        return 1


def main() -> None:
    """Entry point (same name as the workshop skeleton)."""
    cfg = Config.from_env()
    sys.exit(run(RunContext(cfg=cfg, lake=DataLake(cfg.bucket))))


if __name__ == "__main__":
    main()
