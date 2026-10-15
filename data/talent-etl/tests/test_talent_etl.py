"""
Tests for the ACME Talent ETL job (data/talent-etl/job.py).

  Part 1 - schema validation & data-quality rules   (unit)
  Part 2 - Gold business metrics on tiny tables     (unit, known answers)
  Part 3 - full pipeline on the real sample dataset (end-to-end: fake S3 + CSV-backed PostgreSQL)

Run:  python3 -m pytest data/talent-etl/tests -v
"""
import io
import json
import os
import sys
from pathlib import Path

import boto3
import pandas as pd
import pytest
from moto import mock_aws

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # make job.py importable
import job  # noqa: E402

DATASET_DIR = Path(os.getenv(
    "DATASET_DIR", Path.home() / "dataset" / "data-engineering-performance-talent-management"))
BUCKET = "test-datalake"


def csv_pg_reader(cfg, table):
    """Stands in for PostgreSQL: returns the same CSV that load.sh puts into the database."""
    return pd.read_csv(DATASET_DIR / f"{table}.csv", dtype=str, keep_default_na=False)


@pytest.fixture
def fake_s3(monkeypatch):
    """An in-memory S3 bucket with the learning_activity files uploaded to landing/."""
    for var in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("RETRY_BASE_DELAY", "0")
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        root = DATASET_DIR / "learning_activity"
        for path in root.rglob("*.jsonl"):
            s3.upload_file(str(path), BUCKET, f"landing/learning_activity/{path.relative_to(root).as_posix()}")
        yield s3


@pytest.fixture
def ctx(fake_s3, monkeypatch):
    """A RunContext wired to the fake S3 and the CSV-based PostgreSQL reader."""
    monkeypatch.setenv("DATALAKE_BUCKET", BUCKET)
    monkeypatch.setenv("INGEST_DATE", "2026-10-15")
    cfg = job.Config.from_env()
    return job.RunContext(cfg=cfg, lake=job.DataLake(BUCKET, fake_s3), pg_reader=csv_pg_reader)


def new_run(ctx):
    """A fresh RunContext on the same lake (simulates running the job again)."""
    return job.RunContext(cfg=ctx.cfg, lake=ctx.lake, pg_reader=ctx.pg_reader)


# ======================================================================
# PART 1 - SCHEMA VALIDATION & DATA QUALITY
# ======================================================================

def test_valid_review_passes():
    rows = [{"review_id": "1", "employee_id": "5", "review_period": "2025-H1", "rating": "4",
             "manager_comment": "", "submitted_date": "2026-02-27"}]
    valid, rejected = job.validate_records(rows, job.PerformanceReview)
    assert len(valid) == 1 and not rejected
    assert valid[0]["rating"] == 4 and valid[0]["manager_comment"] is None  # cast + empty -> None


def test_rating_out_of_range_is_rejected():
    rows = [{"review_id": "1", "employee_id": "5", "review_period": "2025-H1", "rating": "6",
             "manager_comment": "x", "submitted_date": "2026-02-27"}]
    valid, rejected = job.validate_records(rows, job.PerformanceReview)
    assert not valid and "rating" in rejected[0]["reject_reason"]


def test_bad_period_format_is_rejected():
    rows = [{"employee_id": "1", "review_cycle": "2025 first half", "potential_rating": "High",
             "promotion_readiness": "Ready Now", "attrition_risk": "Low"}]
    _, rejected = job.validate_records(rows, job.TalentReview)
    assert "review_cycle" in rejected[0]["reject_reason"]


def test_unknown_category_is_rejected():
    _, rejected = job.validate_records([{"competency_id": "1", "name": "X", "category": "Sports"}], job.Competency)
    assert len(rejected) == 1


def test_invalid_email_is_rejected():
    rows = [{"employee_id": "1", "first_name": "A", "last_name": "B", "email": "not-an-email",
             "team_id": "1", "job_level": "IC1", "hire_date": "2024-01-01"}]
    _, rejected = job.validate_records(rows, job.Employee)
    assert "email" in rejected[0]["reject_reason"]


def _event(**overrides):
    base = {"event_id": "EV1", "employee_id": "10", "event_date": "2025-01-10T14:42:00",
            "event_type": "quiz_passed", "course_name": "SQL", "text": "ok", "score": 80}
    return {**base, **overrides}


def test_learning_event_bad_date_rejected():
    for bad in ["yesterday", "N/A", ""]:
        _, rejected = job.validate_records([_event(event_date=bad)], job.LearningEvent)
        assert "event_date" in rejected[0]["reject_reason"], bad


def test_learning_event_missing_employee_rejected():
    _, rejected = job.validate_records([_event(employee_id=None)], job.LearningEvent)
    assert "employee_id" in rejected[0]["reject_reason"]


def test_learning_event_score_out_of_range_rejected():
    _, rejected = job.validate_records([_event(score=150)], job.LearningEvent)
    assert "score" in rejected[0]["reject_reason"]


def test_learning_event_null_score_allowed():
    valid, _ = job.validate_records([_event(score=None, event_type="enrollment")], job.LearningEvent)
    assert valid[0]["score"] is None


def test_parse_malformed_json_line():
    rec, error, _ = job.parse_learning_line('{"event_id": "EV1", "employee_id": "1')
    assert rec is None and error.startswith("malformed_json")


def test_parse_schema_drift_notes_mapped_to_text():
    rec, error, drift = job.parse_learning_line('{"event_id": "EV1", "notes": "hello"}')
    assert error is None and drift is True
    assert rec["text"] == "hello" and "notes" not in rec


def test_config_ssl_only_in_cloud(monkeypatch):
    monkeypatch.setenv("IS_LOCAL", "true")
    assert "sslmode" not in job.Config.from_env().pg_conninfo()
    monkeypatch.setenv("IS_LOCAL", "false")
    assert "sslmode=require" in job.Config.from_env().pg_conninfo()


# ======================================================================
# PART 2 - GOLD BUSINESS METRICS
# ======================================================================

TEAMS = pd.DataFrame({"team_id": [1, 2], "name": ["A", "B"], "department": ["Sales", "Design"]})
EMP = pd.DataFrame({"employee_id": [1, 2, 3], "first_name": ["Ann", "Bob", "Cy"], "last_name": ["X", "Y", "Z"],
                    "email": ["a@x.io"] * 3, "team_id": [1, 1, 2], "job_level": ["IC1"] * 3,
                    "hire_date": pd.to_datetime(["2020-01-01"] * 3).date})
REVIEWS = pd.DataFrame({
    "review_id": [1, 2, 3, 4, 5],
    "employee_id": [1, 1, 2, 2, 3],
    "review_period": ["2024-H2", "2025-H1", "2024-H2", "2025-H1", "2025-H1"],
    "rating": [3, 5, 4, 2, 4],
    "manager_comment": ["ok", "great", "ok", "Missed several key deliverables", "ok"],
    "submitted_date": pd.to_datetime(["2025-01-01"] * 5).date,
})
TALENT = pd.DataFrame({"employee_id": [1, 2, 3], "review_cycle": ["2025-H1"] * 3,
                       "potential_rating": ["High", "Low", "High"],
                       "promotion_readiness": ["Ready Now", "Not Ready", "1-2 Years"],
                       "attrition_risk": ["Low", "High", "Medium"]})


def test_rating_summary_trend():
    s = job.employee_rating_summary(REVIEWS).set_index("employee_id")
    assert s.loc[1, "latest_rating"] == 5 and s.loc[1, "previous_rating"] == 3 and s.loc[1, "rating_trend"] == 2
    assert s.loc[2, "rating_trend"] == -2
    assert pd.isna(s.loc[3, "previous_rating"])  # only one review


def test_review_history_rating_change():
    h = job.gold_employee_review_history(EMP, TEAMS, REVIEWS)
    assert len(h) == len(REVIEWS)
    row = h[(h.employee_id == 1) & (h.review_period == "2025-H1")].iloc[0]
    assert row.rating_change == 2 and row.team_name == "A"


def test_skill_gaps():
    comps = pd.DataFrame({"competency_id": [1], "name": ["SQL"], "category": ["Technical"]})
    assess = pd.DataFrame({"assessment_id": [1, 2, 3], "employee_id": [1, 2, 3], "competency_id": [1, 1, 1],
                           "proficiency_level": [1, 3, 5], "required_level": [4, 4, 3],
                           "assessed_date": pd.to_datetime(["2025-01-01"] * 3).date})
    g = job.gold_skill_gaps(EMP, TEAMS, comps, assess).set_index("employee_id")
    assert g.loc[1, "gap"] == 3 and g.loc[1, "is_critical_gap"]
    assert g.loc[2, "gap"] == 1 and g.loc[2, "has_gap"] and not g.loc[2, "is_critical_gap"]
    assert g.loc[3, "gap"] == -2 and not g.loc[3, "has_gap"]


def test_promotion_ready_only_when_all_conditions_met():
    p = job.gold_promotion_pipeline(EMP, TEAMS, REVIEWS, TALENT).set_index("employee_id")
    assert p.loc[1, "is_promotion_ready"]          # High + Ready Now + latest 5
    assert not p.loc[2, "is_promotion_ready"]      # Low potential
    assert not p.loc[3, "is_promotion_ready"]      # 1-2 Years


def test_attrition_risk_score():
    a = job.gold_attrition_risk(EMP, TEAMS, REVIEWS, TALENT).set_index("employee_id")
    # emp 2: High(2) + declining(1) + latest<=2 (1) + negative comment(1) = 5 -> High
    assert a.loc[2, "risk_score"] == 5 and a.loc[2, "risk_band"] == "High"
    # emp 1: Low(0), improving, latest 5, positive comment = 0 -> Low
    assert a.loc[1, "risk_score"] == 0 and a.loc[1, "risk_band"] == "Low"
    # emp 3: Medium(1) only -> Low band
    assert a.loc[3, "risk_score"] == 1


def test_training_summary_counts_and_zero_fill():
    events = pd.DataFrame({
        "event_id": ["EV1", "EV2", "EV3", "EV4"], "employee_id": [1, 1, 1, 2],
        "event_date": pd.to_datetime(["2025-01-01", "2025-02-01", "2025-03-01", "2025-01-05"]),
        "event_type": ["enrollment", "quiz_passed", "course_completed", "certification_earned"],
        "course_name": ["SQL", "SQL", "SQL", "Cloud"], "text": [None] * 4, "score": [None, 90.0, None, None]})
    t = job.gold_training_summary(EMP, TEAMS, events).set_index("employee_id")
    assert t.loc[1, "courses_completed"] == 1 and t.loc[1, "avg_quiz_score"] == 90.0
    assert t.loc[2, "certifications_earned"] == 1
    assert t.loc[3, "total_events"] == 0          # no activity -> zeros, not missing
    assert len(t) == len(EMP)


def test_team_performance():
    projects = pd.DataFrame({"project_id": [1, 2], "name": ["P1", "P2"], "team_id": [1, 1],
                             "outcome_status": ["on-time", "delayed"], "outcome_score": [80.0, 40.0]})
    t = job.gold_team_performance(EMP, TEAMS, REVIEWS, TALENT, projects).set_index("team_id")
    assert t.loc[1, "headcount"] == 2 and t.loc[1, "avg_latest_rating"] == 3.5
    assert t.loc[1, "pct_projects_on_time"] == 50.0 and t.loc[1, "avg_project_score"] == 60.0
    assert t.loc[2, "project_count"] == 0


def test_skill_distribution_percentages_sum_to_100():
    comps = pd.DataFrame({"competency_id": [1], "name": ["SQL"], "category": ["Technical"]})
    assess = pd.DataFrame({"assessment_id": [1, 2, 3, 4], "employee_id": [1, 2, 3, 1], "competency_id": [1] * 4,
                           "proficiency_level": [1, 1, 3, 5], "required_level": [3] * 4,
                           "assessed_date": pd.to_datetime(["2025-01-01"] * 4).date})
    d = job.gold_skill_distribution(comps, assess)
    assert d["employee_count"].sum() == 4
    assert abs(d["pct_of_competency"].sum() - 100) < 0.5


# ======================================================================
# PART 3 - END-TO-END PIPELINE
# ======================================================================

def test_full_run_succeeds_and_reconciles(ctx):
    assert job.run(ctx) == 0
    assert all(c["passed"] for c in ctx.checks), [c for c in ctx.checks if not c["passed"]]


def test_bronze_counts_match_source(ctx):
    job.ingest_into_bronze(ctx)
    assert ctx.metric("bronze", "employees")["written_rows"] == 2000
    assert ctx.metric("bronze", "performance_reviews")["written_rows"] == 4000
    la = ctx.metric("bronze", "learning_activity")
    assert la["source_files"] == 783 and la["size_mismatches"] == 0
    # partition layout is preserved
    keys = ctx.lake.list_keys("bronze/learning_activity/year=2025/month=01/")
    assert keys and all("/day=" in k for k in keys)


def test_known_data_quality_issues_are_quarantined(ctx):
    job.ingest_into_bronze(ctx)
    job.transform_to_silver(ctx)
    m = ctx.metric("silver", "learning_activity")
    assert m["read"] == 29868
    # 152 duplicated event_ids exist in the raw files; 1 of those pairs is also invalid
    # (rejected before de-duplication), so 151 are removed as duplicates.
    assert m["duplicates_removed"] == 151
    assert m["rejected"] == 143          # 13 malformed JSON + 54 bad dates + 76 missing employee_id
    assert m["valid"] + m["rejected"] + m["duplicates_removed"] == m["read"]
    assert m["schema_drift_mapped"] == 39
    reasons = [json.loads(line)["reject_reason"] for line in
               ctx.lake.get_bytes("quarantine/learning_activity/ingest_date=2026-10-15/rejected.jsonl")
               .decode().splitlines()]
    assert sum(r.startswith("malformed_json") for r in reasons) == 13
    assert any("event_date" in r for r in reasons)
    assert any("employee_id" in r for r in reasons)
    # clean PostgreSQL tables lose nothing
    assert ctx.metric("silver", "employees")["valid"] == 2000


def test_silver_has_no_null_keys_or_duplicates(ctx):
    job.ingest_into_bronze(ctx)
    silver = job.transform_to_silver(ctx)
    ev = silver["learning_activity"]
    assert ev["event_id"].is_unique and ev["employee_id"].notna().all()
    assert str(ev["event_date"].dtype).startswith("datetime64")
    for table, (_, pk, _) in job.POSTGRES_TABLES.items():
        assert silver[table][pk].is_unique, table


def test_rerun_is_idempotent(ctx):
    assert job.run(ctx) == 0
    first = {name: len(ctx.lake.read_parquet(f"gold/{name}/")) for name in
             ["employee_review_history", "skill_gaps", "training_summary"]}
    events_first = len(ctx.lake.read_parquet("silver/learning_activity/"))

    second_ctx = new_run(ctx)
    assert job.run(second_ctx) == 0
    second = {name: len(ctx.lake.read_parquet(f"gold/{name}/")) for name in first}
    assert first == second
    assert len(ctx.lake.read_parquet("silver/learning_activity/")) == events_first
    assert second_ctx.metric("bronze", "learning_activity")["copied"] == 0  # bronze is immutable


def test_lineage_manifest_written(ctx):
    job.run(ctx)
    keys = ctx.lake.list_keys("_lineage/")
    manifest = json.loads(ctx.lake.get_bytes(keys[0]))
    assert manifest["status"] == "SUCCESS" and manifest["duration_seconds"] >= 0
    assert {"bronze", "silver", "gold"} <= {m["layer"] for m in manifest["metrics"]}


def test_failure_is_reported_not_crashing(ctx):
    def broken_reader(cfg, table):
        raise ConnectionError("database down")
    bad = job.RunContext(cfg=ctx.cfg, lake=ctx.lake, pg_reader=broken_reader)
    assert job.run(bad) == 1
    manifest = json.loads(ctx.lake.get_bytes(ctx.lake.list_keys("_lineage/")[0]))
    assert manifest["status"] == "FAILED" and "database down" in manifest["error"]


def test_retry_with_backoff(monkeypatch):
    monkeypatch.setenv("RETRY_ATTEMPTS", "3")
    monkeypatch.setenv("RETRY_BASE_DELAY", "0")
    calls = {"n": 0}

    @job.with_retry
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("transient")
        return "ok"

    assert flaky() == "ok" and calls["n"] == 3


# ======================================================================
# PART 4 - CLOUD PATH: READ AURORA TABLES THROUGH THE BACKEND API
# ======================================================================

class _FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def test_api_reader_sends_key_and_parses_csv(monkeypatch):
    import hashlib
    seen = {}

    def fake_urlopen(request, timeout):
        seen["url"], seen["key"] = request.full_url, request.get_header("X-api-key")
        return _FakeResponse(b'team_id,name,department\n1,"Team A, North",Sales\n')

    monkeypatch.setattr(job.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setenv("POSTGRES_API_URL", "https://example.cloudfront.net/api/talent-source/")
    monkeypatch.setenv("POSTGRES_PASS", "pw")
    df = job.read_postgres_via_api(job.Config.from_env(), "teams")
    assert seen["url"] == "https://example.cloudfront.net/api/talent-source?table=teams"
    assert seen["key"] == hashlib.sha256(b"pw").hexdigest()
    assert df.loc[0, "name"] == "Team A, North" and df.loc[0, "team_id"] == "1"


def test_api_reader_requires_https(monkeypatch):
    monkeypatch.setenv("POSTGRES_API_URL", "http://insecure.example/api")
    with pytest.raises(ValueError):
        job.read_postgres_via_api(job.Config.from_env(), "teams")


def test_full_run_through_api_reader(ctx, monkeypatch):
    """The whole pipeline works when the source tables arrive through the backend API."""
    def fake_urlopen(request, timeout):
        table = request.full_url.split("table=")[1]
        return _FakeResponse((DATASET_DIR / f"{table}.csv").read_bytes())

    monkeypatch.setattr(job.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setenv("POSTGRES_API_URL", "https://example.cloudfront.net/api/talent-source")
    cfg = job.Config.from_env()
    api_ctx = job.RunContext(cfg=cfg, lake=ctx.lake, pg_reader=job.read_postgres_via_api)
    assert job.run(api_ctx) == 0
    assert api_ctx.metric("silver", "employees")["valid"] == 2000
