# Testing Evidence: talent-etl

All results below were produced on the workshop VDI (Ubuntu, Python 3.13, LocalStack S3, local PostgreSQL)
on 2026-10-15 against the provided sample dataset.

## 1. Automated tests

```sh
python3 -m pip install --user -r data/talent-etl/requirements-dev.txt
python3 -m pytest data/talent-etl/tests -v
```

**Result:** `31 passed in 65.38s` (2 warnings come from the system `pytz` package, not from this project).

| Group | Tests | What is verified |
|---|---|---|
| Schema validation (unit) | 12 | valid row passes and is cast; rating 6 rejected; bad period format; unknown category; bad email; event dates `"yesterday"`/`"N/A"`/`""` rejected; missing employee; score > 100; null score allowed; malformed JSON line; `notes`→`text` drift mapping; SSL only in cloud mode |
| Gold metrics (unit, known answers) | 8 | rating trend; review history change; gap/critical-gap logic; promotion rule needs all 3 conditions; attrition score = 5 / 0 / 1 for hand-made employees; training counts + zero-fill; team averages and % on-time; skill % sums to 100 |
| Cloud path (backend API reader) | 3 | API key + URL sent correctly and CSV parsed (quoted commas); http (non-TLS) URL rejected; **full pipeline run with the source tables arriving through the API** |
| End-to-end (real dataset, fake S3) | 8 | full run succeeds and all reconciliation checks pass; Bronze counts = source and partitions kept; known DQ issues quarantined with exact counts; no null keys / duplicate keys in Silver, dates typed; **idempotent rerun**; lineage manifest written; DB failure → exit 1 + FAILED manifest; retry with backoff |

The end-to-end tests use `moto` (in-memory S3) and read the same CSVs that `load.sh` loads into PostgreSQL,
so they run anywhere without services.

## 2. Functional checks on the real local stack

### 2.1 Source ingestion: Bronze

| Dataset | Source rows/files | Written | Check |
|---|---|---|---|
| teams / competencies / employees / projects | 100 / 40 / 2,000 / 33 | same | ✔ |
| performance_reviews / competency_assessments / talent_review | 4,000 / 4,000 / 2,000 | same | ✔ |
| learning_activity | 783 files | 783 copied, 0 size mismatches | ✔ |

### 2.2 Transformation & data quality: Silver

```
[SILVER] learning_activity  read=29868 valid=29574 rejected=143 duplicates_removed=151 schema_drift_mapped=39
WARNING | Schema drift: 39 learning events used 'notes' instead of 'text' (mapped to 'text')
WARNING | 143 rejected learning_activity rows quarantined to s3://acme-datalake/quarantine/learning_activity/...
```
All 7 PostgreSQL tables: `rejected=0 duplicates_removed=0`.

### 2.3 Aggregation: Gold + reconciliation

All **16/16 reconciliation checks PASS**, e.g.:
```
CHECK silver learning_activity: read = valid + rejected + duplicates   expected=29868 actual=29868 PASS
CHECK gold employee_review_history rows match silver                    expected=4000  actual=4000  PASS
CHECK gold skill_distribution total = silver assessments                expected=4000  actual=4000  PASS
Run ... finished: status=SUCCESS duration=12.10s
```

### 2.4 Idempotency (run the same job twice)

```
[BRONZE] learning_activity  source_files=783 copied=0 already_present=783 size_mismatches=0
[SILVER] learning_activity  read=29868 valid=29574 rejected=143 duplicates_removed=151 schema_drift_mapped=39
Run ... finished: status=SUCCESS duration=7.79s
```
**Conclusion:** the second run copied nothing into Bronze (immutable) and produced identical Silver/Gold counts, so no duplicates.

## 3. Performance

| Metric | Value |
|---|---|
| Records processed per run | ~38,000 (8,173 HR rows + 29,868 events) |
| End-to-end duration (first run / rerun) | 12.1 s / 7.8 s |
| Throughput | ≈ 190,000 records per minute |
| Memory | well under the 2 GiB EKS pod limit (all data < 50 MB in memory) |

Retry/backoff for transient failures is covered by `test_retry_with_backoff` and `test_failure_is_reported_not_crashing`.

## 4. Cloud run (AWS: Aurora + backend Lambda + EKS + S3)

Commands: README section 2 → *Cloud*. Results:

**Aurora loaded through the backend service**
```
$ curl -X POST -H "X-Api-Key: $KEY" "$API?action=seed"
{"seeded": {"teams": 100, "competencies": 40, "employees": 2000, "projects": 33,
            "performance_reviews": 4000, "competency_assessments": 4000, "talent_review": 2000}}
```

**Pipeline on EKS** (`kubectl logs -f job/coding-workshop-talent-etl`)
```
PostgreSQL source: backend API https://d1jp5c2avx0ucs.cloudfront.net/api/talent-source
Run 20261015T092150Z-acbc79 started | bucket=coding-workshop-datalake-c9ade5e5 ingest_date=2026-10-15 is_local=False
[BRONZE] employees                source_rows=2000 written_rows=2000 path=bronze/postgres/employees/...
[BRONZE] learning_activity        source_files=783 copied=0 already_present=783 size_mismatches=0
[SILVER] learning_activity        read=29868 valid=29574 rejected=143 duplicates_removed=151 schema_drift_mapped=39
[GOLD] promotion_pipeline         rows=2000
... 16 x CHECK ... PASS
Run 20261015T092150Z-acbc79 finished: status=SUCCESS duration=48.56s
```

**Conclusion:** the cloud run reproduces the local results exactly (same row counts at every layer, 16/16 checks).

### Troubleshooting history (what was tried, what it showed)

| Attempt | Result | Learned |
|---|---|---|
| First EKS run | `NoCredentialsError` on S3 | Pods get no AWS identity → credentials passed as env vars |
| Direct Aurora from EKS pod, helper pod and VDI | `connection timeout expired` (the job retried 3x with 1 s / 2 s backoff, then `status=FAILED`) | Retry/backoff and failure reporting work; network path blocked |
| Add SG ingress rule for 5432 | `explicit deny` | Participants cannot change Aurora's security group |
| Repo example `backend/python-service` | Timeout first, then `PostgreSQL 17.7` | Backend Lambdas can reach Aurora; Aurora had been auto-paused |
| Job → `backend/talent-source` → Aurora | `status=SUCCESS` | Final cloud design |

## 5. Conclusions

- Every source row is accounted for at every layer (valid + rejected + duplicates = read).
- Invalid data is isolated in `quarantine/` without stopping the pipeline.
- Re-running is safe and produces identical results.
- The Gold layer answers all 7 business questions (see `talent_insights.ipynb`).
- The same code runs in AWS on Aurora + S3 + EKS with identical results.
