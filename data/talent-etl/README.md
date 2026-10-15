# talent-etl: ACME Performance & Talent Data Pipeline

A batch ETL pipeline that ingests ACME's employee performance, competency, talent-review and
learning data from **PostgreSQL** and **S3** into a **medallion data lake** (Bronze → Silver → Gold)
and answers 7 workforce questions in a **Jupyter notebook**.

| File | Purpose |
|---|---|
| `job.py` | The pipeline: `ingest_into_bronze()` → `transform_to_silver()` → `aggregate_to_gold()` (same structure as the workshop skeleton) |
| `requirements.txt` | Runtime libraries (installed automatically in the EKS job pod) |
| `requirements-dev.txt` | Runtime + test libraries |
| `tests/test_talent_etl.py` | 31 unit + end-to-end tests |
| `talent_insights.ipynb` | Business-facing notebook: answers to the 7 questions with charts |
| `TESTING.md` | Test commands, results and conclusions (local + cloud) |
| `../../backend/talent-source/` | Backend Lambda that loads the HR tables into **Aurora** and serves them to the cloud job (see section 8) |

---

## 1. Architecture

```
 SOURCES                        S3 DATA LAKE  (bucket = $DATALAKE_BUCKET)                        CONSUMERS
 ┌──────────────────────┐       ┌────────────┐   ┌──────────────────┐   ┌──────────────────┐
 │ PostgreSQL (7 tables)│──────►│  bronze/   │──►│     silver/      │──►│      gold/       │──► Jupyter notebook
 └──────────────────────┘       │ raw, as-is │   │ typed, validated │   │ 1 table per      │
 ┌──────────────────────┐       │ immutable  │   │ de-duplicated    │   │ business question│
 │ S3 landing/ (JSONL)  │──────►│            │   │                  │   │                  │
 └──────────────────────┘       └────────────┘   └───────┬──────────┘   └──────────────────┘
                                                         │ invalid rows
                                                         ▼
                                                  quarantine/ (row + reason)       _lineage/ (run manifest)
```

**Separation of concerns:** ingestion (Bronze) only copies, transformation (Silver) only cleans and
validates, serving (Gold) only aggregates. Each layer reads only from the layer before it.

### Storage layout (path contract)

| Layer | Path | Format | Write mode |
|---|---|---|---|
| Landing (source) | `landing/learning_activity/year=YYYY/month=MM/day=DD/events-000.jsonl` | JSONL | external |
| Bronze (Postgres) | `bronze/postgres/<table>/ingest_date=YYYY-MM-DD/<table>.csv` | CSV snapshot | overwrite same-day partition |
| Bronze (events) | `bronze/learning_activity/year=YYYY/month=MM/day=DD/events-000.jsonl` | JSONL, byte-identical | **immutable**: never overwritten |
| Silver | `silver/<table>/data.parquet` | Parquet | overwrite dataset |
| Silver (events) | `silver/learning_activity/event_year=YYYY/data.parquet` | Parquet, partitioned by year | overwrite dataset |
| Quarantine | `quarantine/<table>/ingest_date=YYYY-MM-DD/rejected.jsonl` | JSONL (`record` + `reject_reason`) | overwrite partition |
| Gold | `gold/<dataset>/data.parquet` | Parquet | overwrite dataset |
| Lineage | `_lineage/run_id=<run_id>/manifest.json` | JSON | one per run |

**Partitioning choices:** the source date partitions are preserved in Bronze (cheap re-processing of a
single day). Silver events are partitioned by year (readers usually filter by period). Gold tables are
small (≤ 4,000 rows), so one file each is cheapest to read.

---

## 2. How to run

### Local (LocalStack S3 + local PostgreSQL)

```sh
# 0. Start local services (keep this terminal open)
./bin/start-dev.sh

# 1. Load the source data (once)
cd ~/dataset/data-engineering-performance-talent-management
IS_LOCAL=true POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_NAME=postgres \
POSTGRES_USER=postgres POSTGRES_PASS=postgres123 bash load.sh

export AWS_ENDPOINT_URL=http://localhost.localstack.cloud:4566 AWS_ACCESS_KEY_ID=test \
       AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1
aws s3 mb s3://acme-datalake
aws s3 cp --recursive --quiet learning_activity s3://acme-datalake/landing/learning_activity/

# 2. Install and run the pipeline
cd ~/coding-workshop-participant
python3 -m pip install --user -r data/talent-etl/requirements-dev.txt
export IS_LOCAL=true POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_NAME=postgres \
       POSTGRES_USER=postgres POSTGRES_PASS=postgres123 DATALAKE_BUCKET=acme-datalake
python3 data/talent-etl/job.py 2>&1 | tee /tmp/talent-etl.log

# 3. Tests
python3 -m pytest data/talent-etl/tests -v

# 4. Notebook
python3 -m jupyter notebook data/talent-etl/talent_insights.ipynb
```

### Cloud (EKS + Aurora PostgreSQL + S3)

Use a **fresh terminal** (no LocalStack variables), from the repo root:

```sh
# 0. AWS login + EKS on
./bin/setup-participant.sh && source ENVIRONMENT.config
unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3
echo "export TF_VAR_aws_eks_enabled=true" >> ~/.bashrc && source ~/.bashrc

# 1. Deploy: data-lake bucket, Aurora, backend Lambdas, EKS + the talent-etl Kubernetes Job
./bin/deploy-backend.sh

# 2. S3 source: upload the learning_activity files to the cloud landing zone
aws s3 cp --recursive ~/dataset/data-engineering-performance-talent-management/learning_activity \
    s3://$(cd infra && terraform output -raw datalake_bucket)/landing/learning_activity/

# 3. PostgreSQL source: load the 7 HR tables into Aurora through the backend service
KEY=$(printf %s "$(cd infra && terraform output -raw postgres_pass)" | sha256sum | cut -d' ' -f1)
API=https://$(cd infra && terraform output -raw cloudfront_distribution_url)/api/talent-source
curl -s -X POST -H "X-Api-Key: $KEY" "$API?action=seed"     # {"seeded": {"teams": 100, ...}}
curl -s -H "X-Api-Key: $KEY" "$API?action=health"            # row count per table

# 4. Run the pipeline (re-create the job now that the sources are loaded) and watch it
kubectl delete job coding-workshop-talent-etl --ignore-not-found
./bin/deploy-backend.sh
kubectl logs -f job/coding-workshop-talent-etl   # job name = <helm chart name>-<folder name>
```

The same `job.py` runs locally and in the cloud with **no code changes**. Only environment variables differ.

### Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `IS_LOCAL` | `true` | `false` → PostgreSQL connection uses `sslmode=require` (Aurora) |
| `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_NAME` / `POSTGRES_USER` / `POSTGRES_PASS` | `localhost` / `5432` / `postgres` / `postgres` / *(empty)* | Source database |
| `DATALAKE_BUCKET` | `acme-datalake` | S3 bucket for all layers |
| `AWS_ENDPOINT_URL` | *(unset = real AWS)* | Set to LocalStack URL for local runs |
| `LANDING_PREFIX` | `landing/learning_activity` | Where the S3 source files arrive |
| `INGEST_DATE` | today | Bronze/quarantine partition date (set it to re-run a past day) |
| `POSTGRES_API_URL` | *(unset = connect directly)* | Cloud only: read the source tables through `backend/talent-source` instead of a direct connection (set by `infra/eks.tf`) |
| `RETRY_ATTEMPTS` / `RETRY_BASE_DELAY` | `3` / `1.0` | Exponential backoff for S3/PostgreSQL calls (1s, 2s, 4s …) |
| `LOG_LEVEL` | `INFO` | Logging level |

---

## 3. Schema contracts & data quality

Every Silver table has a **Pydantic model** in `job.py` (section 5). A row that fails validation is
**not dropped silently and does not fail the pipeline**: it is written to `quarantine/` with the reason.

| Rule type | Examples |
|---|---|
| Required / non-null | all primary keys, `employee_id`, dates |
| Type casting | text → `int`, `float`, `date`, `datetime` |
| Domain constraints | `rating` 1–5, `proficiency_level` 1–5, `outcome_score` 0–100, quiz `score` 0–100 |
| Allowed values | `category ∈ {Technical, Leadership, Domain}`, `attrition_risk ∈ {Low, Medium, High}`, 7 event types … |
| Format | `review_period` = `YYYY-H1/H2`, email pattern, `event_id` = `EV<digits>` |
| De-duplication | by primary key (events: by `event_id`) |
| Referential integrity | `employee_id` must exist in `employees`, `team_id` in `teams`, `competency_id` in `competencies` |

### Data-quality issues found in the sample data

The 7 PostgreSQL tables were clean. The **learning_activity** JSONL files (29,868 lines) contained:

| Issue | Rows | Handling |
|---|---|---|
| Truncated / malformed JSON lines | 13 | quarantined (`malformed_json`) |
| Unparseable `event_date` (`"yesterday"`, `"N/A"`, `""`) | 54 | quarantined (`schema: event_date …`) |
| Missing `employee_id` | 76 | quarantined (`schema: employee_id …`) |
| Duplicate `event_id` | 151 removed | kept first occurrence |
| **Schema drift**: field `text` arrives as `notes` | 39 | mapped back to `text`, WARN logged, row kept |

Result: **29,574 clean events** (29,574 valid + 143 rejected + 151 duplicates = 29,868 read ✔).

---

## 4. Gold datasets (one per business question)

| # | Question | Gold dataset | Grain | Key logic |
|---|---|---|---|---|
| 1 | Ratings & review history | `employee_review_history` | employee × review_period | previous rating, rating change |
| 2 | Critical skill gaps | `skill_gaps` | employee × assessment | `gap = required − proficiency`; critical if `gap ≥ 2` |
| 3 | High-potential, ready for promotion | `promotion_pipeline` | employee | potential = High AND readiness = Ready Now AND latest rating ≥ 4 |
| 4 | Training completed | `training_summary` | employee | enrollments, modules, courses completed (+names), quizzes, avg score, certifications |
| 5 | Attrition risk | `attrition_risk` | employee | score = HR risk (0/1/2) + declining rating + latest ≤ 2 + negative comment; band High ≥ 4 |
| 6 | Performance vs team/project outcomes | `team_performance` | team | headcount, avg rating, % high performers, # projects, avg project score, % on-time |
| 7 | Skill distribution | `skill_distribution` | competency × proficiency level | employee count, % within competency |

### Source-to-target mapping (lineage of columns)

| Gold column | Source | Transformation |
|---|---|---|
| `employee_name` | `employees.first_name`, `last_name` | concatenated (Gold) |
| `team_name`, `department` | `teams.name`, `teams.department` | `name` renamed to `team_name` (Gold join) |
| `rating`, `review_period` | `performance_reviews` | cast to int (Silver) |
| `previous_rating`, `rating_change`, `rating_trend` | `performance_reviews.rating` | previous period value / difference per employee (Gold) |
| `latest_rating`, `avg_rating` | `performance_reviews.rating` | last period / mean per employee (Gold) |
| `competency_name`, `category` | `competencies.name`, `category` | `name` renamed (Gold join) |
| `gap`, `has_gap`, `is_critical_gap` | `competency_assessments` | `required_level − proficiency_level`; `> 0`; `≥ 2` (Gold) |
| `is_high_potential`, `is_promotion_ready` | `talent_review` + reviews | rule in table above (Gold) |
| `text` | `learning_activity.text` **or** `.notes` | `notes` → `text` (Silver, schema drift) |
| `event_date` | `learning_activity.event_date` | string → timestamp (Silver) |
| `courses_completed`, `certifications_earned`, … | `learning_activity.event_type` | counted per employee; employees with no events get 0 (Gold) |
| `risk_score`, `risk_band` | `talent_review.attrition_risk` + reviews | rule in table above (Gold) |
| `avg_project_score`, `pct_projects_on_time` | `projects.outcome_score`, `outcome_status` | mean / % `on-time` per team (Gold) |

---

## 5. Reliability & observability

- **Idempotent:** Silver and Gold overwrite their datasets; Bronze Postgres snapshots overwrite the same
  `ingest_date` partition; Bronze event files are immutable and skipped if already present. A rerun on the
  same input produces identical output with no duplicates (verified: 2nd run `copied=0 already_present=783`, same row counts).
- **Reconciliation checks (16)** run at the end of every job: Bronze file sizes = landing; for every table
  `read = valid + rejected + duplicates`; Gold grain/row counts match Silver; skill-distribution total = assessments.
  Any failure → exit code 1 and status `FAILED`.
- **Lineage manifest** per run: run id, start/end/duration, status, counts per layer/dataset, all checks.
- **Structured logging:** `timestamp | LEVEL | message` with `INFO` counts per step, `WARNING` for quarantine/schema drift/empty sources, `ERROR` for failures.
- **Retries:** S3 and PostgreSQL calls retry with exponential backoff; connection errors after the last retry fail the run cleanly (manifest still written).

---

## 6. Results (sample dataset)

| Question | Headline |
|---|---|
| Q1 | Average rating rose **3.02 → 3.41**; rated 4–5 grew **34% → 50%**; 898 improved, 230 declined |
| Q2 | **325** critical gaps across **317** employees; most in Strategic Thinking & Stakeholder Management |
| Q3 | **646** high-potential, **166** promotion-ready now |
| Q4 | All 2,000 employees active; **3,421** course completions, **1,764** certifications |
| Q5 | **195** High-risk (65 of them also high-potential), 280 Medium; Sales & Marketing most exposed |
| Q6 | Weak positive link (r ≈ 0.24) between team rating and project score (27 teams with projects) |
| Q7 | Proficiency mostly 3–4; weakest Stakeholder Management (2.62), strongest Regulatory Compliance (3.31) |

---

## 7. Assumptions, trade-offs & limitations

| Topic | Decision / limitation |
|---|---|
| Engine | **Pandas, not Spark**: ~40k rows fits easily in memory, the EKS pod has 2 GB, and the repo recommends Python/Pandas. The layer functions are isolated, so moving to PySpark later only changes their bodies. |
| Single file | The Helm chart ships only `job.py` + `requirements.txt` to the pod, so helpers live in one sectioned file instead of separate modules. |
| One job, ordered stages | Each `data/*` folder becomes its own Kubernetes Job and they run concurrently, so Bronze/Silver/Gold run in order inside one job instead of 3 racing jobs. |
| Full refresh | Silver/Gold are rebuilt each run (cheap at this size). For large volumes, switch to incremental processing keyed on Bronze partitions. |
| Duplicates | First occurrence of an `event_id` is kept. One duplicated pair was itself invalid and went to quarantine, so 151 (not 152) duplicates are counted. |
| Risk score | Transparent rule-based score, not an ML model; weights are documented and easy to tune. |
| Trends | Only two review periods exist, so a "trend" is the change between two points. |
| Projects | Only 33 projects over 27 teams, so Q6 correlation is indicative only. |
| Aurora access | Aurora only accepts traffic from its own security group. Backend Lambdas are in it, EKS nodes are not, and participants may not change security groups. So the job reads Aurora **through the `talent-source` backend service** (section 8). |
| Aurora auto-pause | Aurora Serverless v2 is configured with `min_capacity = 0` and pauses when idle; the first connection waits ~15 s for it to resume. The backend uses a 60 s connect timeout plus retries. |
| Cloud credentials | Pods get no AWS identity from the node role, so the deploy passes the participant's temporary AWS credentials as env vars. Production should use IRSA / EKS Pod Identity and a Kubernetes Secret. |
| Backend API auth | The backend requires `X-Api-Key = sha256(POSTGRES_PASS)`. Both the Lambda and the EKS job already receive that password, so no new secret is introduced. |

---

## 8. Cloud deployment (AWS)

### Cloud architecture

```
 S3 landing/ (783 JSONL) ─────────────────────────────────────┐
                                                              ▼
 Aurora PostgreSQL ◄──► backend/talent-source (Lambda) ──► EKS Job: job.py ──► S3 data lake
   (7 HR tables)         in Aurora's security group          (talent-etl)       bronze / silver / gold
                         GET ?table=<name> → CSV                                quarantine / _lineage
```

### Why a backend service in front of Aurora?

| Finding | Evidence |
|---|---|
| Aurora's security group (`coding-workshop-<participant>`) allows traffic **only from itself** | `aws ec2 describe-security-groups` |
| Backend Lambdas run in that group (`infra/lambda.tf`), EKS managed nodes do not | Connections from an EKS pod and from the VDI timed out; the repo's own `backend/python-service` example connected (`PostgreSQL 17.7`) |
| Participants cannot add an ingress rule | `ec2:AuthorizeSecurityGroupIngress … explicit deny` |

So `backend/talent-source` is the network path to Aurora, following the repo's design (backend services talk to Aurora).

| Route (via CloudFront `/api/talent-source`) | Purpose |
|---|---|
| `POST ?action=seed` | Drop and recreate the 7 tables from `seed/schema.sql`, then load `seed/*.csv` (idempotent) |
| `GET ?action=health` | Row count per table |
| `GET ?table=<name>` | Whole table as CSV, used by Bronze ingestion (`read_postgres_via_api` in `job.py`) |

### Infra changes (`infra/`)

| File | Change | Why |
|---|---|---|
| `s3.tf` | Bucket `coding-workshop-datalake-<participant>` | The repo had no data-lake bucket |
| `helm/templates/python-job.yaml`, `helm/values.yaml` | `env:` block driven by `pythonJob.env` | The provided job template passed no environment variables to the pod |
| `eks.tf` | Passes `IS_LOCAL=false`, `POSTGRES_*`, `POSTGRES_API_URL`, `DATALAKE_BUCKET`, region and AWS credentials | Same `job.py`, cloud settings |
| `output.tf` | `datalake_bucket`, `postgres_*` (sensitive) | Load the sources without hard-coding anything |

### Result

The cloud run read Aurora through the backend, wrote all layers to `s3://coding-workshop-datalake-c9ade5e5/`,
passed **16/16 reconciliation checks** and produced the **same counts as the local run**
(`status=SUCCESS`, 48.6 s). Log excerpt in `TESTING.md` section 4.
