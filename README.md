# ETL Data Pipeline Automation — AWS + Python
Serverless data pipeline that ingests raw data, transforms it, and writes analytics-ready output.

## Architecture
- **Lambda (ingest):** fetches data from API and stores raw JSON to S3.
- **Glue (transform):** PySpark job cleans/normalizes data and writes Parquet to S3.
- **(Optional) Athena/Redshift:** query analytics layer.

## How to Use
1. Deploy Lambda with IAM allowing `s3:PutObject` to your raw bucket.
2. Create a Glue Job with IAM access to raw + curated buckets.
3. Update bucket names in `src/lambda_ingest.py` and `src/glue_job.py`.
4. Schedule with EventBridge.

## Folders
- `src/lambda_ingest.py` — Lambda handler
- `src/glue_job.py` — Glue PySpark job
- `infrastructure/template.yml` — Starter SAM/CloudFormation (fill in your ARNs)

