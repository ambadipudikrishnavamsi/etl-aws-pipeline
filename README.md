# 🚀 ETL Data Pipeline — AWS Lambda + AWS Glue + S3

A serverless ETL pipeline that ingests raw JSON data via AWS Lambda, transforms it using AWS Glue (PySpark), and writes analytics-ready Parquet files back to S3.

---

## 📌 Architecture Overview

### **1. Lambda (Ingestion)**
- Fetches JSON from a public API.
- Writes raw data → **S3 (Raw Zone)**  
- Filename uses timestamp for uniqueness.

### **2. Glue Job (Transformation)**
- Reads raw JSON from S3.
- Cleans + standardizes the schema.
- Adds metadata (`ingest_date`, `ingest_ts_utc`).
- Writes Parquet output → **S3 (Curated Zone)**.
- Partitions data by `user_id`.

### **3. (Optional) Athena or Redshift**
- Query curated parquet data for analytics.

---

## 📁 Folder Structure

