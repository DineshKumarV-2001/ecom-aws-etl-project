# EcomPulse: AWS E-Commerce Medallion Lakehouse

[![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Glue%20%7C%20Athena-orange?logo=amazon-aws)](https://aws.amazon.com/)
[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/Apache%20Spark-PySpark-E25A1C?logo=apache-spark)](https://spark.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%2B-336791?logo=postgresql)](https://www.postgresql.org/)

An end-to-end, production-grade **Data Lakehouse pipeline** on AWS that incrementally extracts transactional e-commerce data from **PostgreSQL**, processes it through **Bronze, Silver, and Gold** Medallion layers using **Python** and **AWS Glue (PySpark)**, and serves star-schema **Apache Parquet** tables for serverless analytics in **Amazon Athena**.

> **Live Interactive Portfolio:** [View Architecture & Data Lineage Explorer](https://<your-username>.github.io/aws-ecommerce-medallion-lakehouse/) *(Enable GitHub Pages on `/docs` to activate)*

---

## Architecture Overview

```text
[PostgreSQL: ecom_db]
   │  (5 normalized OLTP tables with trigger-maintained updated_at)
   ▼  (High-Watermark Incremental Extract via Python + boto3)
[Amazon S3: Bronze Layer]
   │  (Raw, immutable NDJSON partitioned by ingest_date=YYYY-MM-DD)
   ▼  (AWS Glue PySpark Job 1: Window Deduplication, Null Imputation, Casting)
[Amazon S3: Silver Layer]
   │  (Cleansed, schema-enforced Snappy Parquet)
   ▼  (AWS Glue PySpark Job 2: Star-Schema Modeling + Broadcast Joins)
[Amazon S3: Gold Layer]
   │  (Business-ready Fact & Dimension tables partitioned by order_month)
   ▼
[AWS Glue Data Catalog] ──► [Amazon Athena (Serverless SQL Analytics)]
