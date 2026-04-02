[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)
# Amman Digital Market ETL Pipeline

## Overview
A robust, configurable ETL pipeline built with Python, SQLAlchemy, and Pandas that:
- Extracts data from PostgreSQL
- Transforms data with cleaning, aggregation, and outlier detection (Tier 1)
- Supports incremental loading using metadata tracking (Tier 2)
- Uses a configurable framework with structured logging (Tier 3)

## Features Implemented
- **Tier 1**: Outlier detection (3σ), quality report JSON, `is_outlier` & `z_score` columns
- **Tier 2**: Incremental ETL + `etl_metadata` table for run history
- **Tier 3**: Config-driven framework + Structured logging + Multiple pipelines support

## Setup
1. Start PostgreSQL container:
   ```bash
   docker run -d --name postgres-m3-int -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=amman_market -p 5433:5432 -v pgdata_m3_int:/var/lib/postgresql/data postgres:15-alpine

## Load schema and seed data

2.Load schema and seed data:
psql -h localhost -U postgres -d amman_market -f schema.sql
psql -h localhost -U postgres -d amman_market -f seed_data.sql

3.**Install dependencies**
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

## How to Run
python etl_pipeline.py

## Output

output/customer_analytics.csv
output/quality_report.json
PostgreSQL tables: customer_analytics, etl_metadata
Structured logs with timestamps

## Quality Checks

No nulls in key columns
Positive revenue and order counts
No duplicates
Outlier detection (> 3 standard deviations)

## Tier 3 Framework
Adding a new pipeline requires only a new JSON config file — no code changes needed.
Tests
All required tests pass:

test_transform_filters_cancelled
test_transform_filters_suspicious_quantity
test_validate_catches_nulls


## Challenge Extensions - Tier 1 (Completed)

**Advanced Quality Checks & Reporting**

- Added statistical outlier detection using **3 standard deviations** from the mean of `total_revenue`.
- Added new columns to the final summary:
  - `is_outlier` (boolean)
  - `z_score`
- Generated `output/quality_report.json` containing:
  - ETL run timestamp
  - Total records checked
  - Number of checks passed/failed
  - List of flagged outliers (if any)
  - Summary statistics (mean, std, min, max revenue)

**Current Run Results:**
- Total customers analyzed: 100
- Outliers flagged: 0
- All data quality checks: **PASS**

This improves the reliability of the analytics by highlighting unusual high-spending customers.




## License

This repository is provided for educational use only. See [LICENSE](LICENSE) for terms.

You may clone and modify this repository for personal learning and practice, and reference code you wrote here in your professional portfolio. Redistribution outside this course is not permitted.
