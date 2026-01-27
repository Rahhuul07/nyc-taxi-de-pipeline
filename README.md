## NYC Taxi Data Engineering Pipeline

### Goal
Build a production-style batch + incremental data pipeline using:
- PySpark for data cleaning and transformation
- Apache Airflow for orchestration
- Snowflake for analytics storage
- SQL for incremental MERGE and reporting

### High-Level Architecture
Bronze (raw files) → PySpark (clean) → Snowflake Silver (MERGE) → Gold analytics

### Key Engineering Concepts Demonstrated
- Incremental loads using MERGE
- Idempotent pipeline design
- Data quality validation
- Orchestration with retries and backfills

### Step 2 Completed: Snowflake Foundation (DB + Schemas + Warehouse + Tables)

Implemented a cost-safe Snowflake setup to support the medallion architecture:

- Database: NYC_TAXI
- Schemas: BRONZE, SILVER, GOLD
- Compute warehouse: WH_NYC_TAXI (XSMALL, AUTO_SUSPEND=60s, AUTO_RESUME=TRUE, INITIALLY_SUSPENDED=TRUE)
- Core tables created:
  - NYC_TAXI.SILVER.TAXI_TRIPS_CLEAN (cleaned trip-level fact table for incremental MERGE)
  - NYC_TAXI.GOLD.DAILY_METRICS (daily aggregation table for reporting)

Setup SQL is saved in: sql/snowflake_setup.sql
