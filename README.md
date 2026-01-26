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
