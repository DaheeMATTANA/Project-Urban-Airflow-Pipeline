# Analytics

This folder contains the **analytics engineering layer** of Urban Airflow Pipeline.

It includes dbt models, tests, and BI assets (Power BI dashboards).

---

## Structure
- **dbt/** → dbt project for transformations (intermediate → staging → mart).
  - `models/staging/` → source-aligned tables, cleaned and typed (one-to-one with raw sources).
  - `models/intermediate/` → business logic transformations, enriched datasets, joins across sources.
  - `models/marts/` → star-schema models (facts & dimensions) consumed by BI dashboards.
  - `tests/` → dbt schema & custom tests.
- **bi/powerbi/** → Power BI `.pbix` files and dataset documentation.

---

## Goals
- Apply **dbt transformations** locally using DuckDB (cost-free warehouse).
- Enforce **data quality** via dbt tests (and later Great Expectations).
- Provide **gold-layer marts** that drive Power BI dashboards.
- Show reproducible, professional analytics engineering workflow.
