# 🏥 MedFlow Healthcare Analytics Pipeline
### Medallion Architecture on Databricks Community Edition (Unity Catalog compatible)

---

## What changed from v1 (DBFS fix)

Databricks workspaces with **Unity Catalog** enabled block public DBFS root access (`dbfs:/`).
The pipeline is now **4 notebooks** that work entirely with Delta tables — no raw file storage needed.
Data is generated in Python, converted to Spark DataFrames, and written straight to Bronze Delta tables.
This is actually closer to real production pipelines.

---

## How to import into Databricks Community Edition

1. Go to **https://community.cloud.databricks.com** and sign in
2. Create a cluster (Runtime 13.x LTS or higher)
3. In Workspace, create a folder called `MedFlow`
4. Import each `.py` file — Databricks converts them to notebooks automatically
5. Run in order: `01 → 02 → 03 → 04`

> For `%run` in Notebook 04, update the paths to match your workspace location.
> Example: `%run "/Users/you@email.com/MedFlow/01_setup_and_data_generation"`

---

## Notebook Guide

| # | Notebook | What you learn |
|---|---|---|
| 01 | Setup & Data Generation | `spark.createDataFrame`, Delta writes, `DESCRIBE HISTORY`, time travel |
| 02 | Silver Cleaning | PySpark transforms, `fillna`, `dropDuplicates`, Window + `lag()`, 3-table joins |
| 03 | Gold Aggregations | SparkSQL `GROUP BY`, `PERCENTILE_APPROX`, `OPTIMIZE`, `ZORDER BY` |
| 04 | Orchestration & DQ | `%run`, DQ assertions, pipeline timing summary |

---

## Architecture

```
Notebook 01 — generates data directly into Delta tables
        │
        ▼
BRONZE LAYER  (raw types — strings for dates, booleans stored as "True"/"False")
├── medflow.bronze_patients
├── medflow.bronze_admissions
└── medflow.bronze_diagnoses
        │
        ▼
SILVER LAYER  (cleaned, typed, derived columns, joined)
├── medflow.silver_patients_clean
├── medflow.silver_admissions_enriched   ← 30-day readmission flag via Window function
├── medflow.silver_diagnoses_clean
└── medflow.silver_patient_full_record   ← master wide table (3-table join)
        │
        ▼
GOLD LAYER  (aggregated KPIs, optimized for analytics)
├── medflow.gold_readmission_rate        ← readmission % by ward & month
├── medflow.gold_avg_length_of_stay      ← avg LOS by ward, age, doctor
├── medflow.gold_top_diagnoses           ← top ICD-10 by volume
├── medflow.gold_monthly_admissions      ← trends over time
└── medflow.gold_cost_analysis           ← cost by ward & diagnosis
```

---

## Key Concepts Covered

- Medallion Architecture (Bronze/Silver/Gold)
- Delta Lake (ACID, time travel, OPTIMIZE, ZORDER)
- PySpark DataFrame API
- SparkSQL (CREATE TABLE AS SELECT, GROUP BY, PERCENTILE_APPROX)
- Window Functions (`lag()` for readmission detection)
- Data Quality Assertions
- `spark.createDataFrame` with explicit schemas
- Three-table join to build a wide fact table
- Pipeline orchestration with `%run`

---


