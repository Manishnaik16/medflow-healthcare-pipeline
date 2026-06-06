# 🏥 MedFlow Healthcare Analytics Pipeline
### Medallion Architecture on Databricks Community Edition (Unity Catalog compatible)

---

## Notebook Guide

| # | Notebook | 
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


