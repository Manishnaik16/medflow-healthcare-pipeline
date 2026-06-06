# Databricks notebook source
# MAGIC %md
# MAGIC # 🏥 MedFlow Analytics Pipeline
# MAGIC ## Notebook 3: Gold Layer — Aggregated KPIs & Analytics
# MAGIC
# MAGIC Reads `medflow.silver_patient_full_record` and produces 5 business KPI tables.
# MAGIC
# MAGIC **Tables created:**
# MAGIC - `medflow.gold_readmission_rate`
# MAGIC - `medflow.gold_avg_length_of_stay`
# MAGIC - `medflow.gold_top_diagnoses`
# MAGIC - `medflow.gold_monthly_admissions`
# MAGIC - `medflow.gold_cost_analysis`

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table("medflow.silver_patient_full_record")
print(f"Source rows: {silver.count():,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### KPI 1: 30-Day Readmission Rate by Ward and Month

# COMMAND ----------

gold_readmission = (
    silver
    .filter(F.col("discharge_date").isNotNull())
    .groupBy("ward", "admit_year", "admit_month", "admit_quarter")
    .agg(
        F.count("admission_id")                                         .alias("total_admissions"),
        F.sum(F.col("is_30day_readmission").cast("int"))                .alias("readmission_count"),
        F.round(
            F.sum(F.col("is_30day_readmission").cast("int")) * 100.0
            / F.count("admission_id"), 2)                               .alias("readmission_rate_pct"),
        F.countDistinct("patient_id")                                   .alias("unique_patients"),
    )
    .withColumn("_gold_processed_at", F.current_timestamp())
    .withColumn("_pipeline_layer",    F.lit("gold"))
    .orderBy("ward", "admit_year", "admit_month")
)

(gold_readmission.write
    .format("delta").mode("overwrite").option("overwriteSchema", True)
    .saveAsTable("medflow.gold_readmission_rate"))

print(f"✅ gold_readmission_rate: {spark.table('medflow.gold_readmission_rate').count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### KPI 2: Average Length of Stay (SparkSQL)

# COMMAND ----------

spark.sql("""
  CREATE OR REPLACE TABLE medflow.gold_avg_length_of_stay
  USING DELTA AS
  SELECT
    ward, age_bucket, doctor_id, discharge_type,
    COUNT(admission_id)                                        AS total_admissions,
    ROUND(AVG(length_of_stay_days), 2)                         AS avg_los_days,
    ROUND(MIN(length_of_stay_days), 0)                         AS min_los_days,
    ROUND(MAX(length_of_stay_days), 0)                         AS max_los_days,
    ROUND(PERCENTILE_APPROX(length_of_stay_days, 0.5), 1)      AS median_los_days,
    SUM(CASE WHEN los_bucket = 'Long stay (>14d)' THEN 1 ELSE 0 END)  AS long_stay_count,
    ROUND(SUM(CASE WHEN los_bucket = 'Long stay (>14d)' THEN 1 ELSE 0 END) * 100.0
          / COUNT(admission_id), 2)                            AS long_stay_pct,
    CURRENT_TIMESTAMP()                                        AS _gold_processed_at
  FROM medflow.silver_patient_full_record
  WHERE length_of_stay_days IS NOT NULL
  GROUP BY ward, age_bucket, doctor_id, discharge_type
  ORDER BY avg_los_days DESC
""")
print(f"✅ gold_avg_length_of_stay: {spark.table('medflow.gold_avg_length_of_stay').count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### KPI 3: Top Diagnoses by Volume

# COMMAND ----------

spark.sql("""
  CREATE OR REPLACE TABLE medflow.gold_top_diagnoses
  USING DELTA AS
  SELECT
    primary_icd10                       AS icd10_code,
    primary_diagnosis                   AS diagnosis_name,
    diagnosis_category, diagnosis_severity,
    COUNT(admission_id)                 AS admission_count,
    ROUND(AVG(length_of_stay_days), 2)  AS avg_los_days,
    ROUND(AVG(admission_cost), 2)       AS avg_cost_inr,
    SUM(CASE WHEN is_30day_readmission THEN 1 ELSE 0 END) AS readmission_count,
    ROUND(SUM(CASE WHEN is_30day_readmission THEN 1 ELSE 0 END) * 100.0
          / COUNT(admission_id), 2)     AS readmission_rate_pct,
    COUNT(DISTINCT ward)                AS wards_affected,
    CURRENT_TIMESTAMP()                 AS _gold_processed_at
  FROM medflow.silver_patient_full_record
  WHERE primary_icd10 IS NOT NULL
  GROUP BY primary_icd10, primary_diagnosis, diagnosis_category, diagnosis_severity
  ORDER BY admission_count DESC
""")
print(f"✅ gold_top_diagnoses: {spark.table('medflow.gold_top_diagnoses').count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### KPI 4: Monthly Admission Trends

# COMMAND ----------

spark.sql("""
  CREATE OR REPLACE TABLE medflow.gold_monthly_admissions
  USING DELTA AS
  SELECT
    admit_year, admit_month, admit_quarter, ward,
    COUNT(admission_id)                                       AS total_admissions,
    SUM(CASE WHEN is_emergency THEN 1 ELSE 0 END)             AS emergency_admissions,
    ROUND(SUM(CASE WHEN is_emergency THEN 1 ELSE 0 END)*100.0
          / COUNT(admission_id), 2)                           AS emergency_pct,
    ROUND(AVG(admission_cost), 2)                             AS avg_admission_cost,
    ROUND(SUM(admission_cost), 2)                             AS total_revenue,
    COUNT(DISTINCT patient_id)                                AS unique_patients,
    CURRENT_TIMESTAMP()                                       AS _gold_processed_at
  FROM medflow.silver_patient_full_record
  GROUP BY admit_year, admit_month, admit_quarter, ward
  ORDER BY admit_year, admit_month, ward
""")
print(f"✅ gold_monthly_admissions: {spark.table('medflow.gold_monthly_admissions').count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### KPI 5: Cost Analysis by Ward and Diagnosis Category

# COMMAND ----------

spark.sql("""
  CREATE OR REPLACE TABLE medflow.gold_cost_analysis
  USING DELTA AS
  SELECT
    ward, diagnosis_category, age_bucket,
    COUNT(admission_id)              AS admissions,
    ROUND(AVG(admission_cost), 2)    AS avg_cost,
    ROUND(MIN(admission_cost), 2)    AS min_cost,
    ROUND(MAX(admission_cost), 2)    AS max_cost,
    ROUND(SUM(admission_cost), 2)    AS total_cost,
    ROUND(STDDEV(admission_cost), 2) AS cost_std_dev,
    CURRENT_TIMESTAMP()              AS _gold_processed_at
  FROM medflow.silver_patient_full_record
  WHERE admission_cost IS NOT NULL
  GROUP BY ward, diagnosis_category, age_bucket
  ORDER BY total_cost DESC
""")
print(f"✅ gold_cost_analysis: {spark.table('medflow.gold_cost_analysis').count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Optimize Gold tables for fast queries

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE medflow.gold_readmission_rate ZORDER BY (ward, admit_year, admit_month);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE medflow.gold_avg_length_of_stay ZORDER BY (ward, age_bucket);

# COMMAND ----------
# MAGIC %md
# MAGIC ### Analytics preview — what a dashboard would query

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Which ward has the highest overall readmission rate?
# MAGIC SELECT
# MAGIC   ward,
# MAGIC   SUM(total_admissions) AS total_admissions,
# MAGIC   SUM(readmission_count) AS readmissions,
# MAGIC   ROUND(SUM(readmission_count)*100.0 / SUM(total_admissions), 2) AS readmission_rate_pct
# MAGIC FROM medflow.gold_readmission_rate
# MAGIC GROUP BY ward
# MAGIC ORDER BY readmission_rate_pct DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 5 diagnoses by volume
# MAGIC SELECT icd10_code, diagnosis_name, admission_count, avg_los_days,
# MAGIC        avg_cost_inr, readmission_rate_pct
# MAGIC FROM medflow.gold_top_diagnoses
# MAGIC ORDER BY admission_count DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summary: all Gold tables
# MAGIC SELECT 'gold_readmission_rate'    AS tbl, COUNT(*) AS rows FROM medflow.gold_readmission_rate
# MAGIC UNION ALL
# MAGIC SELECT 'gold_avg_length_of_stay'  AS tbl, COUNT(*) AS rows FROM medflow.gold_avg_length_of_stay
# MAGIC UNION ALL
# MAGIC SELECT 'gold_top_diagnoses'       AS tbl, COUNT(*) AS rows FROM medflow.gold_top_diagnoses
# MAGIC UNION ALL
# MAGIC SELECT 'gold_monthly_admissions'  AS tbl, COUNT(*) AS rows FROM medflow.gold_monthly_admissions
# MAGIC UNION ALL
# MAGIC SELECT 'gold_cost_analysis'       AS tbl, COUNT(*) AS rows FROM medflow.gold_cost_analysis;

# COMMAND ----------

print("✅ Gold layer complete! Run Notebook 04 → Orchestration.")
