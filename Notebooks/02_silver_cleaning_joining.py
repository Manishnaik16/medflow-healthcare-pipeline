# Databricks notebook source
# MAGIC %md
# MAGIC # 🏥 MedFlow Analytics Pipeline
# MAGIC ## Notebook 2: Silver Layer — Cleaning, Validation & Joining
# MAGIC
# MAGIC Reads from Bronze Delta tables and applies:
# MAGIC - Type casting (string dates → DateType, string booleans → BooleanType)
# MAGIC - Null handling and deduplication
# MAGIC - Derived columns: age, length of stay, readmission flag (Window function)
# MAGIC - Three-table join to build a master wide table
# MAGIC
# MAGIC **Tables created:**
# MAGIC - `medflow.silver_patients_clean`
# MAGIC - `medflow.silver_admissions_enriched`
# MAGIC - `medflow.silver_diagnoses_clean`
# MAGIC - `medflow.silver_patient_full_record`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import *

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Clean patients

# COMMAND ----------

bronze_patients = spark.table("medflow.bronze_patients")
print(f"Bronze patients: {bronze_patients.count():,}")

silver_patients = (
    bronze_patients
    .drop("_ingested_at", "_source_file", "_pipeline_layer")

    # Cast DOB from raw string to DateType
    .withColumn("dob", F.to_date(F.col("dob"), "yyyy-MM-dd"))

    # Derive age in years
    .withColumn("age_years",
        F.floor(F.datediff(F.current_date(), F.col("dob")) / 365.25))

    # Age bucket for segmentation
    .withColumn("age_bucket",
        F.when(F.col("age_years") <  18, "0-17")
         .when(F.col("age_years") <  40, "18-39")
         .when(F.col("age_years") <  60, "40-59")
         .when(F.col("age_years") <  80, "60-79")
         .otherwise("80+"))

    # Fill nulls with defaults
    .fillna({"gender": "Unknown", "blood_type": "Unknown", "phone": "Not Provided"})

    # Drop rows with no patient_id (unidentifiable)
    .filter(F.col("patient_id").isNotNull())

    # Remove duplicates — keep first occurrence per patient
    .dropDuplicates(["patient_id"])

    # Standardise gender to title case
    .withColumn("gender", F.initcap(F.col("gender")))

    .withColumn("_silver_processed_at", F.current_timestamp())
    .withColumn("_pipeline_layer",      F.lit("silver"))
)

(silver_patients.write
    .format("delta").mode("overwrite").option("overwriteSchema", True)
    .saveAsTable("medflow.silver_patients_clean"))

print(f"✅ silver_patients_clean: {spark.table('medflow.silver_patients_clean').count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Clean admissions + derive length of stay + 30-day readmission flag

# COMMAND ----------

bronze_admissions = spark.table("medflow.bronze_admissions")

silver_admissions = (
    bronze_admissions
    .drop("_ingested_at", "_source_file", "_pipeline_layer")

    # Cast date strings to DateType
    .withColumn("admit_date",     F.to_date(F.col("admit_date"),     "yyyy-MM-dd"))
    .withColumn("discharge_date", F.to_date(F.col("discharge_date"), "yyyy-MM-dd"))

    # Cast is_emergency from string "True"/"False" to proper BooleanType
    .withColumn("is_emergency", F.col("is_emergency").cast(BooleanType()))

    # Length of stay in days (null if still admitted)
    .withColumn("length_of_stay_days",
        F.when(F.col("discharge_date").isNotNull(),
               F.datediff(F.col("discharge_date"), F.col("admit_date")))
         .otherwise(None))

    # Date parts for time-series analysis
    .withColumn("admit_month",   F.month("admit_date"))
    .withColumn("admit_year",    F.year("admit_date"))
    .withColumn("admit_quarter", F.concat(F.lit("Q"), F.quarter("admit_date")))

    # LOS bucket
    .withColumn("los_bucket",
        F.when(F.col("length_of_stay_days") <= 1,  "Day case")
         .when(F.col("length_of_stay_days") <= 7,  "Short stay (2-7d)")
         .when(F.col("length_of_stay_days") <= 14, "Medium stay (8-14d)")
         .when(F.col("length_of_stay_days") >  14, "Long stay (>14d)")
         .otherwise("Still admitted"))

    .filter(F.col("admission_id").isNotNull())
    .dropDuplicates(["admission_id"])

    .withColumn("_silver_processed_at", F.current_timestamp())
    .withColumn("_pipeline_layer",      F.lit("silver"))
)

# ── 30-day readmission flag using Window function ─────────────────────────────
# lag() looks at the PREVIOUS row for the same patient (ordered by admit date)
# If the gap between the previous discharge and new admission is ≤ 30 days → readmission

window_spec = (
    Window
    .partitionBy("patient_id")   # group by patient
    .orderBy("admit_date")       # sort their admissions chronologically
)

silver_admissions = (
    silver_admissions
    .withColumn("prev_discharge_date",
        F.lag("discharge_date", 1).over(window_spec))
    .withColumn("is_30day_readmission",
        F.when(
            F.datediff(F.col("admit_date"), F.col("prev_discharge_date")) <= 30,
            True
        ).otherwise(False))
)

readmits = silver_admissions.filter(F.col("is_30day_readmission") == True).count()
print(f"30-day readmissions flagged: {readmits:,}")

(silver_admissions.write
    .format("delta").mode("overwrite").option("overwriteSchema", True)
    .saveAsTable("medflow.silver_admissions_enriched"))

print(f"✅ silver_admissions_enriched: {spark.table('medflow.silver_admissions_enriched').count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Clean diagnoses

# COMMAND ----------

bronze_diagnoses = spark.table("medflow.bronze_diagnoses")

silver_diagnoses = (
    bronze_diagnoses
    .drop("_ingested_at", "_source_file", "_pipeline_layer")
    .withColumn("diagnosis_date", F.to_date(F.col("diagnosis_date"), "yyyy-MM-dd"))
    .withColumn("is_primary", F.col("is_primary").cast(BooleanType()))
    .filter(F.col("diagnosis_id").isNotNull())
    .dropDuplicates(["diagnosis_id"])
    .withColumn("_silver_processed_at", F.current_timestamp())
    .withColumn("_pipeline_layer",      F.lit("silver"))
)

(silver_diagnoses.write
    .format("delta").mode("overwrite").option("overwriteSchema", True)
    .saveAsTable("medflow.silver_diagnoses_clean"))

print(f"✅ silver_diagnoses_clean: {spark.table('medflow.silver_diagnoses_clean').count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Build master wide table (3-table join)

# COMMAND ----------

patients   = spark.table("medflow.silver_patients_clean")
admissions = spark.table("medflow.silver_admissions_enriched")
diagnoses  = spark.table("medflow.silver_diagnoses_clean")

# Only one primary diagnosis per admission (prevents row duplication on join)
primary_dx = (
    diagnoses
    .filter(F.col("is_primary") == True)
    .select(
        "admission_id",
        F.col("icd10_code").alias("primary_icd10"),
        F.col("description").alias("primary_diagnosis"),
        F.col("category").alias("diagnosis_category"),
        F.col("severity").alias("diagnosis_severity"),
    )
)

full_record = (
    admissions
    .join(patients.select(
        "patient_id","first_name","last_name",
        "dob","age_years","age_bucket","gender","city"),
        on="patient_id", how="left")
    .join(primary_dx, on="admission_id", how="left")
    .select(
        "admission_id","patient_id","first_name","last_name",
        "gender","age_years","age_bucket","city",
        "admit_date","discharge_date","ward","doctor_id","discharge_type",
        "length_of_stay_days","los_bucket","admission_cost",
        "is_emergency","is_30day_readmission",
        "admit_month","admit_year","admit_quarter",
        "primary_icd10","primary_diagnosis",
        "diagnosis_category","diagnosis_severity",
        "_silver_processed_at",
    )
    .withColumn("_pipeline_layer", F.lit("silver"))
)

(full_record.write
    .format("delta").mode("overwrite").option("overwriteSchema", True)
    .saveAsTable("medflow.silver_patient_full_record"))

print(f"✅ silver_patient_full_record: {full_record.count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Verify Silver tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'silver_patients_clean'      AS tbl, COUNT(*) AS rows FROM medflow.silver_patients_clean
# MAGIC UNION ALL
# MAGIC SELECT 'silver_admissions_enriched' AS tbl, COUNT(*) AS rows FROM medflow.silver_admissions_enriched
# MAGIC UNION ALL
# MAGIC SELECT 'silver_diagnoses_clean'     AS tbl, COUNT(*) AS rows FROM medflow.silver_diagnoses_clean
# MAGIC UNION ALL
# MAGIC SELECT 'silver_patient_full_record' AS tbl, COUNT(*) AS rows FROM medflow.silver_patient_full_record;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Spot-check: notice age_years and length_of_stay_days are now numbers, not strings
# MAGIC SELECT admission_id, first_name, age_years, age_bucket,
# MAGIC        ward, length_of_stay_days, los_bucket,
# MAGIC        is_30day_readmission, primary_diagnosis, diagnosis_severity
# MAGIC FROM medflow.silver_patient_full_record
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Null check after cleaning (should be zeros for key columns)
# MAGIC SELECT
# MAGIC   COUNT(*) FILTER (WHERE patient_id IS NULL)          AS null_patient_ids,
# MAGIC   COUNT(*) FILTER (WHERE age_years IS NULL)           AS null_ages,
# MAGIC   COUNT(*) FILTER (WHERE length_of_stay_days IS NULL) AS null_los,
# MAGIC   COUNT(*) FILTER (WHERE ward IS NULL)                AS null_ward
# MAGIC FROM medflow.silver_patient_full_record;

# COMMAND ----------

print("✅ Silver layer complete! Run Notebook 03 → Gold.")
