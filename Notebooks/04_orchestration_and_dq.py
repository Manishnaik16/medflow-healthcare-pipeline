# Databricks notebook source
# MAGIC %md
# MAGIC # 🏥 MedFlow Analytics Pipeline
# MAGIC ## Notebook 4: Orchestration & Data Quality Checks
# MAGIC
# MAGIC Chains all notebooks and validates data quality at each layer.
# MAGIC Run this notebook to execute the full pipeline end-to-end.
# MAGIC
# MAGIC **Note:** Update the `%run` paths below to match where your notebooks
# MAGIC are saved in your Databricks workspace (e.g. `/Users/you@email.com/MedFlow/...`).

# COMMAND ----------

import time
from datetime import datetime
from pyspark.sql import functions as F

PIPELINE_NAME = "MedFlow Healthcare Analytics Pipeline"
RUN_DATE      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print("=" * 60)
print(f"  {PIPELINE_NAME}")
print(f"  Run date: {RUN_DATE}")
print("=" * 60)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Stage 1: Bronze + Data Generation

# COMMAND ----------

t0 = time.time()
# MAGIC %run "./01_setup_and_data_generation"
t_bronze = round(time.time() - t0, 1)
print(f"⏱  Bronze: {t_bronze}s")

# COMMAND ----------
# MAGIC %md #### Bronze DQ checks

# COMMAND ----------

def dq_assert(ok, rule, detail=""):
    if not ok:
        raise AssertionError(f"❌ DQ FAILURE — {rule}. {detail}")
    print(f"  ✅ {rule}")

print("── Bronze DQ ────────────────────────────────────────────────")
bp = spark.table("medflow.bronze_patients").count()
ba = spark.table("medflow.bronze_admissions").count()
bd = spark.table("medflow.bronze_diagnoses").count()

dq_assert(bp > 0,   "bronze_patients not empty",   f"{bp:,} rows")
dq_assert(ba > 0,   "bronze_admissions not empty",  f"{ba:,} rows")
dq_assert(bd > 0,   "bronze_diagnoses not empty",   f"{bd:,} rows")
dq_assert(ba > bp,  "admissions > patients (multiple admissions per patient)")

null_pids = spark.table("medflow.bronze_patients").filter(F.col("patient_id").isNull()).count()
dq_assert(null_pids == 0, "No null patient_ids in Bronze", f"{null_pids} found")

cols = spark.table("medflow.bronze_patients").columns
dq_assert("_ingested_at" in cols,    "_ingested_at column present")
dq_assert("_pipeline_layer" in cols, "_pipeline_layer column present")

print(f"\n  patients: {bp:,}  admissions: {ba:,}  diagnoses: {bd:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Stage 2: Silver

# COMMAND ----------

t1 = time.time()
# MAGIC %run "./02_silver_cleaning_joining"
t_silver = round(time.time() - t1, 1)
print(f"⏱  Silver: {t_silver}s")

# COMMAND ----------
# MAGIC %md #### Silver DQ checks

# COMMAND ----------

print("── Silver DQ ────────────────────────────────────────────────")
sp = spark.table("medflow.silver_patients_clean").count()
sf = spark.table("medflow.silver_patient_full_record").count()

max_loss = bp * 0.05
lost     = bp - sp
dq_assert(lost <= max_loss, "Silver row loss < 5%",
          f"Lost {lost} of {bp} ({round(lost/bp*100,1)}%)")

null_ids = spark.table("medflow.silver_patients_clean").filter(F.col("patient_id").isNull()).count()
dq_assert(null_ids == 0, "No null patient_ids in Silver")

bad_ages = (spark.table("medflow.silver_patients_clean")
    .filter((F.col("age_years") < 0) | (F.col("age_years") > 120)).count())
dq_assert(bad_ages == 0, "All age_years 0-120", f"{bad_ages} invalid")

neg_los = (spark.table("medflow.silver_admissions_enriched")
    .filter(F.col("length_of_stay_days") < 0).count())
dq_assert(neg_los == 0, "No negative LOS values", f"{neg_los} found")

dq_assert(sf > 0, "silver_patient_full_record not empty")
print(f"\n  silver_patients: {sp:,}  full_record: {sf:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Stage 3: Gold

# COMMAND ----------

t2 = time.time()
# MAGIC %run "./03_gold_aggregations"
t_gold = round(time.time() - t2, 1)
print(f"⏱  Gold: {t_gold}s")

# COMMAND ----------
# MAGIC %md #### Gold DQ checks

# COMMAND ----------

print("── Gold DQ ──────────────────────────────────────────────────")
gr = spark.table("medflow.gold_readmission_rate").count()
gd = spark.table("medflow.gold_top_diagnoses").count()

dq_assert(gr > 0, "gold_readmission_rate not empty")
dq_assert(gd > 0, "gold_top_diagnoses not empty")

bad_rates = (spark.table("medflow.gold_readmission_rate")
    .filter((F.col("readmission_rate_pct") < 0) | (F.col("readmission_rate_pct") > 100)).count())
dq_assert(bad_rates == 0, "All readmission rates 0-100%", f"{bad_rates} invalid")

expected_wards = {"Cardiology","ICU","Emergency","General Medicine"}
actual_wards   = {r["ward"] for r in
    spark.table("medflow.gold_readmission_rate").select("ward").distinct().collect()}
dq_assert(expected_wards.issubset(actual_wards), "All key wards in gold_readmission_rate",
          f"Missing: {expected_wards - actual_wards}")

print(f"\n  readmission rows: {gr:,}  diagnosis rows: {gd:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Pipeline Run Summary

# COMMAND ----------

total = t_bronze + t_silver + t_gold

print("\n" + "=" * 60)
print("  PIPELINE RUN SUMMARY")
print("=" * 60)
print(f"  Status    : ✅ SUCCESS")
print(f"  Run date  : {RUN_DATE}")
print(f"  Bronze    : {t_bronze}s")
print(f"  Silver    : {t_silver}s")
print(f"  Gold      : {t_gold}s")
print(f"  TOTAL     : {total}s")
print()

all_tables = [
    ("BRONZE", ["bronze_patients","bronze_admissions","bronze_diagnoses"]),
    ("SILVER", ["silver_patients_clean","silver_admissions_enriched",
                "silver_diagnoses_clean","silver_patient_full_record"]),
    ("GOLD",   ["gold_readmission_rate","gold_avg_length_of_stay",
                "gold_top_diagnoses","gold_monthly_admissions","gold_cost_analysis"]),
]

for layer, tables in all_tables:
    for t in tables:
        try:
            n = spark.table(f"medflow.{t}").count()
            print(f"  [{layer:6}] medflow.{t}: {n:,} rows")
        except:
            print(f"  [{layer:6}] medflow.{t}: ⚠️  not found")

print()
print("  All DQ checks passed ✅")
print("=" * 60)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Final sanity: all 12 tables in medflow database
# MAGIC SHOW TABLES IN medflow;
