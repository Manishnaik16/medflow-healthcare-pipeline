# Databricks notebook source
# MAGIC %md
# MAGIC # 🏥 MedFlow Analytics Pipeline
# MAGIC ## Notebook 1: Setup & Synthetic Data Generation
# MAGIC
# MAGIC **Tables created:**
# MAGIC - `medflow.bronze_patients`
# MAGIC - `medflow.bronze_admissions`
# MAGIC - `medflow.bronze_diagnoses`

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 1: Create the database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS medflow
# MAGIC COMMENT 'MedFlow Healthcare Analytics Pipeline — Medallion Architecture';
# MAGIC
# MAGIC SHOW DATABASES LIKE 'medflow';

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Generate synthetic data in Python and convert to Spark DataFrames

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

random.seed(42)

# ── Helper data ──────────────────────────────────────────────────────────────
FIRST_NAMES = ["Aarav","Priya","Rohan","Anjali","Vikram","Neha","Arjun",
               "Divya","Kiran","Meera","Siddharth","Pooja","Rahul","Sneha",
               "Amit","Riya","Suresh","Kavya","Deepak","Nisha"]
LAST_NAMES  = ["Sharma","Patel","Singh","Kumar","Gupta","Joshi","Reddy",
               "Nair","Iyer","Mehta","Verma","Rao","Shah","Chandra","Pillai"]
GENDERS     = ["M","F","Other"]
BLOOD_TYPES = ["A+","A-","B+","B-","AB+","AB-","O+","O-"]
CITIES      = ["Mumbai","Delhi","Bengaluru","Chennai","Hyderabad","Pune","Kolkata","Ahmedabad"]
WARDS       = ["Cardiology","Orthopedics","Neurology","General Medicine",
               "Pediatrics","Oncology","Emergency","ICU"]
DISCHARGE_TYPES = ["Recovered","Referred","LAMA","Expired","Transferred"]
DOCTORS     = [f"DR{i:04d}" for i in range(1, 51)]
ICD10_CODES = [
    ("I21","Acute myocardial infarction",       "Cardiovascular"),
    ("I10","Essential hypertension",            "Cardiovascular"),
    ("E11","Type 2 diabetes mellitus",          "Endocrine"),
    ("J18","Pneumonia unspecified",             "Respiratory"),
    ("J44","COPD",                              "Respiratory"),
    ("N18","Chronic kidney disease",            "Renal"),
    ("K92","Gastrointestinal haemorrhage",      "Gastrointestinal"),
    ("S72","Fracture of femur",                 "Musculoskeletal"),
    ("G35","Multiple sclerosis",                "Neurological"),
    ("C34","Malignant neoplasm of bronchus",    "Oncology"),
    ("A09","Infectious gastroenteritis",        "Infectious"),
    ("F32","Depressive episode",                "Mental Health"),
    ("M54","Dorsalgia",                         "Musculoskeletal"),
    ("Z23","Encounter for immunization",        "Preventive"),
    ("O80","Full-term uncomplicated delivery",  "Obstetric"),
]
SEVERITY_LEVELS = ["Mild","Moderate","Severe","Critical"]

def rand_date_str(start_year=1940, end_year=2005):
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31)
    return (start + timedelta(days=random.randint(0, (end - start).days))).strftime("%Y-%m-%d")

ingested_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Generate and write bronze_patients

# COMMAND ----------

NUM_PATIENTS = 1000
patients_rows = []

for i in range(1, NUM_PATIENTS + 1):
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)
    patients_rows.append((
        f"P{i:05d}",
        fn,
        ln,
        rand_date_str() if random.random() > 0.03 else None,    # 3% null DOB
        random.choice(GENDERS) if random.random() > 0.02 else None,
        random.choice(BLOOD_TYPES) if random.random() > 0.05 else None,
        random.choice(CITIES),
        f"+91-{random.randint(7000000000,9999999999)}" if random.random() > 0.08 else None,
        f"{fn.lower()}.{ln.lower()}{i}@email.com",
        ingested_at,
        "patients_source_system",   # _source_file
        "bronze",                   # _pipeline_layer
    ))

patients_schema = StructType([
    StructField("patient_id",      StringType(),  True),
    StructField("first_name",      StringType(),  True),
    StructField("last_name",       StringType(),  True),
    StructField("dob",             StringType(),  True),  # raw string — Silver will cast
    StructField("gender",          StringType(),  True),
    StructField("blood_type",      StringType(),  True),
    StructField("city",            StringType(),  True),
    StructField("phone",           StringType(),  True),
    StructField("email",           StringType(),  True),
    StructField("_ingested_at",    StringType(),  True),
    StructField("_source_file",    StringType(),  True),
    StructField("_pipeline_layer", StringType(),  True),
])

df_patients = spark.createDataFrame(patients_rows, schema=patients_schema)

(df_patients.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("medflow.bronze_patients"))

count = spark.table("medflow.bronze_patients").count()
print(f"✅ bronze_patients: {count:,} rows written")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 4: Generate and write bronze_admissions

# COMMAND ----------

NUM_ADMISSIONS = 3000
patient_ids    = [f"P{i:05d}" for i in range(1, NUM_PATIENTS + 1)]
admissions_rows = []

for i in range(1, NUM_ADMISSIONS + 1):
    admit_dt      = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 364))
    los_days      = random.randint(1, 30)
    discharge_dt  = admit_dt + timedelta(days=los_days)
    has_discharge = random.random() > 0.08   # 8% still admitted

    admissions_rows.append((
        f"A{i:06d}",
        random.choice(patient_ids),
        admit_dt.strftime("%Y-%m-%d"),
        discharge_dt.strftime("%Y-%m-%d") if has_discharge else None,
        random.choice(WARDS),
        random.choice(DOCTORS),
        random.choice(DISCHARGE_TYPES) if has_discharge else None,
        round(random.uniform(5000, 250000), 2),
        str(random.choice([True, False])),   # stored as string — raw source
        ingested_at,
        "admissions_source_system",
        "bronze",
    ))

admissions_schema = StructType([
    StructField("admission_id",    StringType(),  True),
    StructField("patient_id",      StringType(),  True),
    StructField("admit_date",      StringType(),  True),
    StructField("discharge_date",  StringType(),  True),
    StructField("ward",            StringType(),  True),
    StructField("doctor_id",       StringType(),  True),
    StructField("discharge_type",  StringType(),  True),
    StructField("admission_cost",  DoubleType(),  True),
    StructField("is_emergency",    StringType(),  True),  # raw string
    StructField("_ingested_at",    StringType(),  True),
    StructField("_source_file",    StringType(),  True),
    StructField("_pipeline_layer", StringType(),  True),
])

df_admissions = spark.createDataFrame(admissions_rows, schema=admissions_schema)

(df_admissions.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("medflow.bronze_admissions"))

count = spark.table("medflow.bronze_admissions").count()
print(f"✅ bronze_admissions: {count:,} rows written")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 5: Generate and write bronze_diagnoses

# COMMAND ----------

diagnoses_rows = []
diag_id = 1

for i in range(1, NUM_ADMISSIONS + 1):
    num_diags  = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0]
    chosen     = random.sample(ICD10_CODES, num_diags)
    pid        = admissions_rows[i-1][1]           # patient_id from admission
    admit_date = admissions_rows[i-1][2]

    for idx, (code, description, category) in enumerate(chosen):
        diagnoses_rows.append((
            f"D{diag_id:07d}",
            f"A{i:06d}",
            pid,
            code,
            description,
            category,
            random.choice(SEVERITY_LEVELS),
            str(idx == 0),    # first diagnosis is primary — raw string
            admissions_rows[i-1][5],   # doctor_id
            admit_date,
            ingested_at,
            "diagnoses_source_system",
            "bronze",
        ))
        diag_id += 1

diagnoses_schema = StructType([
    StructField("diagnosis_id",    StringType(), True),
    StructField("admission_id",    StringType(), True),
    StructField("patient_id",      StringType(), True),
    StructField("icd10_code",      StringType(), True),
    StructField("description",     StringType(), True),
    StructField("category",        StringType(), True),
    StructField("severity",        StringType(), True),
    StructField("is_primary",      StringType(), True),
    StructField("diagnosed_by",    StringType(), True),
    StructField("diagnosis_date",  StringType(), True),
    StructField("_ingested_at",    StringType(), True),
    StructField("_source_file",    StringType(), True),
    StructField("_pipeline_layer", StringType(), True),
])

df_diagnoses = spark.createDataFrame(diagnoses_rows, schema=diagnoses_schema)

(df_diagnoses.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("medflow.bronze_diagnoses"))

count = spark.table("medflow.bronze_diagnoses").count()
print(f"✅ bronze_diagnoses: {count:,} rows written")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 6: Verify all Bronze tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze_patients'   AS table_name, COUNT(*) AS rows FROM medflow.bronze_patients
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_admissions' AS table_name, COUNT(*) AS rows FROM medflow.bronze_admissions
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_diagnoses'  AS table_name, COUNT(*) AS rows FROM medflow.bronze_diagnoses;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Notice raw types: dob and dates are strings, is_emergency is "True"/"False"
# MAGIC -- Silver layer will clean all of this
# MAGIC SELECT patient_id, first_name, dob, gender, blood_type FROM medflow.bronze_patients LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta time travel — every write is versioned automatically
# MAGIC DESCRIBE HISTORY medflow.bronze_patients;

# COMMAND ----------

# Check null counts to document data quality baseline
from pyspark.sql import functions as F

print("=== NULL COUNTS IN BRONZE PATIENTS (baseline — do not fix here) ===\n")
spark.table("medflow.bronze_patients").select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in ["patient_id","first_name","dob","gender","blood_type","phone"]
]).show()

print("✅ Bronze layer complete! Run Notebook 02 → Silver.")
