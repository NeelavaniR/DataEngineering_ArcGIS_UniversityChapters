# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC
# MAGIC | Field    | Value |
# MAGIC |----------|-------|
# MAGIC | Author   | Neelavani R|
# MAGIC | Date     | 2026-09-21 |
# MAGIC | Purpose  | Initial Version - Load API payload as received into bronze layer|

# COMMAND ----------

dbutils.widgets.text("SOURCE_NAME", "ArcGIS", "Source Name")
dbutils.widgets.text("LAYER", "bronze", "Layer")
dbutils.widgets.text("OBJECT_NAME", "university_chapters", "Object Name")
dbutils.widgets.text("PATH","Path","Path")

SOURCE_NAME  = dbutils.widgets.get("SOURCE_NAME")
LAYER         = dbutils.widgets.get("LAYER")
OBJECT_NAME  = dbutils.widgets.get("OBJECT_NAME")
PATH  = dbutils.widgets.get("PATH")

print(f"SOURCE_NAME  = {SOURCE_NAME}")
print(f"LAYER         = {LAYER}")
print(f"OBJECT_NAME  = {OBJECT_NAME}")
print(f"PATH  = {PATH}")

# COMMAND ----------

TABLE_NAME = f"{LAYER}.tbl_{OBJECT_NAME}"

# COMMAND ----------

# DBTITLE 1,Bronze: Raw API Landing
import requests
import json
import uuid
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# ---------------------------------------------------------------------------
# BRONZE LAYER  —  Raw API payload landing + ingest metadata
#   • Stores each ArcGIS feature as a raw JSON string (no business transforms)
#   • Appends every run so the table keeps full history by run
#   • Adds ingest metadata: run_id, source_url, lastloaddate
# ---------------------------------------------------------------------------

FEATURE_SERVER_URL = (
    "https://services2.arcgis.com/5I7u4SJE1vUr79JC/arcgis/rest/services/"
    "UniversityChapters_Public/FeatureServer/0/query/"
)

# ---------------------------------------------------------------------------
# Storage / Lake Layout  (DBFS base on AWS-backed Databricks)
#   /dbfs/<pipeline>/<layer>/...
#   Managed tables via saveAsTable don't need explicit paths, but the
#   variables are available if you later switch to external tables.
# ---------------------------------------------------------------------------


# Base DBFS path

# --- 1. Fetch raw features from ArcGIS (with pagination) --------------------
# The upstream ArcGIS view already filters Status = 'ACTIVE', so every
# returned feature is active — no client-side filtering needed.
# Pagination via resultOffset ensures we retrieve every matching row even
# when the result set exceeds the server's per-request limit.
MAX_RECORD_COUNT = 1000     # server limit per request

# Scope states — filter at the API so we only fetch rows we'll keep in silver.
SCOPE_STATES = ["CA", "OR", "WA"]
state_filter = "State IN ('{}')".format("','".join(SCOPE_STATES))

all_features = []
offset = 0
while True:
    params = {
       "where": state_filter,          # only CA / OR / WA chapters (upstream view handles ACTIVE filter)
        "outFields": "*",               # return every attribute field
        "returnGeometry": "true",      # point geometry (WGS84)
        "f": "json",
        "resultRecordCount": MAX_RECORD_COUNT,
        "resultOffset": offset,
    }
    resp = requests.get(FEATURE_SERVER_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    # ArcGIS may signal an error inside the JSON body
    if "error" in data:
        raise RuntimeError(f"ArcGIS returned error: {data['error']}")

    batch = data.get("features", [])
    all_features.extend(batch)
    print(f"  Fetched {len(batch)} records (offset {offset}) — running total {len(all_features)}")

    # Stop when a page is shorter than the page size (last page or no rows)
    if len(batch) < MAX_RECORD_COUNT:
        break

    offset += MAX_RECORD_COUNT

print(f"\nTotal features retrieved: {len(all_features)}")

# --- 2. Build raw rows — NO business transforms ----------------------------
# Each feature (attributes + geometry) is stored as a JSON string exactly as
# received from the API.  Only ingest-side metadata is added.
run_id = str(uuid.uuid4())

rows = []
for feat in all_features:
    rows.append({
        "raw_payload": json.dumps(feat),
        "source_url":  FEATURE_SERVER_URL,
        "run_id":      run_id,
    })

# Explicit schema so createDataFrame works even when the API returns zero features
bronze_schema = StructType([
    StructField("raw_payload", StringType(), True),
    StructField("source_url",  StringType(), True),
    StructField("run_id",      StringType(), True),
    StructField("ingest_date", TimestampType(), True),

])

bronze_df = spark.createDataFrame(rows, schema=bronze_schema)
bronze_df = bronze_df.withColumn("ingest_date", current_timestamp())

print("Bronze schema:")
bronze_df.printSchema()
print(f"Bronze row count: {bronze_df.count()}")

# --- 3. Append to Delta table (history by run) ------------------------------
# APPEND mode keeps every run so the bronze table is an immutable raw log.
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

(bronze_df.write
    .mode("append")
    .format("delta")
    .option("mergeSchema", "true")
    .partitionBy("ingest_date")
    .saveAsTable(TABLE_NAME))

print(f"\nBronze layer appended to: {TABLE_NAME}")
print(f"Run ID: {run_id}")

# Preview latest run
display(spark.table(TABLE_NAME).filter(lit(True) == lit(True)).limit(5))
