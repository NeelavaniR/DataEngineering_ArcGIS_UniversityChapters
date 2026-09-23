# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC | Field    | Value |
# MAGIC |----------|-------|
# MAGIC | Author   | Neelavani R|
# MAGIC | Date     | 2026-09-21 |
# MAGIC | Purpose  | Initial Version - Load Cleaned, flattened data and add DQ check and load to silver layer

# COMMAND ----------

dbutils.widgets.text("SOURCE_NAME", "ArcGIS", "Source Name")
dbutils.widgets.text("LAYER", "silver", "Layer")
dbutils.widgets.text("OBJECT_NAME", "university_chapters", "Object Name")
dbutils.widgets.text("PATH","Path","Path")

SOURCE_NAME  = dbutils.widgets.get("SOURCE_NAME").lower()
LAYER         = dbutils.widgets.get("LAYER").lower()
OBJECT_NAME  = dbutils.widgets.get("OBJECT_NAME").lower()
PATH  = dbutils.widgets.get("PATH").lower()

print(f"SOURCE_NAME  = {SOURCE_NAME}")
print(f"LAYER         = {LAYER}")
print(f"OBJECT_NAME  = {OBJECT_NAME}")
print(f"PATH  = {PATH}")

# COMMAND ----------

SOURCE_TABLE_NAME = f"bronze.tbl_{OBJECT_NAME}"
TABLE_NAME        = f"{LAYER}.tbl_{OBJECT_NAME}"
QUARANTINE_TABLE  = f"{LAYER}.tbl_{OBJECT_NAME}_quarantine"

# COMMAND ----------

from pyspark.sql.functions import (
    col, get_json_object, current_timestamp, row_number, when, concat_ws, upper, lit,
)
from pyspark.sql.window import Window
import uuid

INGEST_RUN_ID = str(uuid.uuid4())

# ---------------------------------------------------------------------------
# SILVER LAYER  —  Cleaned, typed, deduped, flattened to chapter grain
#   • Parses raw bronze JSON into typed columns
#   • Deduplicates to one row per chapter_id (latest bronze run wins)
#   • Applies row-level DQ:  FAIL → quarantine  |  WARN → pass + flag
# ---------------------------------------------------------------------------


# Three-state scope: CA expected non-zero; OR/WA may legitimately be zero.
SCOPE_STATES = ["CA", "OR", "WA"]

# --- 1. Read bronze & parse raw JSON payload --------------------------------
# ArcGIS feature shape: {"attributes": {...}, "geometry": {"x": ..., "y": ...}}
bronze_df = spark.table(SOURCE_TABLE_NAME)

silver_base = (
    bronze_df
    .withColumn("chapter_id",       get_json_object(col("raw_payload"), "$.attributes.ChapterID"))
    .withColumn("chapter_name",     get_json_object(col("raw_payload"), "$.attributes.University_Chapter"))
    .withColumn("city",             get_json_object(col("raw_payload"), "$.attributes.City"))
    .withColumn("state",            get_json_object(col("raw_payload"), "$.attributes.State"))
    .withColumn("source_object_id", get_json_object(col("raw_payload"), "$.attributes.OBJECTID"))
    .withColumn("longitude",         get_json_object(col("raw_payload"), "$.geometry.x").cast("double"))
    .withColumn("latitude",          get_json_object(col("raw_payload"), "$.geometry.y").cast("double"))
    .withColumn("lastloaddate", current_timestamp())
    .withColumn("ingest_run_id", lit(INGEST_RUN_ID))
)

# --- 1b. Scope to CA / OR / WA only -----------------------------------------
#   Normalize the raw State field (full name or 2-letter code → state_code)
#   and keep only the three in-scope states.  OR/WA may legitimately be empty;
#   CA is expected to be non-zero — that distinction is reported below.
silver_base = (
    silver_base
    .withColumn("state_code",
        when(upper(col("state")) == "CA", "CA")
        .when(upper(col("state")) == "CALIFORNIA", "CA")
        .when(upper(col("state")) == "OR", "OR")
        .when(upper(col("state")) == "OREGON", "OR")
        .when(upper(col("state")) == "WA", "WA")
        .when(upper(col("state")) == "WASHINGTON", "WA")
        .otherwise(None))
    .filter(col("state_code").isin(SCOPE_STATES))
)

# --- 2. Deduplicate to chapter grain (latest run per chapter_id) -----------
dedupe_window = Window.partitionBy("chapter_id").orderBy(col("lastloaddate").desc())
silver_deduped = (
    silver_base
    .withColumn("_rn", row_number().over(dedupe_window))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

# --- 3. Row-level data-quality checks ---------------------------------------
#   FAIL     (quarantine) : chapter_id missing / coordinates missing
#   WARNING  (pass+flag)  : chapter_name missing / state missing / city missing-blank-UNKNOWN
dq_fail = concat_ws("; ",
    when(col("chapter_id").isNull() | (col("chapter_id") == ""),  "FAIL: chapter_id missing"),
    when(col("longitude").isNull() | col("latitude").isNull()
         | (col("longitude") < -180) | (col("longitude") > 180)
         | (col("latitude") < -90) | (col("latitude") > 90),
         "FAIL: INVALID_COORDINATES"),
)

dq_warnings = concat_ws("; ",
    when(col("chapter_name").isNull() | (col("chapter_name") == ""), "MISSING_OR_UNKNOWN_CHAPTER_NAME"),
    when(col("state").isNull(),                                       "MISSING_OR_UNKNOWN_STATE"),
    when(col("city").isNull() | (col("city") == "")
         | (upper(col("city")) == "UNKNOWN"),
         "MISSING_OR_UNKNOWN_CITY"),
)

silver_tagged = (
    silver_deduped
    .withColumn("dq_fail",     dq_fail)
    .withColumn("dq_warnings", dq_warnings)
    .withColumn("dq_status",
        when(col("dq_fail")      != "", "QUARANTINED")
        .when(col("dq_warnings") != "", "WARNING")
        .otherwise("OK"),
    )
)

# --- 4. Split: clean+warned vs quarantined ---------------------------------
silver_clean      = silver_tagged.filter(col("dq_status") != "QUARANTINED")
silver_quarantine = (
    silver_tagged
    .filter(col("dq_status") == "QUARANTINED")
    .select(
        "ingest_run_id", "chapter_id", "chapter_name", "city", "state", "state_code",
        "source_object_id", "longitude", "latitude", "dq_fail", "dq_warnings", "dq_status",
        "lastloaddate", "raw_payload",
    )
)

rows_in          = silver_tagged.count()
rows_quarantined  = silver_tagged.filter(col("dq_status") == "QUARANTINED").count()
rows_warned       = silver_tagged.filter(col("dq_status") == "WARNING").count()
rows_ok           = silver_tagged.filter(col("dq_status") == "OK").count()

print("--- Silver DQ Summary ---")
silver_tagged.groupBy("dq_status").count().orderBy("dq_status").show()
print(f"rows_in         : {rows_in}")
print(f"rows_quarantined : {rows_quarantined}")
print(f"rows_warned     : {rows_warned}")
print(f"rows_ok         : {rows_ok}")
print(f"Clean + warned rows : {silver_clean.count()}")
print(f"Quarantined rows   : {silver_quarantine.count()}")

# --- 3b. State-volume DQ: CA expected, OR/WA may be empty -------------------
#   Distinguishes "CA missing when historically present" (FAIL) from
#   "OR/WA empty" (acceptable source-data fact).
print("\n--- State-Volume DQ (scoped: CA, OR, WA) ---")
state_rows = (
    silver_tagged
    .filter(col("dq_status") != "QUARANTINED")
    .groupBy("state_code")
    .count()
    .collect()
)
state_count_map = {r["state_code"]: r["count"] for r in state_rows}

for st in SCOPE_STATES:
    cnt = state_count_map.get(st, 0)
    if st == "CA":
        verdict = "OK" if cnt > 0 else "FAIL — CA missing when historically present"
    else:
        verdict = "OK (zero is acceptable)" if cnt == 0 else f"OK ({cnt} rows)"
    print(f"  {st}: {cnt} rows  →  {verdict}")

# --- 5. Write silver tables (overwrite = current-state snapshot) -------------
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

(silver_clean.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(TABLE_NAME))

q_count = silver_quarantine.count()
if q_count > 0:
    (silver_quarantine.write
        .mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(QUARANTINE_TABLE))
    print(f"Quarantine table written: {QUARANTINE_TABLE} ({q_count} rows)")
else:
    print("No quarantined rows — quarantine table not written.")

print(f"\nSilver layer written to: {TABLE_NAME}")
