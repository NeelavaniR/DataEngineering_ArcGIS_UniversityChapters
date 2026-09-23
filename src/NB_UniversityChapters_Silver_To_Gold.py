# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC | Field    | Value |
# MAGIC |----------|-------|
# MAGIC | Author   | Neelavani R|
# MAGIC | Date     | 2026-09-21 |
# MAGIC | Purpose  | Initial Version - Publish a curated data product from the silver layer in a consumer-facing format.|

# COMMAND ----------

dbutils.widgets.text("SOURCE_NAME", "ArcGIS", "Source Name")
dbutils.widgets.text("LAYER", "gold", "Layer")
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

SOURCE_TABLE_NAME = f"silver.tbl_{OBJECT_NAME}"
TABLE_NAME        = f"{LAYER}.tbl_{OBJECT_NAME}"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

spark.sql(f"""MERGE INTO {TABLE_NAME} AS target
USING {SOURCE_TABLE_NAME} AS source
ON target.chapter_id = source.chapter_id
WHEN MATCHED THEN
  UPDATE SET
    chapter_name         = source.chapter_name,
    city                 = source.city,
    state                = source.state,
    `longitude/latitude` = source.longitude/source.latitude,
    lastloaddate         = current_timestamp(),
    state_code           = source.state_code,
    dq_warnings          = source.dq_warnings,
    dq_status            = source.dq_status
WHEN NOT MATCHED THEN
  INSERT (chapter_id, chapter_name, city, state, `longitude/latitude`, lastloaddate, state_code, dq_warnings, dq_status)
  VALUES (source.chapter_id, source.chapter_name, source.city, source.state, source.longitude/source.latitude, current_timestamp(),state_code,dq_warnings,dq_status)
""")




# COMMAND ----------

# DBTITLE 1,Gold: Create Consumer View
VIEW_NAME = f"{LAYER}.vw_tbl_{OBJECT_NAME}"

spark.sql(f"""
    CREATE OR REPLACE VIEW {VIEW_NAME} AS
    SELECT chapter_id, chapter_name, city, state, `longitude/latitude`, lastloaddate, state_code
    FROM {TABLE_NAME}
""")

row_count = spark.sql(f"SELECT COUNT(*) FROM {VIEW_NAME}").collect()[0][0]
print(f"View created: {VIEW_NAME} ({row_count} rows)")

dbutils.notebook.exit(f"Gold view {VIEW_NAME} created with {row_count} rows")