# Databricks notebook source
# DBTITLE 1,Silver DDL
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE OR REPLACE TABLE silver.tbl_university_chapters
# MAGIC (
# MAGIC     raw_payload STRING COMMENT 'Raw JSON payload',
# MAGIC     source_url STRING COMMENT 'URL of the source file',
# MAGIC     run_id STRING COMMENT 'Unique identifier for each ingestion run',
# MAGIC     lastloaddate TIMESTAMP COMMENT 'Timestamp of the last load into the silver table',
# MAGIC     chapter_id STRING COMMENT 'Unique identifier for each university chapter -  Stable business key',
# MAGIC     chapter_name STRING COMMENT 'Name of the university chapter',
# MAGIC     city STRING COMMENT 'City where the chapter is located',
# MAGIC     state STRING COMMENT 'State where the chapter is located -  USPS 2-letter',
# MAGIC     source_object_id STRING COMMENT 'Unique identifier for each source object',
# MAGIC     longitude DOUBLE COMMENT 'longitude coordinate value',
# MAGIC     latitude DOUBLE COMMENT 'latitude coordinate value',
# MAGIC     ingest_run_id STRING COMMENT 'Unique identifier for each ingestion run',
# MAGIC     state_code STRING COMMENT 'USPS 2-letter code for the state',
# MAGIC     dq_fail STRING COMMENT 'Indicates if the record failed any data quality checks',
# MAGIC     dq_warnings STRING COMMENT 'Indicates if the record had any data quality warnings',
# MAGIC     dq_status STRING COMMENT 'Overall data quality status of the record'
# MAGIC )
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Gold DDL

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
spark.sql("""

CREATE OR REPLACE TABLE gold.tbl_university_chapters
(
    chapter_id STRING,
    chapter_name STRING,
    city STRING,
    state STRING,
    `longitude/latitude` DOUBLE,
    lastloaddate TIMESTAMP,
    state_code STRING,
    dq_warnings STRING,
    dq_status STRING
)
""")

