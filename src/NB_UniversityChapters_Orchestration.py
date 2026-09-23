# Databricks notebook source
# MAGIC %md
# MAGIC | Field    | Value |
# MAGIC |----------|-------|
# MAGIC | Author   | Neelavani R|
# MAGIC | Date     | 2026-09-21 |
# MAGIC | Purpose  | Initial Version - Ingest university chapter data from a public API and publishes it as a data product others can consume.
# MAGIC

# COMMAND ----------

dbutils.widgets.text("SOURCE_NAME", "ArcGIS", "Source Name")
dbutils.widgets.text("OBJECT_NAME", "university_chapters", "Object Name")

SOURCE_NAME  = dbutils.widgets.get("SOURCE_NAME")
OBJECT_NAME  = dbutils.widgets.get("OBJECT_NAME")

print(f"SOURCE_NAME  = {SOURCE_NAME}")
print(f"OBJECT_NAME  = {OBJECT_NAME}")
BASE_LOC = f"/dbfs/{SOURCE_NAME}"

# COMMAND ----------

LAYER = "bronze"
PATH = f"{BASE_LOC}/{LAYER}/{OBJECT_NAME}"
dbutils.notebook.run(
    "NB_UniversityChapters_Inbound_To_Bronze",
    1200,
    {
        "SOURCE_NAME": SOURCE_NAME,
        "LAYER": LAYER,
        "OBJECT_NAME": OBJECT_NAME,
        "PATH" : PATH
    },
)

# COMMAND ----------

LAYER = "silver"
PATH = f"{BASE_LOC}/{LAYER}/{OBJECT_NAME}"
display(PATH)
dbutils.notebook.run(
    "NB_UniversityChapters_Bronze_To_Silver",
    1200,
    {
        "SOURCE_NAME": SOURCE_NAME,
        "LAYER": LAYER,
        "OBJECT_NAME": OBJECT_NAME,
        "PATH" : PATH
    },
)

# COMMAND ----------

LAYER = "gold"
PATH = f"{BASE_LOC}/{LAYER}/{OBJECT_NAME}"
output = dbutils.notebook.run(
    "NB_UniversityChapters_Silver_To_Gold",
    1200,
    {
        "SOURCE_NAME": SOURCE_NAME,
        "LAYER": LAYER,
        "OBJECT_NAME": OBJECT_NAME,
        "PATH" : PATH
    },
)
print(output)