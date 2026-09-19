# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3, Part 1: Auto Loader incremental ingestion
# MAGIC Synthetic wind-turbine telemetry (JSON, one record per file) is loaded into bronze Delta table.
# MAGIC Trigger `availableNow`, schema evolution mode `addNewColumns`.
# MAGIC
# MAGIC Catalog and schema are passed as widgets or job parameters; all paths are derived from them.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

# COMMAND ----------

import logging
import sys

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
assert catalog and schema, "Set the 'catalog' and 'schema' widgets before running"

raw_path = f"/Volumes/{catalog}/{schema}/raw/turbine"
schema_path = f"/Volumes/{catalog}/{schema}/raw/_schema"
checkpoint_path = f"/Volumes/{catalog}/{schema}/raw/_checkpoint"
target_table = f"{catalog}.{schema}.turbine_bronze"

log = logging.getLogger("autoloader")
log.setLevel(logging.INFO)
log.propagate = False
if not log.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    log.addHandler(handler)

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw")
dbutils.fs.mkdirs(raw_path)
log.info("Files currently in raw: %d", len(dbutils.fs.ls(raw_path)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest
# MAGIC `maxFilesPerTrigger=100` splits 1000 files into about 10 batches.
# MAGIC `schemaHints` pins `wind_speed_mps` to DOUBLE, so a malformed value goes to `_rescued_data` instead of changing the inferred type.

# COMMAND ----------

from pyspark.sql import functions as F


def run_ingest():
    """Read raw files with Auto Loader and append them to the bronze table."""
    stream_df = (spark.readStream.format("cloudFiles")
                 .option("cloudFiles.format", "json")
                 .option("cloudFiles.schemaLocation", schema_path)
                 .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                 .option("cloudFiles.inferColumnTypes", "true")
                 .option("cloudFiles.schemaHints", "wind_speed_mps DOUBLE")
                 .option("cloudFiles.maxFilesPerTrigger", 100)
                 .load(raw_path)
                 .withColumn("_ingest_ts", F.current_timestamp())
                 .withColumn("_source_file", F.col("_metadata.file_path")))
    q = (stream_df.writeStream
         .option("checkpointLocation", checkpoint_path)
         .option("mergeSchema", "true")
         .trigger(availableNow=True)
         .toTable(target_table))
    q.awaitTermination()
    return q

# COMMAND ----------

q = run_ingest()
log.info("Initial load done")

# COMMAND ----------

# Each file holds one record, so numInputRows equals the number of files in the batch
for p in q.recentProgress:
    log.info("batch %s | rows=%s | triggerMs=%s",
             p["batchId"], p["numInputRows"], p["durationMs"].get("triggerExecution"))

# COMMAND ----------

rescued = spark.table(target_table).filter("_rescued_data IS NOT NULL")
log.info("Rows with _rescued_data: %d", rescued.count())
display(rescued)

# COMMAND ----------

log.info("Total rows in bronze: %d", spark.table(target_table).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema evolution
# MAGIC Files with a new column (`blade_pitch_deg`) are added to the raw folder.
# MAGIC With `addNewColumns`, the first run fails once with `UnknownFieldException` after saving the new column to `schemaLocation`, and the next run succeeds.
# MAGIC In a job, task retries handle this automatically.

# COMMAND ----------

q = run_ingest()
log.info("Post-evolution load done")

# COMMAND ----------

display(spark.sql(
    f"SELECT count(*) AS rows_with_pitch FROM {target_table} WHERE blade_pitch_deg IS NOT NULL"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Safe reload and trigger types
# MAGIC Re-running with the existing checkpoint loads no new files, so no duplicates appear.
# MAGIC A fresh checkpoint reprocesses every file. This is shown with `trigger(once=True)` on a separate demo table, which keeps the main bronze table clean.

# COMMAND ----------

reload_ckpt = f"/Volumes/{catalog}/{schema}/raw/_checkpoint_reload_demo"
reload_table = f"{catalog}.{schema}.turbine_bronze_reload_demo"
dbutils.fs.rm(reload_ckpt, recurse=True)

qr = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", schema_path)
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaHints", "wind_speed_mps DOUBLE")
      .option("cloudFiles.maxFilesPerTrigger", 100)
      .load(raw_path)
      .writeStream
      .option("checkpointLocation", reload_ckpt)
      .option("mergeSchema", "true")
      .trigger(once=True)
      .toTable(reload_table))
qr.awaitTermination()
log.info("Reload demo rows (full reprocess): %d", spark.table(reload_table).count())

# COMMAND ----------

before = spark.table(target_table).count()
q = run_ingest()
after = spark.table(target_table).count()
log.info("Safe reload of main table: before=%d after=%d", before, after)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC Removes the reload demo table and its checkpoint.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {reload_table}")
dbutils.fs.rm(reload_ckpt, recurse=True)