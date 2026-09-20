# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3, Part 2: safe reload from Event Hub
# MAGIC Re-reads the event hub from the earliest available event. A fresh checkpoint would make a plain append duplicate rows, so the events go to a staging table and are merged into the main table by (`_eh_partition`, `_eh_offset`), inserting only the missing ones.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

# COMMAND ----------

import logging
import sys

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
assert catalog and schema, "Set the 'catalog' and 'schema' widgets before running"

secret_scope = "default"
eh_namespace = dbutils.secrets.get(secret_scope, "danylo-ev-hub-namespace")
eh_name = dbutils.secrets.get(secret_scope, "danylo-ev-hub-name")
eh_connection_str = dbutils.secrets.get(secret_scope, "danylo-ev-hub-connection-str")

target_table = f"{catalog}.{schema}.turbine_eventhub_bronze"
staging_table = f"{catalog}.{schema}.turbine_eventhub_staging"
reload_checkpoint = f"/Volumes/{catalog}/{schema}/raw/_checkpoint_eventhub_reload"

log = logging.getLogger("reload")
log.setLevel(logging.INFO)
log.propagate = False
if not log.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    log.addHandler(handler)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reload into staging
# MAGIC A fresh checkpoint and `startingOffsets=earliest` make the stream read every event still stored in the event hub. The result goes to a staging table, so the main table is not touched.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

payload_schema = StructType([
    StructField("device_id", StringType()),
    StructField("event_time", StringType()),
    StructField("wind_speed_mps", DoubleType()),
    StructField("rotor_rpm", DoubleType()),
    StructField("power_output_kw", DoubleType()),
    StructField("nacelle_temp_c", DoubleType()),
    StructField("status", StringType()),
])

# Databricks ships a shaded Kafka client, hence the kafkashaded prefix
jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="$ConnectionString" password="{eh_connection_str}";'
)

raw_stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", f"{eh_namespace}.servicebus.windows.net:9093")
              .option("subscribe", eh_name)
              .option("kafka.security.protocol", "SASL_SSL")
              .option("kafka.sasl.mechanism", "PLAIN")
              .option("kafka.sasl.jaas.config", jaas_config)
              .option("startingOffsets", "earliest")
              .option("failOnDataLoss", "false")
              .load())

parsed_stream = (raw_stream
                 .select(F.from_json(F.col("value").cast("string"), payload_schema).alias("data"),
                         F.col("partition").alias("_eh_partition"),
                         F.col("offset").alias("_eh_offset"),
                         F.col("timestamp").alias("_eh_enqueued_ts"))
                 .select("data.*", "_eh_partition", "_eh_offset", "_eh_enqueued_ts")
                 .withColumn("_ingest_ts", F.current_timestamp()))

# COMMAND ----------

dbutils.fs.rm(reload_checkpoint, recurse=True)
spark.sql(f"DROP TABLE IF EXISTS {staging_table}")

q = (parsed_stream.writeStream
     .option("checkpointLocation", reload_checkpoint)
     .trigger(availableNow=True)
     .toTable(staging_table))
q.awaitTermination()
log.info("Reload into staging finished")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare with the main table
# MAGIC Events present in both tables are the ones a plain append would duplicate. Events missing from the main table are the ones the merge will insert.

# COMMAND ----------

keys = ["_eh_partition", "_eh_offset"]
main_df = spark.table(target_table)
staging_df = spark.table(staging_table)

log.info("Staging rows: %d | already in main: %d | missing in main: %d",
         staging_df.count(),
         staging_df.join(main_df, keys, "left_semi").count(),
         staging_df.join(main_df, keys, "left_anti").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge into the main table
# MAGIC The merge inserts only events that are not in the main table yet, so running it repeatedly never creates duplicates.

# COMMAND ----------

before = spark.table(target_table).count()
spark.sql(f"""
    MERGE INTO {target_table} AS t
    USING {staging_table} AS s
    ON t._eh_partition = s._eh_partition AND t._eh_offset = s._eh_offset
    WHEN NOT MATCHED THEN INSERT *
""")
after = spark.table(target_table).count()
log.info("Merge into main: before=%d after=%d", before, after)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC Removes the staging table and its checkpoint.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {staging_table}")
dbutils.fs.rm(reload_checkpoint, recurse=True)