# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3, Part 2: Event Hub consumer
# MAGIC Wind turbine telemetry sent by a local producer is read from Azure Event Hubs with Structured Streaming and appended to a bronze Delta table together with Event Hub metadata (partition, offset, enqueued time).
# MAGIC
# MAGIC Catalog and schema are passed as widgets or job parameters. The connection details are read from a Key Vault backed secret scope.

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

checkpoint_path = f"/Volumes/{catalog}/{schema}/raw/_checkpoint_eventhub"
target_table = f"{catalog}.{schema}.turbine_eventhub_bronze"

log = logging.getLogger("consumer")
log.setLevel(logging.INFO)
log.propagate = False
if not log.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    log.addHandler(handler)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from Event Hub
# MAGIC Event Hubs exposes a Kafka compatible endpoint, so the stream is read with the built-in Kafka source and no extra library is needed. The event hub name is used as the Kafka topic and the connection string as the SASL password.

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
              # Retention is short, so expired offsets must not fail the job
              .option("failOnDataLoss", "false")
              .load())

parsed_stream = (raw_stream
                 .select(F.from_json(F.col("value").cast("string"), payload_schema).alias("data"),
                         F.col("partition").alias("_eh_partition"),
                         F.col("offset").alias("_eh_offset"),
                         F.col("timestamp").alias("_eh_enqueued_ts"))
                 .select("data.*", "_eh_partition", "_eh_offset", "_eh_enqueued_ts")
                 .withColumn("_ingest_ts", F.current_timestamp()))