# Databricks notebook source
# MAGIC %pip install Faker

# COMMAND ----------

from faker import Faker
from pyspark.sql.functions import *
from pyspark.sql.protobuf.functions import *
import random
fake_generator = Faker()

# COMMAND ----------

game_name_udf = udf(fake_generator.catch_phrase)
player_id_udf = udf(fake_generator.uuid4)
score_udf = udf(fake_generator.random_int)
game_level_udf = udf(fake_generator.military_ship)
game_over_udf = udf(fake_generator.boolean)

# COMMAND ----------

df = (
  spark.range(100000)
    .withColumn("game_name", game_name_udf())
    .withColumn("player_id", player_id_udf())
    .withColumn("score", score_udf().cast("int"))
    .withColumn("game_level", game_level_udf())
    .withColumn("game_over", game_over_udf().cast("boolean"))
    .withColumn("event_timestamp", current_timestamp().cast("string"))
).drop(col("id"))

# COMMAND ----------

display(df)

# COMMAND ----------

df.writeTo("craig_lukasik.game_heartbeats").createOrReplace()

# COMMAND ----------

KAFKA_KEY = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_KEY")
KAFKA_SECRET = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SECRET")
KAFKA_SERVER = dbutils.secrets.get(scope = "protobuf-prototype", key = "KAFKA_SERVER")

# COMMAND ----------

  df = spark.read.table("craig_lukasik.game_heartbeats").selectExpr("struct(*) as game_heartbeat")

# COMMAND ----------

display(df
    .selectExpr("to_json(game_heartbeat) as value"))

# COMMAND ----------

(
  df
    .selectExpr("to_json(game_heartbeat) as value")
    .write.format("kafka")
    .option("topic", "json_events2")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", 
            f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{KAFKA_KEY}' password='{KAFKA_SECRET}';")
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("kafka.sasl.mechanism", "PLAIN")
    .save()
)
# 19.54MB

# COMMAND ----------

(
  df
    .withColumn("proto_payload", to_protobuf(col("game_heartbeat"), "HeartbeatTelemetry", descFilePath="/dbfs/tmp/craig_lukasik/HeartbeatTelemetry.desc"))
    .select("proto_payload")
    .selectExpr("cast(proto_payload as string) as value")
    .write.format("kafka")
    .option("topic", "protobuf_events2")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", 
            f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{KAFKA_KEY}' password='{KAFKA_SECRET}';")
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("kafka.sasl.mechanism", "PLAIN")
    .save()
)
#11.3MB
