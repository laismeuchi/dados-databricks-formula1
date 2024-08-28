# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the Json file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import col, concat, current_timestamp, lit, col

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("postionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("status", StringType(), True)])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json("/mnt/formula1dlmeuchi/raw/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add new columns
# MAGIC

# COMMAND ----------

results_with_columns_df = ( results_df.withColumnRenamed("resultId", "result_id")
                           .withColumnRenamed("raceId", "race_id")
                           .withColumnRenamed("driverId", "driver_id")
                           .withColumnRenamed("constructorId", "constructor_id")
                           .withColumnRenamed("positionText", "postion_text")
                           .withColumnRenamed("positionOrder", "postion_order")
                           .withColumnRenamed("fastestLap", "fastest_lap")
                           .withColumnRenamed("fastestLapTime", "fastest_lap_time")
                           .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")
                           .withColumn("ingestion_date", current_timestamp())
) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop the unwanted column

# COMMAND ----------

results_final = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

results_final.write.mode("overwrite").partitionBy('race_id').parquet("/mnt/formula1dlmeuchi/processed/results")
