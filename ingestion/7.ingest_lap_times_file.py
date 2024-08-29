# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest lap_times folder

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the Json file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import col, concat, current_timestamp, lit, col

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True)
                                    ])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add new columns
# MAGIC

# COMMAND ----------

lap_times_final_df = ( lap_times_df.withColumnRenamed("raceId", "race_id")
                           .withColumnRenamed("driverId", "driver_id")
                           .withColumn("data_source", lit(v_data_source))
) 

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")
