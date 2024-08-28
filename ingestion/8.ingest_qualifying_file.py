# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest qualifying folder

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the Json file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import col, concat, current_timestamp, lit, col

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                     StructField("raceId", IntegerType(), True),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("constructorId", IntegerType(), True),
                                     StructField("number", IntegerType(), True ),
                                     StructField("position", IntegerType(), True),
                                     StructField("q1", StringType(), True),
                                     StructField("q2", StringType(), True),
                                     StructField("q3", StringType(), True)
                                    ])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiline", True).json("/mnt/formula1dlmeuchi/raw/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add new columns
# MAGIC

# COMMAND ----------

qualifying_final_df = ( qualifying_df.withColumnRenamed("qualifyingId", "qualifying_id")
                           .withColumnRenamed("raceId", "race_id")
                           .withColumnRenamed("driverId", "driver_id")
                           .withColumnRenamed("constructorId", "constructor_id")
                           .withColumn("ingestion_date", current_timestamp())
) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet("/mnt/formula1dlmeuchi/processed/qualifying")
