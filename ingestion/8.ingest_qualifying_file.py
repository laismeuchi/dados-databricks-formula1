# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest qualifying folder

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

qualifying_df = spark.read.schema(qualifying_schema).option("multiline", True).json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add new columns
# MAGIC

# COMMAND ----------

qualifying_final_df = ( qualifying_df.withColumnRenamed("qualifyingId", "qualifying_id")
                           .withColumnRenamed("raceId", "race_id")
                           .withColumnRenamed("driverId", "driver_id")
                           .withColumnRenamed("constructorId", "constructor_id")
                           .withColumn("data_source", lit(v_data_source))
) 

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

# qualifying_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")
qualifying_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")
