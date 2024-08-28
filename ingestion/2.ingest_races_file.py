# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, current_timestamp, to_timestamp, lit, concat

# COMMAND ----------

races_schema = StructType(
    fields=[
        StructField("raceId", IntegerType(), False),
        StructField("year", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("circuitId", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("date", DateType(), True),
        StructField("time", StringType(), True)
    ]
)

# COMMAND ----------

races_df = (
    spark.read.option("header", True)
    .schema(races_schema)
    .csv("/mnt/formula1dlmeuchi/raw/races.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Slelect only required columns

# COMMAND ----------

races_selected_df = races_df.select(
    col("raceId"),
    col("year"),
    col("round"),
    col("circuitId"),
    col("name"),
    col("date"),
    col("time"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename the columns as required

# COMMAND ----------

races_renamed_df = (
    races_selected_df.withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("year", "race_year")
    .withColumnRenamed("circuitId", "circuit_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Transform column time to race_timestamp
# MAGIC

# COMMAND ----------

races_transformed_df = (
    races_renamed_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Add ingestion date to dataframe

# COMMAND ----------

races_final_df = races_transformed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Write data to datalake as parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy('race_year').parquet("/mnt/formula1dlmeuchi/processed/races")
