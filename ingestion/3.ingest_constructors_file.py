# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest constructor.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

constuctor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = (
    spark.read
    .schema(constuctor_schema)
    .json("/mnt/formula1dlmeuchi/raw/constructors.json")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Drop unwanted columns from dataframe
# MAGIC

# COMMAND ----------

constructor_dropped_df = constructors_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

constructor_final_df = (
    constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("constructorRef", "constructor_ref")
    .withColumn("ingestion_date", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/formula1dlmeuchi/processed/constructors")
