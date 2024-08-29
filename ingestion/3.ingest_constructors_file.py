# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest constructor.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

constuctor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = (
    spark.read
    .schema(constuctor_schema)
    .json(f"{raw_folder_path}/constructors.json")
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
    .withColumn("data_source", lit(v_data_source))
    .withColumn("ingestion_date", current_timestamp())
)

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
