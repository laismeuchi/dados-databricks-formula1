-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Lesson Objectives
-- MAGIC 1. Create managed table using Python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run ../includes/configuration

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/races_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df.write.format("delta").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

describe extended demo.race_results_python

-- COMMAND ----------

SELECT * from demo.race_results_python
where race_year = 2020;

-- COMMAND ----------

CREATE TABLE race_results_sql AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

DESCRIBE EXTENDED race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------


