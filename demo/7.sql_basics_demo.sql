-- Databricks notebook source
show databases;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use f1_processed

-- COMMAND ----------

show tables

-- COMMAND ----------

select  * from drivers
where nationality = 'British'
and dob > '1990-01-01'

-- COMMAND ----------

select  name, dob from drivers
where nationality = 'British'
and dob > '1990-01-01'

-- COMMAND ----------

desc drivers

-- COMMAND ----------


