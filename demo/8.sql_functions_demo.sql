-- Databricks notebook source
USE f1_processed

-- COMMAND ----------

SELECT *,
concat(driver_ref,"-", code ) as new_driver_ref
FROM drivers

-- COMMAND ----------

SELECT *,
split(name, ' ')[0] AS forename,
split(name, ' ')[1] AS surname
FROM drivers

-- COMMAND ----------

SELECT *, current_timestamp
from drivers

-- COMMAND ----------

SELECT *, date_format(dob, 'dd-MM-yyyy')
from drivers

-- COMMAND ----------

SELECT *, date_add(dob, 1)
from drivers

-- COMMAND ----------

SELECT COUNT(*)
FROM drivers;

-- COMMAND ----------

SELECT MAX(dob) from drivers

-- COMMAND ----------

select * from drivers where dob = (SELECT MAX(dob) from drivers)

-- COMMAND ----------

SELECT COUNT(*)
from drivers 
where nationality = 'British'

-- COMMAND ----------

SELECT COUNT(*), nationality
from drivers 
GROUP BY nationality

-- COMMAND ----------

SELECT COUNT(*), nationality
from drivers 
GROUP BY ALL

-- COMMAND ----------

SELECT COUNT(*), nationality
from drivers 
GROUP BY nationality
HAVING count(*) > 100

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob DESC) as age_rank
FROM drivers
order by nationality, age_rank
