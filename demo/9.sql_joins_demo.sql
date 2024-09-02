-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
AS
SELECT race_year, driver_name, team, total_points, wins, rank
from driver_standings
where race_year = 2018;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
AS
SELECT race_year, driver_name, team, total_points, wins, rank
from driver_standings
where race_year = 2020;

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
INNER JOIN v_driver_standings_2020 d_2020 on d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
LEFT JOIN v_driver_standings_2020 d_2020 on d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
RIGHT JOIN v_driver_standings_2020 d_2020 on d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
FULL JOIN v_driver_standings_2020 d_2020 on d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
SEMI JOIN v_driver_standings_2020 d_2020 on d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
ANTI JOIN v_driver_standings_2020 d_2020 on d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
CROSS JOIN v_driver_standings_2020 d_2020 

-- COMMAND ----------


