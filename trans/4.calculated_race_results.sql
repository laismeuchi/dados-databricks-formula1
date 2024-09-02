-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
USING delta
AS
SELECT races.race_year,
constructors.name as team_name,
drivers.name as driver_name,
results.position,
results.points,
11 - results.position as calculated_points
 FROM results
 JOIN drivers ON results.driver_id = drivers.driver_id
 JOIN constructors ON constructors.constructor_id = results.constructor_id
 JOIN races ON races.race_id = results.race_id
 WHERE results.points <= 10

-- COMMAND ----------

select * from f1_presentation.calculated_race_results
