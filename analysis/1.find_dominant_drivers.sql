-- Databricks notebook source
SELECT driver_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
group by driver_name
HAVING total_races >= 50
order by avg_points desc

-- COMMAND ----------

SELECT driver_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 and 2020
group by driver_name
HAVING total_races >= 50
order by avg_points desc
