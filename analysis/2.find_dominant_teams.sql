-- Databricks notebook source
SELECT team_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
group by team_name
HAVING total_races >= 100
order by avg_points desc

-- COMMAND ----------

SELECT team_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 and 2020
group by team_name
HAVING total_races >= 100
order by avg_points desc
