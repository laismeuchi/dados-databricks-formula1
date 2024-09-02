-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;

CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuit_id INT,
circuitRef STRING,
name STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING CSV 
OPTIONS (header "true")
LOCATION 'abfss://raw@formula1dlmeuchi.dfs.core.windows.net/circuits.csv'

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;

CREATE TABLE IF NOT EXISTS f1_raw.races(race_id INT,
yera INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING)
USING CSV 
OPTIONS (header "true")
LOCATION 'abfss://raw@formula1dlmeuchi.dfs.core.windows.net/races.csv'


-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING)
USING JSON 
LOCATION 'abfss://raw@formula1dlmeuchi.dfs.core.windows.net/constructors.json'

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
LOCATION 'abfss://raw@formula1dlmeuchi.dfs.core.windows.net/drivers.json'

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
miliseconds INT,
fatestLap INT,
rank INT,
fatestLapTime STRING,
fatestLapSpeed FLOAT,
statsId STRING
)
USING json
LOCATION 'abfss://raw@formula1dlmeuchi.dfs.core.windows.net/results.json'

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

DROP TABLE IF EXISTS  f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING
)
USING JSON
OPTIONS(multiLine true)
LOCATION 'abfss://raw@formula1dlmeuchi.dfs.core.windows.net/pit_stops.json'

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING CSV
LOCATION 'abfss://raw@formula1dlmeuchi.dfs.core.windows.net/lap_times/'

-- COMMAND ----------

select count(*) from f1_raw.lap_times

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT
)
USING json
OPTIONS (multiLine true)
LOCATION 'abfss://raw@formula1dlmeuchi.dfs.core.windows.net/qualifying/'

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

describe extended f1_raw.qualifying
