# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

races_df = (spark.read.table("f1_processed.races")
            .withColumnRenamed("name","race_name")
            .withColumnRenamed("date","race_date")
            .withColumnRenamed("time", "race_time")
            )

# COMMAND ----------

circuits_df = (spark.read.table("f1_processed.circuits")
               .withColumnRenamed("location","circuit_location"))

# COMMAND ----------

drivers_df = (spark.read.table("f1_processed.drivers")
              .withColumnRenamed("name", "driver_name")
              .withColumnRenamed("number", "driver_number")
              .withColumnRenamed("nationality", "driver_nationality")
              )

# COMMAND ----------

constructors_df = (spark.read.table("f1_processed.constructors")
                   .withColumnRenamed("name", "team"))

# COMMAND ----------

results_df = spark.read.table("f1_processed.results")

# COMMAND ----------

races_result_df = (races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")
                          .join(results_df, races_df.race_id == results_df.race_id, "inner")
                          .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner")
                          .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner")
                          .select(races_df.race_id,
                                  races_df.race_year,
                                  races_df.race_name,
                                  races_df.race_date,
                                  circuits_df.circuit_location,
                                  drivers_df.driver_name,
                                  drivers_df.driver_number,
                                  drivers_df.driver_nationality,
                                  constructors_df.team,
                                  results_df.grid,
                                  results_df.fastest_lap,
                                  races_df.race_time,
                                  results_df.points,
                                  results_df.position
                                  )
                          .withColumn("crated_date", current_timestamp())
)

# COMMAND ----------

display(races_result_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(races_result_df.points.desc()))

# COMMAND ----------

races_result_df.write.mode("overwrite").saveAsTable("f1_presentation.races_results")
