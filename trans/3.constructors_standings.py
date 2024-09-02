# Databricks notebook source
# MAGIC %md
# MAGIC ### Produce driver standings

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, desc, rank, asc
from pyspark.sql.window import Window

# COMMAND ----------

race_results_df = spark.read.table("f1_presentation.races_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

constructors_standings_df = (race_results_df.groupBy("race_year", "team")
                       .agg(sum('points').alias('total_points'),
                        count(when(col("position") == 1, True)).alias("wins")
                       )
)

# COMMAND ----------

display(constructors_standings_df.filter("race_year = 2020"))

# COMMAND ----------

constructors_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_df = constructors_standings_df.withColumn("rank", rank().over(constructors_rank_spec))


# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").saveAsTable("f1_presentation.constructors_standings")
