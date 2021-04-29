# Databricks notebook source
#python
print("Hello World")

# COMMAND ----------

# MAGIC %r
# MAGIC wd <- getwd()
# MAGIC print(paste0("Current working dir: ", wd))

# COMMAND ----------

spark.sql("USE training_session_dsp_lambda_user")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fire_calls_parquet
# MAGIC LIMIT 10

# COMMAND ----------

