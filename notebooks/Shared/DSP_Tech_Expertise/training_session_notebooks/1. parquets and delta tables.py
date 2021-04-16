# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://www.danone.com/content/dam/danone-corp/danone-com/homepage/DANONE_LOGO.png" alt="Data Science Platform" style="width: 600px">
# MAGIC </div>
# MAGIC 
# MAGIC <div>
# MAGIC <h1 align="center"> Data Science Platform</h1> 
# MAGIC <h2 align="center"> Section 1 : Parquet and Delta tables</h2> 
# MAGIC </div>
# MAGIC Delta Lake is a technology for building robust data lakes and is a component of building your cloud data platform. 
# MAGIC Delta Lake is a storage solution specifically designed to work with Apache Spark and is read from and written to using Apache Spark. 
# MAGIC As a reminder, a Datalake is a repository for raw data in a variety of formats.
# MAGIC 		 
# MAGIC <br>- Compute/Storage separation : advantage of scalability, availability and cost, Apache spark does not handle permanent storage
# MAGIC <br>- Support streaming data : Delta lake allow users to work with both batch and structured data. Data written to Delta Lake is immediately available for query.
# MAGIC <br>- It allows writes from both batch and streaming jobs into the same table : With other formats, data written into a table from a Structured Streaming job will overwrite any existing data in the table. This is because the metadata maintained in the table to ensure exactly-once guarantees for streaming writes does not account for other non streaming writes. Delta Lake’s advanced metadata management allows both batch and streaming data to be written.
# MAGIC <br>- It provides ACID guarantees even under concurrent writes :  Unlike built-in formats, Delta Lake allows concurrent batch and streaming operations to write data with ACID guarantees.
# MAGIC <br>- It allows multiple streaming jobs to append data to the same table:   The same limitation of metadata with other formats also prevents multiple Structured Streaming queries from appending to the same table. Delta Lake’s metadata maintains transaction information for each streaming query, thus enabling any number of streaming queries to concurrently write into a table with exactly-once guarantees. 
# MAGIC 		
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://live-delta-io.pantheonsite.io/wp-content/uploads/2019/04/Delta-Lake-marketecture-0423c.png" alt="Delta lake" style="width: 600px">
# MAGIC </div>
# MAGIC 
# MAGIC <h2> Objectives </h2>
# MAGIC - How to create a database on databricks and use it
# MAGIC - Save data to parquet and delta format
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC #### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png)  1. Create personal database for the training
# MAGIC 
# MAGIC  

# COMMAND ----------

username = "lambda_user" #enter your username
dbutils.widgets.text("username", username)
spark.sql(f"CREATE DATABASE IF NOT EXISTS training_session_DSP_{username}")
spark.sql(f"USE training_session_DSP_{username}")
#path_csv = f"/DSP_training_{username}"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC %md ##### <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> Check your work
# MAGIC You should see a database with the right name  created in **Data** section on left panel

# COMMAND ----------

# MAGIC %md
# MAGIC #### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 2. Drag the files shared with you in spark files system and show them

# COMMAND ----------

# MAGIC %md
# MAGIC - <a href="https://docs.databricks.com/dev-tools/databricks-utils.html" target="_blank">Databricks utilities</a>: `dbutils.fs` (`%fs`), `dbutils.notebooks` (`%run`), `dbutils.widgets`

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 2.1 Display in the same manner the content of `dbfs:/FileStore`

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 3. Read the csv data at the specified location *fire_call_path*
# MAGIC 
# MAGIC  

# COMMAND ----------

fire_call_path = "dbfs:/FileStore/tables/Fire_Department_Calls_for_Service.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 3.1 Infer data schema on a small chunk

# COMMAND ----------

# MAGIC %md
# MAGIC Define the **schema** allow databricks to read data faster

# COMMAND ----------

fire_calls_sample = (
  spark.read
  .format("csv") #FILL_THIS_IN
  .option("samplingRatio", 0.001)
  .option("header", True)
  .option("inferSchema", True)
  .option("sep",",")
  .load(fire_call_path) #FILL_THIS_IN
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 3.2 Use the infered schema to read the entire table

# COMMAND ----------

fire_calls = (
  spark.read
  .format("csv") #FILL_THIS_IN
  .schema(fire_calls_sample.schema)
  .option("header", "True")
  .option("sep",",")
  .load(fire_call_path) #FILL_THIS_IN
)


# COMMAND ----------

# MAGIC %md
# MAGIC #### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 4. Count the number of rows in *Fire_Department_Calls_for_Service*
# MAGIC 
# MAGIC 
# MAGIC  

# COMMAND ----------


fire_calls.count()


# COMMAND ----------

# MAGIC %md
# MAGIC #### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 5. Display the content of the spark dataframe *fire_calls* with command *display*

# COMMAND ----------

#display the dataset in a custom format
display(fire_calls) #FILL_THIS_IN

# COMMAND ----------

# MAGIC %md
# MAGIC #### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 6. Show first lines of the spark dataframe *fire_calls* with command `show`

# COMMAND ----------

nombre_lignes = 5
fire_calls.show(nombre_lignes, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 7. Remove space in columns space for allowing saving to parquet, delta....

# COMMAND ----------

for elt in fire_calls.columns:
  if " " in elt:
    fire_calls = fire_calls.withColumnRenamed(elt, elt.replace(" ", "_"))
    


# COMMAND ----------

# MAGIC %md
# MAGIC #### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 8. Write DataFrames to parquet with DataFrameWriter's `save` method and the following configurations:
# MAGIC - `snappy` compression
# MAGIC - `overwrite` mode

# COMMAND ----------

workingDir = "dbfs:/FileStore/tables/"
parquetOutputPath = workingDir + "/fire_calls.parquet"

(fire_calls.write
  .option("compression", "snappy")
  .mode("overwrite")
  .parquet(parquetOutputPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 9. Write `fire_calls` to <a href="https://delta.io/" target="_blank">Delta</a> with DataFrameWriter's `save` method and the following configurations:
# MAGIC - `delta` format
# MAGIC - `overwrite` mode

# COMMAND ----------

deltaOutputPath = workingDir + "/delta/fire_calls"

(fire_calls.write
  .format("delta")
  .mode("overwrite")
  .save(deltaOutputPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 10. Write DataFrames to tables
# MAGIC 
# MAGIC Write `fire_calls` to a table using the DataFrameWriter method `saveAsTable`
# MAGIC 
# MAGIC  This creates a global table, unlike the local view created by the DataFrame method `createOrReplaceTempView`

# COMMAND ----------

fire_calls.write.mode("overwrite").saveAsTable("fire_calls_table")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 11. Create a table name `fire_calls_parquet` with the written  parquet file created earlier

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS fire_calls_parquet;
# MAGIC 
# MAGIC CREATE TABLE fire_calls_parquet                        
# MAGIC USING PARQUET
# MAGIC 
# MAGIC LOCATION "dbfs:/FileStore/tables/fire_calls.parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC #### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 12. Create a delta table name `fire_calls_delta` with the delta partition created earlier

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS fire_calls_delta;
# MAGIC 
# MAGIC CREATE TABLE fire_calls_analysis                        
# MAGIC USING DELTA   
# MAGIC 
# MAGIC LOCATION "dbfs:/FileStore/tables/delta/fire_calls"

# COMMAND ----------

# MAGIC %md
# MAGIC ***Delta*** is storing the data as parquet, just has an additional layer over it with advanced features, providing history of events, (transaction log) and more flexibility on changing the content like, update, delete and merge capabilities

# COMMAND ----------

# MAGIC %md
# MAGIC #### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 13. Reading the created table

# COMMAND ----------

#FILL_THIS_IN
fire_calls_parquet = spark.read.table("fire_calls_analysis")
display(fire_calls)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png)  14. Get description of the tables

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE DETAIL fire_calls_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE DETAIL fire_calls_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE DETAIL fire_calls_table

# COMMAND ----------

