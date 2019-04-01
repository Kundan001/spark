-- Databricks notebook source
-- MAGIC %python
-- MAGIC # List all of the Databricks datasets
-- MAGIC display(dbutils.fs.ls("/databricks-datasets"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC with open("/dbfs/databricks-datasets/README.md") as f:
-- MAGIC     x = ''.join(f.readlines())
-- MAGIC 
-- MAGIC print(x)

-- COMMAND ----------

/* Option 1: Create a Spark table from the CSV data */
DROP TABLE IF EXISTS diamonds;
CREATE TABLE diamonds USING CSV OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # Option 2: Write the CSV data to Databricks Delta format and create a Delta table
-- MAGIC diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")
-- MAGIC diamonds.write.format("delta").save("/delta/diamonds")

-- COMMAND ----------

/* Option 2 contd: Create a Delta table at the stored location */
DROP TABLE IF EXISTS diamonds;
CREATE TABLE diamonds USING DELTA LOCATION '/delta/diamonds/'

-- COMMAND ----------

/* Query the table */
SELECT color, avg(price) AS price FROM diamonds GROUP BY color ORDER BY COLOR

-- COMMAND ----------


