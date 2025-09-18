-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Delta Time Travel
-- MAGIC enables querying previous versions of a Delta table by timestamp or version number. It accesses the table's historical data to audit changes, perform rollbacks, and reproduce experiments.

-- COMMAND ----------

USE CATALOG workspace

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

SELECT * 
FROM employees VERSION AS OF 5

-- COMMAND ----------

SELECT * FROM employees@v4

-- COMMAND ----------

DELETE FROM employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

RESTORE TABLE employees TO VERSION AS OF 6

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## OPTIMIZE Command
-- MAGIC
-- MAGIC The **OPTIMIZE** command improves query performance by compacting small data files into larger ones. This process, known as bin-packing, reduces metadata overhead and speeds up read operations.
-- MAGIC
-- MAGIC Using the optional **Z-ORDER BY** clause further enhances performance by co-locating related information, which enables efficient data skipping on filtered queries.

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

OPTIMIZE employees
ZORDER BY id

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- hive_metastore files exploring (NOT available in Free Edition)
--%fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## VACUUM Command
-- MAGIC
-- MAGIC is a Delta Lake operation that permanently deletes files from storage that are no longer referenced by a Delta table and are older than a specified retention period. Its main purpose is to save on cloud storage costs and enforce compliance by removing old or deleted data. This action is irreversible and removes the ability to use Time Travel for table versions older than the retention period (which defaults to 7 days).
-- MAGIC
-- MAGIC **Key Differences from OPTIMIZE:**
-- MAGIC
-- MAGIC VACUUM: Deletes old, unreferenced files to save storage space. This is a file removal operation.
-- MAGIC
-- MAGIC OPTIMIZE: Rewrites existing, active files by compacting them into larger ones to improve query speed. This is a file layout operation.

-- COMMAND ----------

VACUUM employees

-- COMMAND ----------

-- %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- we are not able to retain so low period because Vacumm command aplies a 7 days time period
-- DONT USE IT IN PRODUCTION
VACUUM employees RETAIN 0 HOURS

-- COMMAND ----------

-- just for demonstration porpuses, the retention duration can be disabled (NOT AVAILABLE IN FREE EDITION)
-- DONT USE IT IN PRODUCTION
SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

-- NOT able to demonstrate a lower retention period. Not avaible in free edition.

-- VACUUM employees RETAIN 0 HOURS

-- COMMAND ----------

-- %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

SELECT * FROM employees@v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Tables

-- COMMAND ----------

DROP TABLE employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- %fs ls 'dbfs:/user/hive/warehouse/employees'
