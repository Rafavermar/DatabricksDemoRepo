-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Creating Delta Lake Tables

-- COMMAND ----------

--USE CATALOG hive_metastore
-- # [UC_HIVE_METASTORE_DISABLED_EXCEPTION] The operation attempted to use Hive Metastore, which is disabled due to legacy features being turned off in your account or workspace. Please enable Unity Catalog as the Hive Metastore is disabled. SQLSTATE: XXKUC

-- COMMAND ----------

USE CATALOG workspace

-- COMMAND ----------

CREATE TABLE employees
  (id INT, name STRING, salary DOUBLE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Catalog Explorer
-- MAGIC
-- MAGIC Check the created **employees** table in the **Catalog** explorer.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Inserting Data

-- COMMAND ----------

-- NOTE: With latest Databricks Runtimes, inserting few records in single transaction is optimized into single data file.
-- For this demo, we will insert the records in multiple transactions in order to create 4 data files.

INSERT INTO employees
VALUES 
  (1, "Adam", 3500.0),
  (2, "Sarah", 4020.5);

INSERT INTO employees
VALUES
  (3, "John", 2999.3),
  (4, "Thomas", 4000.3);

INSERT INTO employees
VALUES
  (5, "Anna", 2500.0);

INSERT INTO employees
VALUES
  (6, "Kim", 6200.3)

-- NOTE: When executing multiple SQL statements in the same cell, only the last statement's result will be displayed in the cell output.

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table Metadata

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table Directory

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - Community edition has not the hive_metastore available
-- MAGIC - the table was created in the Unity Catalog.
-- MAGIC - Unity Catalog unlike Hive_metastore manage the parquet + delta_log files under an abstraction layer, therefore we are not able to list the files in the way that hive_metastore does and the column location is returned empty above.
-- MAGIC - Solution: using DESCRIBE HISTORY

-- COMMAND ----------

-- %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- getting performing history
DESCRIBE HISTORY employees;

-- COMMAND ----------

-- Get schema and metadata
DESCRIBE EXTENDED employees;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Updating Table

-- COMMAND ----------

UPDATE employees 
SET salary = salary + 100
WHERE name LIKE "A%"

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- getting performing history
DESCRIBE HISTORY employees;

-- COMMAND ----------

-- Here the table shows only 1 numFiles because of the table is managed by Unity Catalog
-- by hive_metastore we were able to see 4 numFiles
DESCRIBE DETAIL employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table History

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'

-- COMMAND ----------

-- %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000005.json'

-- COMMAND ----------


