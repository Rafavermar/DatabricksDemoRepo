-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Preparing Sample Data

-- COMMAND ----------

USE CATALOG workspace;

CREATE TABLE IF NOT EXISTS smartphones
(id INT, name STRING, brand STRING, year INT);

INSERT INTO smartphones
VALUES (1, 'iPhone 14', 'Apple', 2022),
      (2, 'iPhone 13', 'Apple', 2021),
      (3, 'iPhone 6', 'Apple', 2014),
      (4, 'iPad Air', 'Apple', 2013),
      (5, 'Galaxy S22', 'Samsung', 2022),
      (6, 'Galaxy Z Fold', 'Samsung', 2022),
      (7, 'Galaxy S9', 'Samsung', 2016),
      (8, '12 Pro', 'Xiaomi', 2022),
      (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
      (10, 'Redmi Note 11', 'Xiaomi', 2021)

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Stored Views

-- COMMAND ----------

CREATE VIEW view_apple_phones
AS  SELECT * 
    FROM smartphones 
    WHERE brand = 'Apple';

-- COMMAND ----------

SELECT * FROM view_apple_phones;

-- COMMAND ----------

--  see not temporary table classification
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating Temporary Views

-- COMMAND ----------

CREATE TEMP VIEW temp_view_phones_brands
AS  SELECT DISTINCT brand
    FROM smartphones;

SELECT * FROM temp_view_phones_brands;

-- COMMAND ----------

-- see temporary table classification
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating Global Temporary Views

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Global Temporary Views **are not supported on Databricks SQL Serverless (including Free/Community edition)**.
-- MAGIC If you try to create one, you will see the error:
-- MAGIC
-- MAGIC [NOT_SUPPORTED_WITH_SERVERLESS] GLOBAL TEMPORARY VIEW is not supported on serverless compute.

-- COMMAND ----------

CREATE GLOBAL TEMP VIEW global_temp_view_latest_phones
AS SELECT * FROM smartphones
    WHERE year > 2020
    ORDER BY year DESC;

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### As an alternative can be used:
-- MAGIC
-- MAGIC **TEMP VIEW** → available only in the current session or notebook.
-- MAGIC
-- MAGIC ``` CREATE OR REPLACE TEMP VIEW latest_phones AS SELECT * FROM smartphones WHERE year > 2020 ORDER BY year DESC;```
-- MAGIC
-- MAGIC **Permanent VIEW** → available across sessions and notebooks within a catalog/schema.
-- MAGIC
-- MAGIC ``` CREATE OR REPLACE VIEW my_catalog.my_schema.latest_phones AS SELECT * FROM smartphones WHERE year > 2020 ORDER BY year DESC;```
-- MAGIC
-- MAGIC Instead of a GLOBAL TEMP VIEW, a permanent VIEW inside a schema that everyone can access (for example **global_temp_like** or any schema in your Unity Catalog) is created.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC When a permanent view is created inside a custom schema (for example global_temp_like), it will not appear by runing **SHOW TABLES;** without specifying a schema, because that command only lists objects under the default database.
-- MAGIC
-- MAGIC - **SHOW TABLES**; → shows only tables/views inside the default schema.
-- MAGIC
-- MAGIC - **SHOW TABLES IN global_temp_like**; → shows objects inside the global_temp_like schema, including the new permanent view pview_latest_phones.
-- MAGIC
-- MAGIC That is why the first SHOW TABLES; output does not display the new view, while the second SHOW TABLES IN global_temp_like; correctly lists it.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **temp_view_phones_branch** appears without a schema because it is a session-scoped TEMP VIEW. It only exists during the current session of this notebook, is not stored in any schema or catalog, and will disappear once the session ends.

-- COMMAND ----------

CREATE SCHEMA global_temp_like

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.global_temp_like.pview_latest_phones AS
SELECT *
FROM smartphones
WHERE year > 2020
ORDER BY year DESC;


-- COMMAND ----------

-- this command shows the tables under default database only
SHOW TABLES;

-- COMMAND ----------

-- this command shows the tables under global_temp_like schema
SHOW TABLES IN global_temp_like;
