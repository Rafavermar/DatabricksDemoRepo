-- Databricks notebook source
USE CATALOG workspace

-- COMMAND ----------

-- we see here that both persisted Views created on 1.4 Views are present in the catalog
SHOW TABLES;

-- COMMAND ----------

-- Here it is suppoused the global temp view to be checked, but due to Free editon Limitation que are not able to check it

-- SHOW TABLES IN global_temp;

-- COMMAND ----------

-- here we emulate the desired behaviour of checking a global temp view but just using the global temp like Schema and persistent view created at 1.4 View.
-- What we are trying to test is: how a globa temp view attached to a cluster is available in other session and other notebooks.

SHOW TABLES IN global_temp_like;

-- COMMAND ----------

-- SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

SELECT * FROM workspace.global_temp_like.pview_latest_phones

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Views

-- COMMAND ----------

DROP TABLE smartphones;

DROP VIEW view_apple_phones;
DROP VIEW workspace.global_temp_like.pview_latest_phones
-- DROP VIEW global_temp.global_temp_view_latest_phones;

