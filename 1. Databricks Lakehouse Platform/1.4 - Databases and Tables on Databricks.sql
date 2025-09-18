-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Managed Tables

-- COMMAND ----------

USE CATALOG workspace;

CREATE TABLE managed_default
  (width INT, length INT, height INT);

INSERT INTO managed_default
VALUES (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

DESCRIBE EXTENDED managed_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## External Tables

-- COMMAND ----------

-- Not supported en Free Editon as we dont have a real Unity Catalog available.
-- We have a kind of hive_metastore under the name of "workspace"

CREATE TABLE workspace.default.external_default (
  width INT, length INT, height INT
)
USING DELTA
LOCATION 'dbfs:/tmp/demo/external_default';

INSERT INTO workspace.default.external_default VALUES (3,2,1);


-- COMMAND ----------

DESCRIBE EXTENDED external_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## WORKAROUND due to Free Edition limitations:
-- MAGIC
-- MAGIC In Databricks Community Edition there is only a single built-in catalog called `workspace`, which acts as a simplified metastore.
-- MAGIC
-- MAGIC * Managed tables are supported and stored under `dbfs:/user/hive/warehouse/<schema>.db/<table>`.
-- MAGIC * Creating external tables with `LOCATION dbfs:/...` is not supported.
-- MAGIC * Path-based Delta tables can be used instead, for example:
-- MAGIC
-- MAGIC   ```sql
-- MAGIC   CREATE TABLE delta.`dbfs:/tmp/demo/external_default` (width INT, length INT, height INT);
-- MAGIC   ```
-- MAGIC * SHOW TABLES will not show the new created table but should allow to work with it.
-- MAGIC
-- MAGIC This allows demonstration of both managed tables and Delta tables stored directly at a path.
-- MAGIC

-- COMMAND ----------

CREATE TABLE workspace.default.managed_demo (
  width INT, length INT, height INT
);

INSERT INTO workspace.default.managed_demo VALUES (3,2,1);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Public DBFS root is disabled in Free Editon. Access is denied on path: /tmp/demo/external_default**

-- COMMAND ----------

CREATE TABLE delta.`dbfs:/tmp/demo/external_default` (
  width INT, length INT, height INT
);

INSERT INTO delta.`dbfs:/tmp/demo/external_default` VALUES (3,2,1);


-- COMMAND ----------

SELECT * FROM delta.`dbfs:/tmp/demo/external_default`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Tables

-- COMMAND ----------

DROP TABLE managed_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/tmp/demo/external_default'

-- COMMAND ----------

-- %fs ls 'dbfs:/user/hive/warehouse/managed_default'

-- COMMAND ----------

-- ET not created due to Free edition limitations
-- DROP TABLE external_default
DROP TABLE managed_demo

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/tmp/demo/external_default'

-- COMMAND ----------

-- %fs ls 'dbfs:/mnt/demo/external_default'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Schemas

-- COMMAND ----------

CREATE SCHEMA new_default

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED new_default

-- COMMAND ----------

USE new_default;

CREATE TABLE managed_new_default
  (width INT, length INT, height INT);
  
INSERT INTO managed_new_default
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------
-- Due to Free edition Limitations we are not able to create External Tables from a path
-----------------------------------

--CREATE TABLE external_new_default
--  (width INT, length INT, height INT)
--LOCATION 'dbfs:/temp/demo/external_new_default';
--  
--INSERT INTO external_new_default
--VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_new_default

-- COMMAND ----------

-- Not created due to Free edition limitations

--DESCRIBE EXTENDED external_new_default

-- COMMAND ----------

DROP TABLE managed_new_default;

-- DROP TABLE external_new_default;

-- COMMAND ----------

-- %fs ls 'dbfs:/user/hive/warehouse/new_default.db/managed_new_default'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Public DBFS root is disabled (FREE EDITION). Access is denied on path: /user/hive/warehouse/new_default.db/managed_new_default

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/new_default.db/managed_new_default"
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/temp/demo/external_new_default'

-- COMMAND ----------

-- %fs ls 'dbfs:/mnt/demo/external_new_default'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Schemas in Custom Location
-- MAGIC
-- MAGIC In Databricks Community Edition it is not possible to specify a custom MANAGED LOCATION on dbfs:/.
-- MAGIC Unity Catalog requires cloud storage URIs (abfss://, s3://, gs://, …), which are only available in paid workspaces with external storage configured.
-- MAGIC In CE, schemas can only be created in the default warehouse path:
-- MAGIC dbfs:/user/hive/warehouse/<schema>.db

-- COMMAND ----------

-- not supported in FREE EDITION

CREATE SCHEMA custom
LOCATION 'dbfs:/Shared/schemas/custom.db'

-- COMMAND ----------

-- For FREE EDITION we are only able to run the following:
-- it will be saved under the default path dbfs:/user/hive/warehouse/custom.db

CREATE SCHEMA custom

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED custom

-- COMMAND ----------

USE custom;

CREATE TABLE managed_custom
  (width INT, length INT, height INT);
  
INSERT INTO managed_custom
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------
-- Due to Free edition Limitations we are not able to create External Tables from a path
-----------------------------------

--CREATE TABLE external_custom
--  (width INT, length INT, height INT)
--LOCATION 'dbfs:/mnt/demo/external_custom';
--  
--INSERT INTO external_custom
--VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESCRIBE EXTENDED managed_custom

-- COMMAND ----------

--DESCRIBE EXTENDED external_custom

-- COMMAND ----------

DROP TABLE managed_custom;
--DROP TABLE external_custom;

-- COMMAND ----------

--NOT SUPPORTED IN FREE EDITION
--%fs ls 'dbfs:/Shared/schemas/custom.db/managed_custom'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Public DBFS root is disabled. Access is denied on path: /user/hive/warehouse/custom.db/managed_custom

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/custom.db/managed_custom'

-- COMMAND ----------

--%fs ls 'dbfs:/mnt/demo/external_custom'
