# Databricks notebook source
# MAGIC  %run ../Includes/Copy-Datasets

# COMMAND ----------

dbutils.widgets.text("dataset_bookstore", dataset_bookstore)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtering Arrays

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   books,
# MAGIC   FILTER (books, i -> i.quantity >= 2) AS multiple_copies
# MAGIC FROM orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, multiple_copies
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     order_id,
# MAGIC     FILTER (books, i -> i.quantity >= 2) AS multiple_copies
# MAGIC   FROM orders)
# MAGIC WHERE size(multiple_copies) > 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transforming Arrays

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   books,
# MAGIC   TRANSFORM (
# MAGIC     books,
# MAGIC     b -> CAST(b.subtotal * 0.8 AS INT)
# MAGIC   ) AS subtotal_after_discount
# MAGIC FROM orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## User Defined Functions (UDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_url(email STRING)
# MAGIC RETURNS STRING
# MAGIC
# MAGIC RETURN concat("https://www.", split(email, "@")[1])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT email, get_url(email) domain
# MAGIC FROM customers

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION get_url

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION EXTENDED get_url

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION site_type(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE 
# MAGIC           WHEN email like "%.com" THEN "Commercial business"
# MAGIC           WHEN email like "%.org" THEN "Non-profits organization"
# MAGIC           WHEN email like "%.edu" THEN "Educational institution"
# MAGIC           ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
# MAGIC        END;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT email, site_type(email) as domain_category
# MAGIC FROM customers

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION get_url;
# MAGIC DROP FUNCTION site_type;
