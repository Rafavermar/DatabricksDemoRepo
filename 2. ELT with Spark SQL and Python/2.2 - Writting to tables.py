# Databricks notebook source
# MAGIC %md
# MAGIC # Working with Delta Tables in Databricks
# MAGIC
# MAGIC This notebook demonstrates key operations on Delta tables using the bookstore dataset:
# MAGIC - Creating tables from files
# MAGIC - Overwriting tables
# MAGIC - Appending data
# MAGIC - Merging updates
# MAGIC
# MAGIC It shows how Delta enables ACID transactions, schema evolution, and time travel for reliable data engineering workflows.
# MAGIC

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Initialize the dataset path with a widget to make queries parameterized.
# MAGIC

# COMMAND ----------

dbutils.widgets.text("dataset_bookstore", dataset_bookstore)

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create a managed Delta table `orders` by reading Parquet files from the dataset path.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚠️ When creating the `orders` table, the table may be created but appear empty.  
# MAGIC This happens because in Free/Serverless Databricks, `CREATE TABLE ... AS SELECT` with file paths can succeed in registering the table schema but fail to ingest rows into Delta.  
# MAGIC
# MAGIC The files exist and can be listed, but the ingestion step does not materialize the records into the table.  
# MAGIC A reliable approach is to read the Parquet files with Spark (Python or SQL) and then write them explicitly into a Delta table.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE orders AS
# MAGIC SELECT * FROM parquet.`${dataset_bookstore}/orders`

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders

# COMMAND ----------

# Reading parquet with Spark into a DataFrame
df = (spark.read
      .parquet(f"{dataset_bookstore}/orders"))

# Write the DataFrame as a Delta table
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("orders_parquet_delta")

# Display the Delta table
display(spark.sql("select * from orders_parquet_delta"))      
display(spark.sql("select count(*) from orders_parquet_delta"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_parquet_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Overwriting Tables

# COMMAND ----------

# MAGIC %md
# MAGIC Demonstrates how to fully overwrite an existing table with new data using `CREATE OR REPLACE` and `INSERT OVERWRITE`.  
# MAGIC The `DESCRIBE HISTORY` command shows Delta transaction logs (time travel and audit).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️ In Free/Serverless Databricks, `CREATE TABLE ... AS SELECT` with Parquet creates an empty Delta table.  
# MAGIC Use Spark (`spark.read.parquet(...).write.format("delta")...`) to reliably ingest Parquet files into a managed Delta table.  
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_parquet_delta AS
# MAGIC SELECT * FROM parquet.`${dataset_bookstore}/orders`

# COMMAND ----------

df = (spark.read.parquet(f"{dataset_bookstore}/orders"))

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("orders_parquet_delta")


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_parquet_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚠️ CTAS vs INSERT OVERWRITE from files (Free/Serverless)
# MAGIC - `CREATE OR REPLACE TABLE ... AS SELECT` from parquet/csv/json may create an **empty table** in the Free/Serverless runtime (CTAS-on-files limitation).  
# MAGIC - Once the target exists as a **Delta** table, DML works:  
# MAGIC   `INSERT OVERWRITE/INSERT INTO <delta_table> SELECT * FROM parquet/json/csv` **loads rows** successfully.  
# MAGIC - If the table does not exist yet, **pre-create it as Delta** (e.g., with Spark `df.write.format("delta").saveAsTable(...)`) and then run `INSERT OVERWRITE` from the files.  
# MAGIC - In `DESCRIBE HISTORY`, DataFrame writes can appear with the operation name *“CREATE OR REPLACE TABLE AS SELECT”*; this is just the Delta log label, not evidence that a SQL CTAS-from-files succeeded.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE orders
# MAGIC SELECT * FROM parquet.`${dataset_bookstore}/orders`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Appending Data

# COMMAND ----------

# MAGIC %md
# MAGIC Use `INSERT INTO` to append new rows to the existing `orders` table.  
# MAGIC This preserves existing data while adding new records.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders
# MAGIC SELECT * FROM parquet.`${dataset_bookstore}/orders-new`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.Merging Data

# COMMAND ----------

# MAGIC %md
# MAGIC Demonstrates an **upsert** (update or insert) using `MERGE INTO`.  
# MAGIC - If a match is found, update missing email values.  
# MAGIC - If no match exists, insert new records.  
# MAGIC This is a typical pattern for CDC (Change Data Capture).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##⚠️ Creating a TEMP VIEW on top of JSON/CSV/Parquet files works in Free/Serverless because no data is materialized; the view simply references the files.
# MAGIC
# MAGIC In contrast, creating a managed table (`CREATE TABLE ... AS SELECT`) requires Delta format and often fails or creates empty tables if the source is not Delta.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temp view for the new customers
# MAGIC CREATE OR REPLACE TEMP VIEW customers_updates AS 
# MAGIC SELECT * FROM json.`${dataset_bookstore}/customers-json-new`;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge the new customers into the customers table
# MAGIC MERGE INTO customers c
# MAGIC USING customers_updates u
# MAGIC ON c.customer_id = u.customer_id
# MAGIC WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
# MAGIC   UPDATE SET email = u.email, updated = u.updated
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.Books updates

# COMMAND ----------

# MAGIC %md
# MAGIC Read new book records from CSV files into a temp view.  
# MAGIC These records are later merged into the `books` table with specific conditions.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW books_updates AS
# MAGIC SELECT * FROM read_files(
# MAGIC     '${dataset_bookstore}/books-csv-new',
# MAGIC     format => 'csv',
# MAGIC     header => 'true',
# MAGIC     delimiter => ';');
# MAGIC
# MAGIC SELECT * FROM books_updates

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.Merge into books

# COMMAND ----------

# MAGIC %md
# MAGIC Merge book updates into the `books` table.  
# MAGIC Only inserts new records if they belong to the 'Computer Science' category.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO books b
# MAGIC USING books_updates u
# MAGIC ON b.book_id = u.book_id AND b.title = u.title
# MAGIC WHEN NOT MATCHED AND u.category = 'Computer Science' THEN 
# MAGIC   INSERT *
