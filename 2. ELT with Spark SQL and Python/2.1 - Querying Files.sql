-- Databricks notebook source
-- MAGIC %md
-- MAGIC # GOAL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Learn how to query raw files (JSON, CSV, Text, Binary) directly then register them as tables in Unity Catalog (workspace.default).
-- MAGIC
-- MAGIC WHY IMPORTANT?
-- MAGIC - Direct queries are flexible but ad-hoc.
-- MAGIC - Tables (managed or external) give schema, persistence, and governance.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Load dataset variables (from Copy-Datasets.py)

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a widget so we can pass the dataset path dynamically
-- MAGIC In free edition, dataset_bookstore is in workspace.default Volumes

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Configure the dataset path as a notebook parameter using Widgets, enabling its use in SQL queries through the ${dataset_bookstore} placeholder.
-- MAGIC
-- MAGIC dbutils.widgets.text("dataset_bookstore", dataset_bookstore)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DIRECT FILES QUERYING
-- MAGIC
-- MAGIC Direct file queries (e.g., json.`path`, csv.`path`) are not supported in Free Edition (Serverless). 
-- MAGIC
-- MAGIC They are used here only for learning purposes. 
-- MAGIC
-- MAGIC In production, the recommended approach is to create a table or view.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1. QUERYING JSON FILES DIRECTLY
-- MAGIC Spark SQL allows to query files with a "file format prefix"
-- MAGIC
-- MAGIC Example: json.`<path>`, csv.`<path>`, parquet.`<path>`, etc.
-- MAGIC
-- MAGIC This is flexible but does not persist metadata in the catalog.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⚠ The variable reference style ${variable} is deprecated. 
-- MAGIC
-- MAGIC The recommended syntax is :variable for parameter substitution. 
-- MAGIC
-- MAGIC The legacy style is kept here for learning continuity.
-- MAGIC

-- COMMAND ----------

SELECT * FROM json.`${dataset_bookstore}/customers-json/export_001.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Correct parameterized syntax (works in full Databricks editions with Unity Catalog)
-- MAGIC
-- MAGIC Fails in Free Edition due to unsupported direct query on files
-- MAGIC
-- MAGIC SELECT * 
-- MAGIC FROM json.`:dataset_bookstore/customers-json/export_001.json`;
-- MAGIC

-- COMMAND ----------

SELECT * FROM json.:dataset_bookstore/customers-json/export_001.json;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⚠ Direct query on JSON files using format_string is not supported in Free Edition (Serverless).  
-- MAGIC
-- MAGIC This example demonstrates the recommended parameterized syntax, but execution will fail here.  
-- MAGIC
-- MAGIC In production or full Databricks editions, this syntax ensures compatibility with Unity Catalog.
-- MAGIC

-- COMMAND ----------

-- Use :param_name style for parameter substitution

-- Recommended parameterized path using format_string
SELECT *
FROM json.`${format_string("%s/customers-json/export_001.json", dataset_bookstore)}`;



-- COMMAND ----------

SELECT * FROM json.`${dataset_bookstore}/customers-json/export_*.json`

-- COMMAND ----------

--SELECT * FROM json.`${dataset.bookstore}/customers-json`
SELECT * FROM json.`${dataset_bookstore}/customers-json`

-- COMMAND ----------

--SELECT count(*) FROM json.`${dataset.bookstore}/customers-json`
SELECT count(*) FROM json.`${dataset_bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### This query reads JSON files from the bookstore dataset and adds a column with the physical file path using `_metadata.file_path`. 
-- MAGIC
-- MAGIC This provides traceability of each row back to its source file, which is useful for debugging and incremental data processing.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The course uses `input_file_name()` because it was supported in legacy Hive metastore environments.  
-- MAGIC In Unity Catalog (the default in new workspaces, including the free edition), this function is deprecated.  
-- MAGIC The equivalent functionality is provided by `_metadata.file_path`, which works in Unity Catalog to return the source file path.
-- MAGIC

-- COMMAND ----------

 --SELECT *,
 --   input_file_name() source_file
 -- FROM json.`${dataset.bookstore}/customers-json`;

 SELECT *,
    -- input_file_name() source_file
    _metadata.file_path  source_file
  FROM json.`${dataset_bookstore}/customers-json`;

-- COMMAND ----------

-- SELECT * FROM text.`${dataset.bookstore}/customers-json`
SELECT * FROM text.`${dataset_bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  # 2. QUERYING BINARY FILES DIRECTLY

-- COMMAND ----------

-- SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json`
SELECT * FROM binaryFile.`${dataset_bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  # 3. QUERYING CSV FILES DIRECTLY

-- COMMAND ----------

-- SELECT * FROM csv.`${dataset.bookstore}/books-csv`
SELECT * FROM csv.`${dataset_bookstore}/books-csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⚠ IMPORTANT: In newer runtime versions, you can use read_files() in CTAS statements to create delta tables.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC  # 4. CREATING A MANAGED TABLE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This statement attempts to create a managed table from CSV files.  
-- MAGIC
-- MAGIC In full Unity Catalog environments, the `LOCATION` clause allows binding the table to external storage.  
-- MAGIC
-- MAGIC In the free edition, external `LOCATION` is not supported, so only default managed storage can be used.  
-- MAGIC
-- MAGIC The table can still be created by omitting the `LOCATION` clause, in which case Databricks manages the storage path internally.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4.1 FROM CSV

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ⚠ Only Delta is supported for managed tables. Provided datasource format is CSV.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ⚠ Environment limitations (Free / Serverless)
-- MAGIC
-- MAGIC In the Free / Serverless edition, creating managed tables directly from CSV is not supported.
-- MAGIC
-- MAGIC Hive Metastore is not available → CREATE TABLE ... USING CSV cannot be executed.
-- MAGIC
-- MAGIC Only Delta format is allowed for managed tables → every CREATE TABLE operation expects to write in Delta.
-- MAGIC
-- MAGIC Public DBFS root is disabled → Delta cannot be persisted under /mnt/... or /Volumes/....
-- MAGIC
-- MAGIC As a result, files can be queried directly (csv., json., read_files()), but converting them into managed tables is restricted. 
-- MAGIC
-- MAGIC The recommended approach is to read the files with Spark and explicitly write them as Delta tables.

-- COMMAND ----------

-- this will not work in Free Edition
CREATE TABLE books_csv
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";"
)
--LOCATION "${dataset_bookstore}/books-csv"
--LOCATION "${dataset.bookstore}/books-csv"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ⚠  In the free edition, the public DBFS root is disabled. 
-- MAGIC
-- MAGIC In the free edition, LOCATION is not supported for CREATE TABLE statements.  
-- MAGIC
-- MAGIC The platform manages Delta storage internally, and only fully managed Delta tables can be created.  
-- MAGIC
-- MAGIC External paths (CSV, JSON, Parquet) can only be queried directly or converted into managed Delta tables without specifying LOCATION.
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE books_csv
LOCATION '${dataset_bookstore}/books-delta'
AS
SELECT * FROM read_files(
    '${dataset_bookstore}/books-csv/export_*.csv',
    format => 'csv',
    header => 'true',
    delimiter => ';');


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### In free edition, `read_files()` cannot populate tables because Unity Catalog requires managed Volumes with write access to create Delta logs. 
-- MAGIC

-- COMMAND ----------

-- READFILES()
CREATE OR REPLACE TABLE books_csv AS
SELECT *
FROM read_files(
  '/Volumes/workspace/default/bookstore_dataset/books-csv/*.csv',
  format => 'csv',
  header => true,
  delimiter => ';'
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ⚠ This CTAS statement returns an empty table because in the Free/Serverless edition there is no writable DBFS root and only Delta is supported for managed tables.
-- MAGIC
-- MAGIC The CSV files are correctly read, but Databricks cannot persist them as a managed table in this environment, so the result is a table with no rows.

-- COMMAND ----------

CREATE OR REPLACE TABLE books_csv_delta
AS
SELECT *
FROM csv.`/Volumes/workspace/default/bookstore_dataset/books-csv`;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### By reading the CSV with spark.read.csv(...) and explicitly writing it as Delta (.write.format("delta")...saveAsTable), the data is ingested and persisted as a Delta table.
-- MAGIC
-- MAGIC This approach works in the Free/Serverless edition because it bypasses the CTAS restrictions and directly uses the supported Delta format.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = (spark.read
-- MAGIC       .option("header", True)
-- MAGIC       .option("delimiter", ";")
-- MAGIC       .csv("/Volumes/workspace/default/bookstore_dataset/books-csv"))
-- MAGIC
-- MAGIC # Save as Delta table (overwrites if exists)
-- MAGIC df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("books_csv_delta")
-- MAGIC

-- COMMAND ----------

SELECT * FROM books_csv_delta

-- COMMAND ----------

DESCRIBE EXTENDED books_csv_delta

-- COMMAND ----------

-- NOT SUPPORTED IN SERVELSS
REFRESH TABLE books_csv_delta;

-- COMMAND ----------

--SELECT * FROM books_csv

-- COMMAND ----------

-- DESCRIBE EXTENDED books_csv

-- COMMAND ----------

-- REFRESH TABLE books_csv;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### This operation reads the Delta table `books_csv_delta` and writes its contents as CSV files into the target path.
-- MAGIC
-- MAGIC It demonstrates how to export Delta data into a more generic format using `.write` with options for header, delimiter, and output mode.  
-- MAGIC
-- MAGIC This is mainly didactic: 
-- MAGIC
-- MAGIC Delta remains the recommended storage format, but exporting to CSV shows interoperability with external tools and practice with write options.  
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC         .table("books_csv_delta")
-- MAGIC       .write
-- MAGIC         .mode("append")
-- MAGIC         .format("csv")
-- MAGIC         .option('header', 'true')
-- MAGIC         .option('delimiter', ';')
-- MAGIC         .save(f"{dataset_bookstore}/books-csv"))
-- MAGIC
-- MAGIC
-- MAGIC #############
-- MAGIC #(spark.read
-- MAGIC #        .table("books_csv")
-- MAGIC #      .write
-- MAGIC #        .mode("append")
-- MAGIC #        .format("csv")
-- MAGIC #        .option('header', 'true')
-- MAGIC #        .option('delimiter', ';')
-- MAGIC #        .save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

--SELECT COUNT(*) FROM books_csv
SELECT COUNT(*) FROM books_csv_delta

-- COMMAND ----------

--NOT SUPPORTED IN FREE EDITION
--REFRESH TABLE books_csv
REFRESH TABLE books_csv_delta

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a table from CSV using a temporary view
-- MAGIC
-- MAGIC In Free/Serverless Databricks, `CREATE TABLE ... USING CSV` may fail.  
-- MAGIC A reliable workaround is:  
-- MAGIC 1. Create a **temporary view** over the CSV files with the expected schema.  
-- MAGIC 2. Use `CREATE TABLE AS SELECT` (CTAS) from the temp view to persist data as a managed table.  
-- MAGIC
-- MAGIC This ensures CSV data is properly loaded and queryable as a Delta table.
-- MAGIC

-- COMMAND ----------

CREATE TEMP VIEW books_tmp_vw
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset_bookstore}/books-csv/export_*.csv",
  --path = "${dataset.bookstore}/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);

CREATE TABLE books AS
  SELECT * FROM books_tmp_vw;
  
SELECT * FROM books

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT * FROM csv.`${dataset_bookstore}/books-csv`;
--SELECT * FROM csv.`${dataset.bookstore}/books-csv`;

SELECT * FROM books_unparsed;

-- COMMAND ----------

DESCRIBE EXTENDED books

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4.2 FROM JSON

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###⚠️ Behavior of CTAS with JSON in Free/Serverless Databricks
-- MAGIC
-- MAGIC - Queries on **individual JSON files** (`SELECT * FROM json.\`path/file.json\``) fail, since direct queries on single files are not supported.  
-- MAGIC - Queries on **directories of JSON files** (`SELECT * FROM json.\`path/folder\``) work correctly.  
-- MAGIC - `CREATE TABLE AS SELECT` from JSON works only when pointing to a folder, because the data is materialized as a Delta table.  
-- MAGIC
-- MAGIC As a result, managed tables cannot persist a single JSON file, but can be created from a JSON dataset (folder with multiple files).
-- MAGIC

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM json.`${dataset_bookstore}/customers-json`;
--SELECT * FROM json.`${dataset.bookstore}/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Persisting JSON data as Delta
-- MAGIC
-- MAGIC JSON files cannot be stored directly as managed tables (`CREATE TABLE ... USING JSON` is not supported).  
-- MAGIC
-- MAGIC Workaround:  
-- MAGIC 1. Load JSON files into a Spark DataFrame (`spark.read.json`).  
-- MAGIC 2. Write the DataFrame as a Delta table (`.write.format("delta").saveAsTable(...)`).  
-- MAGIC
-- MAGIC This way, JSON remains the ingestion format but queries run on top of Delta.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read JSON files directly into a DataFrame
-- MAGIC df = (spark.read
-- MAGIC     .option("multiline", "true")   # optional, in case JSON lines are multiline
-- MAGIC     .json("/Volumes/workspace/default/bookstore_dataset/customers-json/export_*.json")
-- MAGIC )
-- MAGIC
-- MAGIC # Save as Delta table (overwrite if exists)
-- MAGIC df.write.format("delta").mode("overwrite").saveAsTable("customers_delta")
-- MAGIC
