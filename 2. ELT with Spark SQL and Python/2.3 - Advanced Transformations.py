# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

dbutils.widgets.text("dataset_bookstore", dataset_bookstore)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parsing JSON Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE customers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Accessing nested fields in a JSON string column using colon (:) syntax
# MAGIC SELECT customer_id, profile:first_name, profile:address:country 
# MAGIC FROM customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT profile 
# MAGIC FROM customers 
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Parse the JSON string in the 'profile' column into a struct using a schema inferred from an example JSON object
# MAGIC CREATE OR REPLACE TEMP VIEW parsed_customers AS
# MAGIC   SELECT customer_id, from_json(profile, schema_of_json('{"first_name":"Thomas","last_name":"Lane","gender":"Male","address":{"street":"06 Boulevard Victor Hugo","city":"Paris","country":"France"}}')) AS profile_struct
# MAGIC   FROM customers;
# MAGIC   
# MAGIC SELECT * FROM parsed_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE parsed_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Accessing fields from a parsed JSON struct using dot (.) notation
# MAGIC SELECT customer_id, profile_struct.first_name, profile_struct.address.country
# MAGIC FROM parsed_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Expanding the profile_struct fields into top-level columns for easier access
# MAGIC CREATE OR REPLACE TEMP VIEW customers_final AS
# MAGIC   SELECT customer_id, profile_struct.*
# MAGIC   FROM parsed_customers;
# MAGIC   
# MAGIC SELECT * FROM customers_final

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, customer_id, books
# MAGIC FROM orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explode Function

# COMMAND ----------

# MAGIC %sql
# MAGIC -- explode(books) expands each book in the books array into a separate row
# MAGIC SELECT order_id, customer_id, explode(books) AS book 
# MAGIC FROM orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collecting Rows

# COMMAND ----------

# MAGIC %sql
# MAGIC -- collect_set aggregates unique values into arrays per group (here: orders and book_ids per customer)
# MAGIC SELECT customer_id,
# MAGIC   collect_set(order_id) AS orders_set,
# MAGIC   collect_set(books.book_id) AS books_set
# MAGIC FROM orders
# MAGIC GROUP BY customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC ##Flatten Arrays

# COMMAND ----------

# MAGIC %sql
# MAGIC -- collect_set(books.book_id) returns a set of arrays of book_ids per customer
# MAGIC -- flatten merges nested arrays into a single array, array_distinct removes duplicates
# MAGIC SELECT customer_id,
# MAGIC   collect_set(books.book_id) AS before_flatten,
# MAGIC   array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
# MAGIC FROM orders
# MAGIC GROUP BY customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC ##Join Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This view joins each exploded book in an order with its details from the books table
# MAGIC CREATE OR REPLACE VIEW orders_enriched AS
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT *, explode(books) AS book 
# MAGIC   FROM orders) o
# MAGIC INNER JOIN books b
# MAGIC ON o.book.book_id = b.book_id;
# MAGIC
# MAGIC SELECT * FROM orders_enriched

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This view loads new order data from a Parquet file for comparison and merging
# MAGIC CREATE OR REPLACE TEMP VIEW orders_updates
# MAGIC AS SELECT * FROM parquet.`${dataset_bookstore}/orders-new`;
# MAGIC
# MAGIC -- UNION combines all rows from both tables, removing duplicates
# MAGIC SELECT * FROM orders 
# MAGIC UNION 
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INTERSECT returns rows that are present in both orders and orders_updates
# MAGIC SELECT * FROM orders 
# MAGIC INTERSECT 
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MINUS returns rows that are present in orders but not in orders_updates
# MAGIC SELECT * FROM orders 
# MAGIC MINUS 
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reshaping Data with Pivot

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE transactions AS
# MAGIC
# MAGIC SELECT * FROM (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     book.book_id AS book_id,
# MAGIC     book.quantity AS quantity
# MAGIC   FROM orders_enriched
# MAGIC ) PIVOT (
# MAGIC   sum(quantity) FOR book_id in (
# MAGIC     'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
# MAGIC     'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
# MAGIC   )
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM transactions
