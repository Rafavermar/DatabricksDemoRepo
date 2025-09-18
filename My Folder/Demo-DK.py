# Databricks notebook source
# MAGIC %md
# MAGIC ### Set some default variables for the notebook

# COMMAND ----------

catalog_name = 'samples'
schema_name = 'tpch'

# COMMAND ----------

print(catalog_name)

# COMMAND ----------

print(schema_name)

# COMMAND ----------

print(f"{catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### List accessable tables

# COMMAND ----------

# spark.sql allows the use of variables
tables = spark.sql(f"""SHOW TABLES IN {catalog_name}.{schema_name}""")
display(tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query a table via SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The notation for comments depends on the used language
# MAGIC select * from samples.tpch.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC # MODIFIED - using variables in pure sql not possible

# COMMAND ----------

#%sql
#select * from {catalog_name}.{schema_name}.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## NEW - Deprecated Method
# MAGIC SET inside SQL Magic

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Deprecated
# MAGIC
# MAGIC --SET mySchema = tpch;
# MAGIC --SET catalog_name = samples
# MAGIC --SELECT * FROM ${catalog_name}.${mySchema}.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## NEW - Using dbutils.widgets.dropdown + Query as FString +  spark.sql

# COMMAND ----------

# Creating a dropdown parameters selector withtin the notebook
dbutils.widgets.dropdown("catalog_name", "samples", ["samples", "hr", "default"], "Choose a catalog")
dbutils.widgets.dropdown("schema_name", "tpch", ["tpch", "samples", "hr"], "Choose a schema")


# COMMAND ----------

# Notebook Parameters can be defined as text too

#dbutils.widgets.text("catalog_name", "samples")
#dbutils.widgets.text("schema_name", "tpch")

# Getting the values
#catalog_name = dbutils.widgets.get("catalog_name")
#schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

# Defining de query as FString
query = f"""
select * from {catalog_name}.{schema_name}.orders
"""

# Showing and passing the query as an argument for the display function containing the query invoked
display(
  spark.sql(query)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using SparkSQL to query the table

# COMMAND ----------

spark.sql(f"""
          
          select * from {catalog_name}.{schema_name}.orders;
          
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding .display() method

# COMMAND ----------

spark.sql(f"""
          
          select * from {catalog_name}.{schema_name}.orders;
          
          """).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## create a dataframe via spark.sql

# COMMAND ----------

# df -> naming for dataframes good practice
df_orders = spark.sql(f"""
                      select * from {catalog_name}.{schema_name}.orders;
                       """)

# COMMAND ----------

df_orders.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## NEW - Convert Spark DF to a RDD (Resilient Distributed Dataset) NOT AVAILABLE WITH SERVERLESS COMPUTE
# MAGIC Not often used but could be helpful for:
# MAGIC - low level transformation control
# MAGIC - operations not supported by DataFames
# MAGIC - Non structured procesing
# MAGIC - work with functional API like map, flatmap, reduceByKey

# COMMAND ----------

# convert DF to RDD
#rdd_orders = df_orders.rdd

# Counting words on column "o_comment"
#word_counts = (
#    rdd_orders
#    .flatMap(lambda row: row.o_comment.split(" "))
#    .map(lambda word: (word, 1))
#    .reduceByKey(lambda a, b: a + b)
#)
#
#print(word_counts.take(10))

# COMMAND ----------

display(df_orders_processed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MODIFIED - example of applying multiple methods to one dataframe & using built-in pyspark.sql.functions
# MAGIC
# MAGIC See the official pySpark Documentation:
# MAGIC For methods that can be used on dataframes: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html
# MAGIC
# MAGIC For Built-In pySpark Functions: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.datediff.html

# COMMAND ----------

from pyspark.sql.functions import datediff, current_date

df_orders_processed = df_orders.filter(df_orders.o_totalprice > 1000) \
                                .withColumnRenamed('o_orderkey', 'Order_BK') \
                                .withColumn('daysSinceOrder', datediff(current_date(), df_orders.o_orderdate))

display(df_orders_processed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding new calculated columns

# COMMAND ----------

from pyspark.sql.functions import year, when

# Adding new columns:
# - orderYear as year of o_orderdate
# - priceCategory as HIGH if o_totalprice > 5000, otherwise LOW

df_orders_processed = (
    df_orders_processed
    .withColumn("orderYear", year(df_orders_processed.o_orderdate))
    .withColumn("priceCategory", when(df_orders_processed.o_totalprice > 5000, "HIGH").otherwise("LOW"))
)

display(df_orders_processed)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Ceating a new grouped dataframe upon the df_orders_processed

# COMMAND ----------

# Grouping by o_orderstatus and aggregating by count(Order_BK) and avg(o_totalprice)

df_grouped = (
    df_orders_processed
    .groupBy("o_orderstatus")
    .agg(
        {"Order_BK": "count", "o_totalprice": "avg"}
    )
    .withColumnRenamed("count(Order_BK)", "numOrders")
)

display(df_grouped)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a new Top 10 orders dataframe by total price

# COMMAND ----------

# Top 10 orders by o_totalprice (descending)

df_top_orders = df_orders_processed.orderBy(df_orders_processed.o_totalprice.desc())
display(df_top_orders.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a new joined dataframe between df_orders_processed and df_customer by o_custkey and c_custkey (inner join)

# COMMAND ----------


df_customers = spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.customer")

df_joined_orders_customer = df_orders_processed.join(
    df_customers,
    df_orders_processed.o_custkey == df_customers.c_custkey,
    "inner"
)

display(df_joined_orders_customer.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC # MODIFIED - creating a tempview to use sql again

# COMMAND ----------

df_joined_orders_customer.createOrReplaceTempView('_joined_orders_customer')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from _joined_orders_customer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying directly from SQL - TempView of df_joined
# MAGIC Creating a temp view of the df_joined
# MAGIC querying the temp view to get the same results as above but using SQL

# COMMAND ----------

df_joined_orders_customer.createOrReplaceTempView("joined_orders_customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT orderYear, priceCategory, COUNT(*) as numOrders, AVG(o_totalprice) as avgPrice
# MAGIC FROM joined_orders_customer
# MAGIC GROUP BY orderYear, priceCategory
# MAGIC ORDER BY orderYear
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## NEW - Global Temp View (shared between notebooks, but still temporary until the shutdown of the cluster)

# COMMAND ----------

# Create a global temp view (NOT SUPPORTED WITH SERVERLESS)
# df_orders_processed.createOrReplaceGlobalTempView("orders_processed_global")

# COMMAND ----------

# Queryinhg the global temp view
%sql
#SELECT priceCategory, AVG(o_totalprice) as avgPrice
#FROM global_temp.orders_processed_global
#GROUP BY priceCategory

# COMMAND ----------

# MAGIC %md
# MAGIC ## NEW - Persistent view on the Catalog ( Unity or hive_metastore required)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hive Metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create or replace a view in the default hive metastore (Hive disabled in Community edition)
# MAGIC
# MAGIC --CREATE OR REPLACE VIEW hive_metastore.default.orders_processed_view AS
# MAGIC --SELECT *
# MAGIC --FROM _joined_orders_customer;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the view
# MAGIC
# MAGIC -- SELECT * FROM hive_metastore.default.orders_processed_view;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNITY CATALOG
# MAGIC Create a persistent table in the catalog in order to be referenced by CREATE OR REPLACE VIEW

# COMMAND ----------

df_joined_orders_customer.write.mode("overwrite").saveAsTable("workspace.default.joined_orders_customer")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- The table is already available from Unity Catalog
# MAGIC -- SELECT * FROM workspace.default.joined_orders_customer LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create or replace a view if we only need this table within this notebook
# MAGIC
# MAGIC CREATE OR REPLACE VIEW workspace.default.orders_processed_view AS
# MAGIC SELECT *
# MAGIC FROM workspace.default.joined_orders_customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the view
# MAGIC SELECT * FROM orders_processed_view LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## creating a customer dataframe

# COMMAND ----------

df_customer = spark.sql(f"""
                      select * from {catalog_name}.{schema_name}.customer;
                       """)

# COMMAND ----------

# MAGIC %sql
# MAGIC desc samples.tpch.customer
