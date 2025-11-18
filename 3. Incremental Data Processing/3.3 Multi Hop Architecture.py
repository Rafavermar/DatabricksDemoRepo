# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col, count, date_trunc

# COMMAND ----------

# MAGIC  %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploring The Source Directory
# MAGIC

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{checkpoints_bookstore}/orders_raw")
    .load(f"{dataset_bookstore}/orders-raw")
    .createOrReplaceTempView("orders_raw_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enriching Raw Data

# COMMAND ----------

spark.sql("SELECT * FROM orders_raw_temp") \
     .withColumn("ingestion_time", current_timestamp()) \
     .createOrReplaceTempView("orders_tmp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Bronze Table

# COMMAND ----------

def process_bronze():
      query = (spark.table("orders_tmp")
                    .writeStream
                    .format("delta")
                    .option("checkpointLocation", f"{checkpoints_bookstore}/orders_bronze")
                    .outputMode("append")
                    .trigger(availableNow=True) # we use trigger AvailableNow as Trigger type ProcessingTime is not supported for Serverless compute.
                    .table("orders_bronze"))
      
      query.awaitTermination()

process_bronze()

# COMMAND ----------

load_new_data()

process_bronze() # Rerun the Bronze layer process to ingest the new data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating Static Lookup Table

# COMMAND ----------

(spark.read
      .format("json")
      .load(f"{dataset_bookstore}/customers-json")
      .createOrReplaceTempView("customers_lookup"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Silver Table

# COMMAND ----------

(spark.readStream
  .table("orders_bronze")
  .createOrReplaceTempView("orders_bronze_tmp"))

# COMMAND ----------

def transform_silver():
    (spark.table("orders_bronze_tmp")
        .join(spark.table("customers_lookup"), on="customer_id", how="left")
        .createOrReplaceTempView("orders_enriched_tmp"))

# COMMAND ----------

def process_silver():
      transform_silver()
      query = (spark.table("orders_enriched_tmp")
                    .writeStream
                    .format("delta")
                    .option("checkpointLocation", f"{checkpoints_bookstore}/orders_silver")
                    .outputMode("append")
                    .trigger(availableNow=True) # we use trigger AvailableNow as Trigger type ProcessingTime is not supported for Serverless compute.
                    .table("orders_silver"))
      
      query.awaitTermination()

process_silver()

# COMMAND ----------

load_new_data()

process_bronze()
process_silver()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Gold Table

# COMMAND ----------

(spark.readStream
  .table("orders_silver")
  .createOrReplaceTempView("orders_silver_tmp"))

# COMMAND ----------

def transform_gold():
    (spark.table("orders_silver_tmp")
        .withColumn("order_timestamp_ts", (col("order_timestamp") / 1000).cast("timestamp"))
        .withColumn("order_date", date_trunc("dd", col("order_timestamp_ts")))
        .drop("order_timestamp_ts")
        .groupBy("customer_id", "order_date", "books")
        .agg(count("*").alias("quantity"))
        .createOrReplaceTempView("daily_customer_books_tmp"))

# COMMAND ----------

def process_gold():
      transform_gold()
      query = (spark.table("daily_customer_books_tmp")
                    .writeStream
                    .format("delta")
                    .outputMode("complete")
                    .option("checkpointLocation", f"{checkpoints_bookstore}/daily_customer_books")
                    .trigger(availableNow=True)
                    .table("daily_customer_books"))
      
      query.awaitTermination()

process_gold()

# COMMAND ----------


load_new_data()

process_bronze()
process_silver()
process_gold()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stopping active streams

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()
