# Databricks notebook source
# MAGIC  %run ../Includes/Copy-Datasets

# COMMAND ----------

# List files in the raw orders data directory to verify new data ingestion
files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# Ingest new Parquet files from the raw orders data directory using Auto Loader (cloudFiles) and write to the 'orders_updates' table.
(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{checkpoints_bookstore}/orders")
        .load(f"{dataset_bookstore}/orders-raw")
      .writeStream
        .trigger(availableNow=True) # we use trigger AvailableNow as Trigger type ProcessingTime is not supported for Serverless compute.
        .option("checkpointLocation", f"{checkpoints_bookstore}/orders")
        .table("orders_updates")
)

# COMMAND ----------

# Load new data into the orders_updates table using Auto Loader and trigger AvailableNow
load_new_data()

# COMMAND ----------

# List files in the raw orders data directory to verify new data ingestion
files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

dbutils.fs.rm(f"{checkpoints_bookstore}/orders", True)
