# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

spark.sql("USE CATALOG workspace")
spark.sql("USE DATABASE default")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Stream

# COMMAND ----------

# Read the streaming books table and create/update a temp view for downstream streaming queries
(
  spark.readStream
       .table("workspace.default.books")
       .createOrReplaceTempView("books_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Displaying Streaming Data

# COMMAND ----------

# Display the streaming books data from the temp view (checkpoint required for streaming display)
books_streaming_df = spark.sql("SELECT * FROM books_streaming_tmp_vw")
display(books_streaming_df, checkpointLocation = f"{checkpoints_bookstore}/tmp/books_streaming_{time.time()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying Transformations

# COMMAND ----------

# Display author counts from the streaming temp view (checkpoint required for streaming display)
author_counts_df = spark.sql("""SELECT author, count(book_id) AS total_books
                                  FROM books_streaming_tmp_vw
                                  GROUP BY author""")
display(author_counts_df, checkpointLocation = f"{checkpoints_bookstore}/tmp/author_counts_{time.time()}")

# COMMAND ----------

# Author: streaming aggregation -> Delta (complete mode)
(author_counts_df
  .writeStream
  .trigger(availableNow=True)                         # Serverless-friendly
  .outputMode("complete")                             # required for aggregations without watermark
  .option("checkpointLocation", f"{checkpoints_bookstore}/ui/author_counts")
  .table("workspace.default.author_counts_ui")
  .awaitTermination()
)

# Now, display in batch (sorting is only supported on batch tables, not streaming DataFrames)
display(spark.table("workspace.default.author_counts_ui").orderBy("author"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unsupported Operations

# COMMAND ----------

#sorted_books_df = books_streaming_df.orderBy("author")
#
#(sorted_books_df.writeStream
#                .option("checkpointLocation", f"{checkpoints_bookstore}/sorted_books")
#                .trigger(availableNow=True)
#                .format("console")
#                .start()
#)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Supported Operations

# COMMAND ----------

# create/update the temp view just before to avoid "not found" errors
spark.sql("""
CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS
SELECT author, COUNT(book_id) AS total_books
FROM books_streaming_tmp_vw
GROUP BY author
""")

# Write the aggregated author counts to a batch table using availableNow trigger
(spark.table("author_counts_tmp_vw")
  .writeStream
  .trigger(availableNow=True)
  .outputMode("complete")
  .option("checkpointLocation", f"{checkpoints_bookstore}/author_counts")
  .table("workspace.default.author_counts")
  .awaitTermination())

# COMMAND ----------

# snapshot of the stream
(spark.table("books_streaming_tmp_vw")
  .writeStream
  .trigger(availableNow=True)
  .option("checkpointLocation", f"{checkpoints_bookstore}/books_snapshot")
  .table("workspace.default.books_snapshot")
  .awaitTermination())

# now, sort in batch (sorting is only supported on batch tables, not streaming DataFrames)
display(spark.table("workspace.default.books_snapshot").orderBy("author"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persisting Streaming Data

# COMMAND ----------

(
  spark.table("author_counts_tmp_vw")
       .writeStream
       .trigger(availableNow=True)
       .outputMode("complete")
       .option("checkpointLocation", f"{checkpoints_bookstore}/author_counts")
       .table("workspace.default.author_counts")
       .awaitTermination()
)

(
  spark.table("books_streaming_tmp_vw")
       .writeStream
       .trigger(availableNow=True)
       .option("checkpointLocation", "/Volumes/workspace/default/bookstore_checkpoints/books_snapshot")
       .table("workspace.default.books_snapshot")
       .awaitTermination()
)
