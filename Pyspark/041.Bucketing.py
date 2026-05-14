# Databricks notebook source
# MAGIC %run /Workspace/Users/aboudeh.rifaii@gmail.com/Azure-Data-Engineer/Pyspark/Utilities

# COMMAND ----------

df = read_csv_df(
    "/Volumes/mycatalog/default/myvolume/Databricks_Practice/Orders_new.csv",
    infer_schema=True
)
display(df)

# COMMAND ----------

df.write.option("path","/Volumes/mycatalog/default/myvolume/bucketing").bucketBy(5,"order_id").save()

# COMMAND ----------

df.write.bucketBy(5,"order_id").saveAsTable("orders_bucketed")

# COMMAND ----------

df.write.format("parquet").bucketBy(5,"order_id").saveAsTable("orders_bucketed")

# COMMAND ----------

df.write.format("csv").option("path","abfss://mycontainer@cduadlsdev.dfs.core.windows.net/OrdersBucketed").bucketBy(5,"order_id").saveAsTable("cdudevcatalog.bronze.orders_bucketed")

# COMMAND ----------

df.write.format("csv").option("path","abfss://mycontainer@cduadlsdev.dfs.core.windows.net/OrdersBucketed2").partitionBy("year").bucketBy(5,"order_id").saveAsTable("cdudevcatalog.bronze.orders_bucketed2")

# COMMAND ----------

🔹 Core Idea

Instead of randomly distributing data across files, Spark:

Applies a hash function on a column (or columns)
Assigns each row to a bucket number
Writes data into N bucket files

So if you say “8 buckets”, Spark creates 8 files and distributes rows deterministically.

🔹 How it Works Internally

For a bucket column like user_id:

Spark computes:

bucket_number = hash(user_id) % num_buckets
All rows with the same user_id will always go to the same bucket
