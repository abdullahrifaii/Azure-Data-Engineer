# Databricks notebook source
# MAGIC %run /Workspace/Users/aboudeh.rifaii@gmail.com/Azure-Data-Engineer/Pyspark/Utilities

# COMMAND ----------

df_trip=read_csv_df("/Volumes/mycatalog/default/myvolume/Databricks_Practice/nyctaxi.csv")
display(df_trip)

# COMMAND ----------

df_location=read_csv_df("/Volumes/mycatalog/default/myvolume/Databricks_Practice/nyclocations.csv")
display(df_location)

# COMMAND ----------

df_joined = df_trip.join(df_location,df_trip.PUlocationID==df_location.PUlocationID,"inner")
display(df_joined)

# COMMAND ----------

df_joined.explain(True)

# COMMAND ----------

spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# COMMAND ----------

10485760/1024/1024

# COMMAND ----------

from pyspark.sql.functions import broadcast

# COMMAND ----------

df_joined2 = df_trip.join(broadcast(df_location),df_trip.PUlocationID==df_location.PUlocationID,"inner")
display(df_joined2)

# COMMAND ----------

df_joined2.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC A Broadcast Hash Join (BHJ) in PySpark is the fastest join strategy when one dataset is small enough to fit in memory.
# MAGIC
# MAGIC Instead of shuffling both datasets (like in Sort-Merge Join), Spark does something smarter.
# MAGIC
# MAGIC 🧠 Core Idea
# MAGIC
# MAGIC 👉 Send the small table to every executor, and then join locally using a hash lookup.
# MAGIC
# MAGIC ⚙️ How it works (step-by-step)
# MAGIC 1. Pick the smaller dataset
# MAGIC
# MAGIC Spark identifies the smaller DataFrame (or you explicitly tell it).
# MAGIC
# MAGIC 2. Broadcast it
# MAGIC The small dataset is copied to all worker nodes
# MAGIC No shuffle required for the large dataset
# MAGIC 3. Build a hash map
# MAGIC Each executor creates an in-memory hash table from the small dataset
# MAGIC 4. Scan the large dataset
# MAGIC Each partition of the large dataset:
# MAGIC Looks up matching rows in the hash table
# MAGIC Produces joined results

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold",100*1024*1024)

# COMMAND ----------

spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

# COMMAND ----------

df_joined3 = df_trip.join(df_location,df_trip.PUlocationID==df_location.PUlocationID,"inner")
display(df_joined3)

# COMMAND ----------

df_joined3.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC Core Idea
# MAGIC
# MAGIC Sort-Merge Join works in 3 main steps:
# MAGIC
# MAGIC 1. Shuffle both datasets
# MAGIC Spark redistributes (shuffles) both DataFrames across the cluster based on the join key.
# MAGIC Rows with the same key end up in the same partition.
# MAGIC
# MAGIC 👉 This is the expensive part (network + disk I/O).
# MAGIC
# MAGIC 2. Sort each partition
# MAGIC Inside each partition, Spark sorts data by the join key.
# MAGIC 3. Merge (like merge step of merge sort)
# MAGIC Spark scans both sorted partitions sequentially
# MAGIC Matches rows with the same join key efficiently
# MAGIC ⚙️ Example
# MAGIC df1.join(df2, on="id", how="inner")
# MAGIC
# MAGIC If:
# MAGIC
# MAGIC both df1 and df2 are large
# MAGIC no broadcast hint is used
# MAGIC
# MAGIC 👉 Spark will likely choose Sort-Merge Join
# MAGIC
# MAGIC 🔍 Why sorting helps
# MAGIC
# MAGIC Because both sides are sorted:
# MAGIC
# MAGIC df1:   1, 2, 3, 5
# MAGIC df2:   2, 3, 4, 5
# MAGIC
# MAGIC Spark can walk through them like:
# MAGIC
# MAGIC Compare 1 vs 2 → move forward
# MAGIC Compare 2 vs 2 → match
# MAGIC Compare 3 vs 3 → match
# MAGIC
# MAGIC 👉 No need for nested loops → much faster for big data
# MAGIC
# MAGIC 🚀 When Spark uses Sort-Merge Join
# MAGIC
# MAGIC Spark prefers SMJ when:
# MAGIC
# MAGIC Datasets are large
# MAGIC Join keys are sortable
# MAGIC spark.sql.join.preferSortMergeJoin = true (default)
# MAGIC No broadcast hint is applicable
# MAGIC ⚡ Pros
# MAGIC Scales well for very large datasets
# MAGIC Efficient for distributed systems
# MAGIC Works well with equi-joins (= conditions)
# MAGIC ⚠️ Cons
# MAGIC Shuffle is expensive
# MAGIC Sorting adds overhead
# MAGIC Slower than broadcast join for small datasets

# COMMAND ----------

spark.conf.get("spark.sql.join.preferSortMergeJoin")

# COMMAND ----------

spark.conf.set("spark.sql.join.preferSortMergeJoin",False)

# COMMAND ----------

spark.conf.get("spark.sql.join.preferSortMergeJoin")

# COMMAND ----------

df_joined4 = df_trip.join(df_location,df_trip.PUlocationID==df_location.PUlocationID,"inner")
display(df_joined3)

# COMMAND ----------

df_joined4.explain(True)

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold",10*1024*1024)

# COMMAND ----------

df_joined5 = df_trip.join(df_location)
display(df_joined5)

# COMMAND ----------

df_joined5.explain(True)

# COMMAND ----------

df_joined5.count()

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

# COMMAND ----------

df_joined6 = df_trip.join(df_location)
display(df_joined6)

# COMMAND ----------

df_joined6.explain(True)

# COMMAND ----------


