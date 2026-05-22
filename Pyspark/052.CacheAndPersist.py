# Databricks notebook source
# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

df_trip=read_csv_df("/Volumes/cdudevcatalog/bronze/datavol/nyctaxi/nyctaxi.csv")
display(df_trip)


# COMMAND ----------

df_trip_cols = df_trip.select("Pickup_DateTime","DropOff_datetime","PUlocationID","DOlocationID","Dispatching_base_number")

# COMMAND ----------

df_trip_cols.cache()

# COMMAND ----------

df_trip_cols.is_cached

# COMMAND ----------

df_trip_cols.explain(True)

# COMMAND ----------

df_trip_cols.count()

# COMMAND ----------

df_trip_cols.is_cached

# COMMAND ----------

df_trip_cols.unpersist()

# COMMAND ----------

from pyspark.storagelevel import StorageLevel

# COMMAND ----------

help(StorageLevel)

# COMMAND ----------

df_trip_cols.persist(StorageLevel.MEMORY_ONLY)

# COMMAND ----------

df_trip_cols.count()

# COMMAND ----------

df_trip_cols.unpersist()

# COMMAND ----------

df_trip_cols.persist(StorageLevel.MEMORY_AND_DISK)

# COMMAND ----------

df_trip_cols.count()

# COMMAND ----------

df_trip_cols.unpersist()

# COMMAND ----------

df_trip_cols.persist(StorageLevel.DISK_ONLY)

# COMMAND ----------

df_trip_cols.count()

# COMMAND ----------

df_trip_cols.unpersist()

# COMMAND ----------

df_trip_cols.persist(StorageLevel.MEMORY_ONLY_2)

# COMMAND ----------

df_trip_cols.count()

# COMMAND ----------

df_trip_cols.unpersist()

# COMMAND ----------

df_trip_cols.persist(StorageLevel.DISK_ONLY_3 )

# COMMAND ----------

df_trip_cols.count()

# COMMAND ----------

df_trip_cols.unpersist()

# COMMAND ----------


