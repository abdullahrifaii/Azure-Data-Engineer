# Databricks notebook source
# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

df_trip=read_csv_df("/Volumes/cdudevcatalog/bronze/datavol/nyctaxi/nyctaxi.csv")
display(df_trip)


# COMMAND ----------

df_location=read_csv_df("/Volumes/cdudevcatalog/bronze/datavol/nyctaxi/nyclocations.csv")
display(df_location)

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

# COMMAND ----------

df_trip.write.format("csv").option("path","abfss://mycontainer@cduadlsdev.dfs.core.windows.net/tripbucketed").bucketBy(5,"PUlocationID").saveAsTable("cdudevcatalog.bronze.tripbucketed")

# COMMAND ----------

df_trip_bucketed = spark.table("cdudevcatalog.bronze.tripbucketed")

# COMMAND ----------

df_joined = df_trip_bucketed.join(df_location, df_trip_bucketed.PUlocationID == df_location.PUlocationID,"inner")
display(df_joined)


# COMMAND ----------

df_joined.explain(True)

# COMMAND ----------

df_location.write.format("csv").option("path","abfss://mycontainer@cduadlsdev.dfs.core.windows.net/locationbucketed").bucketBy(5,"PUlocationID").saveAsTable("cdudevcatalog.bronze.locationbucketed")

# COMMAND ----------

df_location_bucketed = spark.table("cdudevcatalog.bronze.locationbucketed")

# COMMAND ----------

df_joined2 = df_trip_bucketed.join(df_location_bucketed, df_trip_bucketed.PUlocationID == df_location_bucketed.PUlocationID,"inner")
display(df_joined2)


# COMMAND ----------

df_joined2.explain(True)

# COMMAND ----------

df_location.write.format("csv").option("path","abfss://mycontainer@cduadlsdev.dfs.core.windows.net/locationbucketed2").bucketBy(10,"PUlocationID").saveAsTable("cdudevcatalog.bronze.locationbucketed2")

# COMMAND ----------

df_location_bucketed2 = spark.table("cdudevcatalog.bronze.locationbucketed2")

# COMMAND ----------

df_joined3 = df_trip_bucketed.join(df_location_bucketed2, df_trip_bucketed.PUlocationID == df_location_bucketed2.PUlocationID,"inner")
display(df_joined3)


# COMMAND ----------

df_joined3.explain(True)

# COMMAND ----------


