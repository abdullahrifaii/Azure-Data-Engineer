# Databricks notebook source
# MAGIC %run /Workspace/Users/aboudeh.rifaii@gmail.com/Azure-Data-Engineer/Pyspark/Utilities

# COMMAND ----------

df = read_csv_df(
    "/Volumes/mycatalog/default/myvolume/Databricks_Practice/Orders_new.csv",
    infer_schema=True
)
display(df)

# COMMAND ----------

df.write.format("parquet").save("/Volumes/cdudevcatalog/bronze/datavol/parquetfiles")

# COMMAND ----------

df.write.save("/Volumes/mycatalog/default/myvolume/delta")

# COMMAND ----------

df.write.format("delta").save("/Volumes/cdudevcatalog/bronze/datavol/delta")

# COMMAND ----------

df_log = spark.read.json("/Volumes/cdudevcatalog/bronze/datavol/delta/_delta_log/00000000000000000000.json")
display(df_log)

# COMMAND ----------


