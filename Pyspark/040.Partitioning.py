# Databricks notebook source
# MAGIC %run /Workspace/Users/aboudeh.rifaii@gmail.com/Azure-Data-Engineer/Pyspark/Utilities

# COMMAND ----------

df = read_csv_df(
    "/Volumes/mycatalog/default/myvolume/Databricks_Practice/Orders_new.csv",
    infer_schema=True
)
display(df)

# COMMAND ----------

# DBTITLE 1,Cell 3
df.write.format("delta").mode("overwrite").option("path","/Volumes/mycatalog/default/myvolume/Output/").partitionBy("year").save()

# COMMAND ----------

df_year = spark.read.format("parquet").option("path","/Volumes/mycatalog/default/myvolume/Output/year=2024/part-00000-b39ad0aa-b149-424f-bdea-dc900aab7a3c.c000.snappy.parquet").load()
display(df_year)

# COMMAND ----------

df_year = spark.read.format("parquet").option("path","/Volumes/cdudevcatalog/bronze/datavol/PartitionData/year=2024/part-00000-0e3bd5d9-753d-40b0-a37d-6c4732fe7414.c000.snappy.parquet").load()
display(df_year)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

df_year = spark.read.format("parquet").option("path","/Volumes/cdudevcatalog/bronze/datavol/PartitionData/year=2024/part-00000-0e3bd5d9-753d-40b0-a37d-6c4732fe7414.c000.snappy.parquet").load()
display(df_year)

# COMMAND ----------

df.write.option("path","/Volumes/cdudevcatalog/bronze/datavol/PartitionData2").partitionBy("year","product").save()

# COMMAND ----------

df.write.partitionBy("year").saveAsTable("cdudevcatalog.bronze.orders_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cdudevcatalog.bronze.orders_partitioned where year = 2025

# COMMAND ----------

_sqldf.explain(True)

# COMMAND ----------


