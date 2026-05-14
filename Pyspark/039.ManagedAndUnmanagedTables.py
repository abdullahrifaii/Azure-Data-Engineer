# Databricks notebook source
# MAGIC %run /Workspace/Users/aboudeh.rifaii@gmail.com/Azure-Data-Engineer/Pyspark/Utilities

# COMMAND ----------

df = read_csv_df(
    "/Volumes/mycatalog/default/myvolume/Databricks_Practice/Orders_new.csv",
    infer_schema=True
)
display(df)

# COMMAND ----------

df.write.saveAsTable("mycatalog.default.orders_new")

# COMMAND ----------

df.write.option("path", "abfss://mycontainer@cduadlsdev.dfs.core.windows.net/Orders").saveAsTable("cdudevcatalog.bronze.orders_unmanaged")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table cdudevcatalog.bronze.orders_new2

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table cdudevcatalog.bronze.orders_unmanaged

# COMMAND ----------

df_orders=spark.table("cdudevcatalog.bronze.orders_new")
display(df_orders)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cdudevcatalog.bronze.orders_new

# COMMAND ----------


