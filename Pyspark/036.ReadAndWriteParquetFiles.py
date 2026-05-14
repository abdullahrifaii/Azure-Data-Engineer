# Databricks notebook source
# MAGIC %run /Workspace/Users/aboudeh.rifaii@gmail.com/Azure-Data-Engineer/Pyspark/Utilities

# COMMAND ----------

df = read_csv_df(
    "/Volumes/mycatalog/default/myvolume/Databricks_Practice/Orders_new.csv",
    infer_schema=True
)
display(df)

# COMMAND ----------

df.write.format("parquet").option("path","/Volumes/mycatalog/default/myvolume/outputpqr").save()

# COMMAND ----------

df_parquet = spark.read.format("parquet").option("path","/Volumes/mycatalog/default/myvolume/outputpqr/part-00000-tid-2617427713803847259-4f7223ce-2166-4641-84d2-a81cd637b3f2-216-1.c000.snappy.parquet").load()
display(df_parquet)

# COMMAND ----------

display(spark.read.text("/Volumes/mycatalog/default/myvolume/outputpqr/part-00000-tid-2617427713803847259-4f7223ce-2166-4641-84d2-a81cd637b3f2-216-1.c000.snappy.parquet"))

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets"))

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/nyctaxi/tripdata/fhv"))

# COMMAND ----------

df_trip=read_csv_df("/databricks-datasets/nyctaxi/tripdata/fhv")
display(df_trip)

# COMMAND ----------

df_trip.count()

# COMMAND ----------

df_csv_size=dbutils.fs.ls("/Volumes/cdudevcatalog/bronze/datavol/tripdata/")
display(df_csv_size)             

# COMMAND ----------

df_trip.write.format("parquet").option("path","/Volumes/mycatalog/default/myvolume/tripdata_parquet").save()

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/mycatalog/default/myvolume/tripdata_parquet/"))

# COMMAND ----------

df_trip_csv = read_csv_df("/Volumes/cdudevcatalog/bronze/datavol/tripdata/")
display(df_trip_csv)

# COMMAND ----------

df_trip_parquet = spark.read.parquet("/Volumes/mycatalog/default/myvolume/tripdata_parquet/")
display(df_trip_parquet)

# COMMAND ----------


