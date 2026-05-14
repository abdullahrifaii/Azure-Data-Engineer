# Databricks notebook source
# MAGIC %run /Workspace/Users/aboudeh.rifaii@gmail.com/Azure-Data-Engineer/Pyspark/Utilities

# COMMAND ----------

df_trip_parquet = spark.read.parquet("/Volumes/mycatalog/default/myvolume/tripdata_parquet/")
display(df_trip_parquet)

# COMMAND ----------

display(spark.createDataFrame([(f,) for f in df_trip_parquet.inputFiles()], schema=['file_path']))

# COMMAND ----------

df_trip_parquet.rdd.getNumPartitions()

# COMMAND ----------

spark.conf.get("spark.sql.files.maxPartitionBytes")

# COMMAND ----------

134217728/1024/1024/1024*249

# COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes",512*1024*1024)

# COMMAND ----------

spark.conf.get("spark.sql.files.maxPartitionBytes")

# COMMAND ----------

df_trip_csv2 = read_csv_df("/Volumes/cdudevcatalog/bronze/datavol/tripdata/")
display(df_trip_csv2)

# COMMAND ----------

df_trip_csv2.rdd.getNumPartitions()

# COMMAND ----------

536870912/1024/1024/1024*65

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id

# COMMAND ----------

display(df_trip_csv2.groupBy(spark_partition_id()).count())
