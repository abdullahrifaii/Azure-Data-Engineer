# Databricks notebook source
# MAGIC %sql
# MAGIC select * from csv.`/Volumes/cdudevcatalog/bronze/datavol/data/Orders_new.csv`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from read_files(
# MAGIC   '/Volumes/cdudevcatalog/bronze/datavol/data/Orders_new.csv',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   inferSchema => true)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`/Volumes/cdudevcatalog/bronze/datavol/parquetfiles/part-00000-tid-18981933710016264-5dc4d2ab-0bf4-4a17-83a9-f49e6bddd082-242-1.c000.snappy.parquet`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/cdudevcatalog/bronze/datavol/delta/`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`/Volumes/cdudevcatalog/bronze/datavol/delta/_delta_log/00000000000000000000.json`
