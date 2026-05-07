# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help("ls")

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/ADE202601/bronze/voldata"))

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/ADE202601/bronze/voldata/data"))

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/ADE202601/bronze/voldata/data/Employees_Wind.csv"))

# COMMAND ----------

dbutils.fs.help("mkdirs")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/ade202601/bronze/voldata/myfolder")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/ade202601/bronze/voldata/myfolder2/data")

# COMMAND ----------

dbutils.fs.rm("/Volumes/ade202601/bronze/voldata/myfolder2/data")

# COMMAND ----------

dbutils.fs.rm("/Volumes/ade202601/bronze/voldata/myfolder")

# COMMAND ----------

dbutils.fs.rm("/Volumes/ade202601/bronze/voldata/myfolder",recurse=True)

# COMMAND ----------

dbutils.fs.rm("/Volumes/ade202601/bronze/voldata/test/Employees_Wind.csv")

# COMMAND ----------

dbutils.fs.cp("/Volumes/ade202601/bronze/voldata/data","/Volumes/ade202601/bronze/voldata/test/",recurse=True)

# COMMAND ----------

dbutils.fs.mv("/Volumes/ade202601/bronze/voldata/test","/Volumes/ade202601/bronze/voldata/myfolder2/",recurse=True)

# COMMAND ----------


