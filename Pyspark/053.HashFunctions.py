# Databricks notebook source
# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

df_emp=read_csv_df("/Volumes/cdudevcatalog/bronze/datavol/Employees.csv")
display(df_emp)


# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

help(hash) #32 bit

# COMMAND ----------

df_emp_hash = df_emp.withColumn("HashKey",hash("id"))
display(df_emp_hash)

# COMMAND ----------

df_emp_hash2 = df_emp.withColumn("HashKey",hash("id","name"))
display(df_emp_hash2)

# COMMAND ----------

help(md5)

# COMMAND ----------

df_emp_hash3 = df_emp.withColumn("HashKey",md5("email"))
display(df_emp_hash3)

# COMMAND ----------

df_emp_hash4 = df_emp.withColumn("HashKey",md5(concat("country","email")))
display(df_emp_hash4)

# COMMAND ----------

help(sha1)

# COMMAND ----------

df_emp_hash5 = df_emp.withColumn("HashKey",sha1("email"))
display(df_emp_hash5)

# COMMAND ----------

df_emp_hash5 = df_emp.withColumn("HashKey",sha1(concat("email","name")))
display(df_emp_hash5)

# COMMAND ----------

help(sha2)

# COMMAND ----------

df_emp_hash6 = df_emp.withColumn("HashKey",sha2("email",256))
display(df_emp_hash6)

# COMMAND ----------

df_emp_hash7 = df_emp.withColumn("HashKey",sha2(concat("email","name"),256))
display(df_emp_hash7)

# COMMAND ----------

help(xxhash64)

# COMMAND ----------

df_emp_hash8 = df_emp.withColumn("HashKey",xxhash64("email"))
display(df_emp_hash8)

# COMMAND ----------

df_emp_hash9 = df_emp.withColumn("HashKey",xxhash64("email","name"))
display(df_emp_hash9)

# COMMAND ----------


