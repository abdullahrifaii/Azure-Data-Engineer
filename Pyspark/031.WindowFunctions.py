# Databricks notebook source
# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

df = read_csv_df(
  "/Volumes/ADE202601/bronze/voldata/data/Employees_Wind.csv",
    infer_schema=True
)
display(df)

# COMMAND ----------

df2 = spark.sql("""
SELECT *, row_number() OVER (ORDER BY Salary) AS rownum
FROM {df_name}
""",df_name = df)
display(df2)

# COMMAND ----------

from pyspark.sql.window import * 
from pyspark.sql.functions import * 

# COMMAND ----------

mywin = Window.orderBy("Salary")

df2=df.withColumn("Rownum",row_number().over(mywin))
display(df2)

# COMMAND ----------

mywin = Window.orderBy(col("Salary").desc())

df2=df.withColumn("Rownum",row_number().over(mywin))
display(df2)

# COMMAND ----------

mywin = Window.partitionBy("City").orderBy(col("Salary").desc())

df2=df.withColumn("Rownum",row_number().over(mywin))
display(df2)

# COMMAND ----------

mywin = Window.partitionBy("City","department").orderBy(col("Salary").desc())

df2=df.withColumn("Rownum",row_number().over(mywin))
display(df2)

# COMMAND ----------

mywin = Window.orderBy(col("Salary").desc())

df2=df.withColumn("Rank",rank().over(mywin))
display(df2)

# COMMAND ----------

mywin = Window.orderBy(col("Salary").desc())

df2=df.withColumn("Rank",dense_rank().over(mywin))
display(df2)

# COMMAND ----------

mywin = Window.orderBy(col("Salary"))

df2=df.withColumn("RunningTotal",sum("Salary").over(mywin))
display(df2)

# COMMAND ----------

mywin = Window.orderBy(col("Salary")).rowsBetween(Window.unboundedPreceding,Window.currentRow)

df2=df.withColumn("RunningTotal",sum("Salary").over(mywin))
display(df2)

# COMMAND ----------

mywin = Window.orderBy(col("Salary")).rowsBetween(-2,Window.currentRow)

df2=df.withColumn("RunningTotal",sum("Salary").over(mywin))
display(df2)

# COMMAND ----------


