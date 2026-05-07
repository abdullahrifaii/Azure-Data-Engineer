# Databricks notebook source
from pyspark.sql.functions import * 

# COMMAND ----------

help(current_date)

# COMMAND ----------

current_date()

# COMMAND ----------

df=spark.range(1,10)
display(df)

# COMMAND ----------

df2=df.withColumn("Today",current_date())
display(df2)

# COMMAND ----------

df3 = df2.withColumn("CurrentDatetime",current_timestamp())
display(df3)

# COMMAND ----------

df4=df3.withColumn("Year",year("Today"))
display(df4)

# COMMAND ----------

df4=df3.withColumn("Year",year(current_date()))
display(df4)

# COMMAND ----------

df4=df3.withColumn("Year",year("Today")).withColumn("month",month("Today")).withColumn("day",dayofmonth("Today"))
display(df4)

# COMMAND ----------

df5=df4.withColumn("Hour",hour("CurrentDatetime")).withColumn("min",minute("CurrentDatetime")).withColumn("sec",second("CurrentDatetime"))
display(df5)

# COMMAND ----------

df6=df5.withColumn("Add10",date_add("Today",10))
display(df6)

# COMMAND ----------

df6=df5.withColumn("sub10",date_sub("Today",10))
display(df6)

# COMMAND ----------

df7=df6.withColumn("addmon",add_months("Today",4))
display(df7)

# COMMAND ----------

df8=df7.withColumn("diff",datediff("addmon","sub10"))
display(df8)

# COMMAND ----------

df9=df8.withColumn("today2",date_format("Today","dd-MMM-yy"))
display(df9)

# COMMAND ----------

df9=df8.withColumn("CurrentDatetime",date_format("Today","dd-MMM-yy HH:mm:ss"))
display(df9)

# COMMAND ----------


