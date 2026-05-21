# Databricks notebook source
# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

df=ReadTableFromDatabase("dbo.orders")
display(df)

# COMMAND ----------

df2 = QueryFromDatabase("select orderdate, product, orderamount from orders")
display(df2)

# COMMAND ----------

WriteDataframeToDatabase(df,"dbo.orders3")

# COMMAND ----------

WriteDataframeToDatabaseOverwrite(df,"orders3")

# COMMAND ----------

WriteDataframeToDatabaseMode(df,"dbo.orders3","append")

# COMMAND ----------

WriteDataframeToDatabaseMode(df,"dbo.Orders2","overwrite")

# COMMAND ----------

driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
con = driver_manager.getConnection(url, sqluser, sqlpass)
statement =f"""TRUNCATE TABLE dbo.Orders2"""
exec_statement = con.prepareCall(statement)

exec_statement.execute()

exec_statement.close()
con.close()

