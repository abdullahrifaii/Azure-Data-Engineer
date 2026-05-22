# Databricks notebook source
# MAGIC %run /Workspace/Users/aboudeh.rifaii@gmail.com/Azure-Data-Engineer/Pyspark/Utilities1

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS mycatalog.bronze.Incremental_Load_Mappings

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE mycatalog.bronze.Incremental_Load_Mappings(
# MAGIC   TableName string,
# MAGIC   WaterMarkColumn string,
# MAGIC   WaterMarkValue DATE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE mycatalog.bronze.Incremental_Load_Mappings

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED mycatalog.bronze.Incremental_Load_Mappings

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO mycatalog.bronze.Incremental_Load_Mappings(TableName, WaterMarkColumn, WaterMarkValue)
# MAGIC VALUES
# MAGIC ('dbo.tbl_orders', 'order_date', '2015-01-01');
# MAGIC
# MAGIC SELECT * FROM mycatalog.bronze.Incremental_Load_Mappings;

# COMMAND ----------

maxDate = spark.table("mycatalog.bronze.Incremental_Load_Mappings").select("WaterMarkValue").where("TableName = 'dbo.tbl_orders'").collect()
maxDate[0]['WaterMarkValue']

# COMMAND ----------

df  = read_csv_df("/Volumes/cdudevcatalog/bronze/datavol/Orders/",infer_schema=True)
display(df)

# COMMAND ----------

delta_load = df.filter(df.order_date > maxDate[0]['WaterMarkValue'])
display(delta_load)

# COMMAND ----------

WriteDataframeToDatabaseMode(delta_load, "dbo.tbl_orders", "append")


# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

New_WaterMark_Value = delta_load.withColumn(
    "order_date", F.col("order_date").cast("date")
).agg(F.max("order_date")).collect()[0][0]
print(New_WaterMark_Value)

# COMMAND ----------

query = f"""
UPDATE cdudevcatalog.bronze.incremental_load_mappings 
SET WaterMarkValue = CAST('{New_WaterMark_Value}' AS DATE)
WHERE TableName = 'dbo.tbl_orders'
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cdudevcatalog.bronze.incremental_load_mappings

# COMMAND ----------


