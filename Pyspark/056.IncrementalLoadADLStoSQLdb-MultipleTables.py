# Databricks notebook source
# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cdudevcatalog.bronze.Incremental_Load_Mappings_Multi

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE cdudevcatalog.bronze.Incremental_Load_Mappings_Multi(
# MAGIC   TableName STRING,
# MAGIC   VolumePath STRING,
# MAGIC   WaterMarkColumn STRING,
# MAGIC   WateMarkValue TIMESTAMP,
# MAGIC   IsActive INT
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO cdudevcatalog.bronze.Incremental_Load_Mappings_Multi(TableName,VolumePath, WaterMarkColumn, WateMarkValue,IsActive)
# MAGIC VALUES
# MAGIC ('dbo.tbl_sales', '/Volumes/cdudevcatalog/bronze/datavol/Sales/','SalesDate', '2015-01-01',1),
# MAGIC ('dbo.tbl_orders', '/Volumes/cdudevcatalog/bronze/datavol/Orders/','order_date', '2015-01-01',1),
# MAGIC ('dbo.tbl_purchase', '/Volumes/cdudevcatalog/bronze/datavol/Purchase/','PurchaseDate' ,'2015-01-01',1);
# MAGIC
# MAGIC SELECT * FROM cdudevcatalog.bronze.Incremental_Load_Mappings_Multi;

# COMMAND ----------

table_metadf = spark.sql("select * from cdudevcatalog.bronze.incremental_load_mappings_multi where IsActive=1")
display(table_metadf)

# COMMAND ----------

from pyspark.sql.functions import col,lit
from datetime import datetime


rows = table_metadf.collect()

for tablerow in rows:
    TableName = tablerow["TableName"]
    VolumePath = tablerow["VolumePath"]
    WaterMarkColumn = tablerow["WaterMarkColumn"]
    WaterMarkValue = tablerow["WateMarkValue"]
    print( WaterMarkValue)
    print(f"Loading incremental data for table: {TableName}")

    # Read new data from ADLS
    df =  read_csv_df(VolumePath,infer_schema=True).withColumn(WaterMarkColumn, col(WaterMarkColumn).cast("timestamp")).filter(col(WaterMarkColumn) > lit(WaterMarkValue))
          
    if not df.isEmpty():
        # Write to SQL
        WriteDataframeToDatabaseMode(df,TableName,"append")

        # Update watermark (get max value from current data)
        new_watermark = df.agg({WaterMarkColumn: "max"}).collect()[0][0]
        

        # Here you would persist the new watermark to a config file or table
        query = f"""
                UPDATE cdudevcatalog.bronze.incremental_load_mappings_multi 
                SET WateMarkValue = CAST('{new_watermark}' AS timestamp)
                WHERE TableName = '{TableName}'
                """
        spark.sql(query)

        print(f"Updated watermark for {TableName}: {new_watermark}")
    else:
        print(f"No new data found for table: {TableName}")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cdudevcatalog.bronze.incremental_load_mappings_multi

# COMMAND ----------


