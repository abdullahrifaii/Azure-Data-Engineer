# Databricks notebook source
# MAGIC %run /Workspace/Users/aboudeh.rifaii@gmail.com/Azure-Data-Engineer/Pyspark/Utilities

# COMMAND ----------

df = read_csv_df(
    "/Volumes/mycatalog/default/myvolume/Databricks_Practice/Orders_new.csv",
    infer_schema=True
)
display(df)

# COMMAND ----------

help(df.write)

# COMMAND ----------

df.write.format("csv").option("path","/Workspace/Users/aboudeh.rifaii@gmail.com/Azure-Data-Engineer/Pyspark").mode("overwrite").save()

# COMMAND ----------

df2 = read_csv_df(
    "/Volumes/mycatalog/default/myvolume/Output",
    infer_schema=True
)
display(df2)

# COMMAND ----------

df.write.format("csv").option("path","/Volumes/mycatalog/default/myvolume/Output2").option("header",True).save()

# COMMAND ----------

(df.write.format("csv")
.option("path","/Volumes/ADE202601/bronze/voldata/Output/")
.option("header",True)
.mode("append")
.save())

# COMMAND ----------

(df.write.format("csv")
.option("path","/Volumes/ADE202601/bronze/voldata/Output/")
.option("header",True)
.mode("overwrite")
.save())

# COMMAND ----------

(df.write.format("csv")
.option("path","/Volumes/ADE202601/bronze/voldata/Output/")
.option("header",True)
.mode("ignore")
.save())

# COMMAND ----------

(df.write.format("csv")
.option("path","/Volumes/ADE202601/bronze/voldata/Output/")
.option("header",True)
.mode("error")
.save())

# COMMAND ----------


