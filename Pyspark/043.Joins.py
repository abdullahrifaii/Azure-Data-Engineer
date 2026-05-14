# Databricks notebook source
# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

df_trip=read_csv_df("/Volumes/cdudevcatalog/bronze/datavol/nyctaxi/")
display(df_trip)

# COMMAND ----------

df_trip.count()

# COMMAND ----------

df_location=read_csv_df("/Volumes/cdudevcatalog/bronze/datavol/nyctaxi/nyclocations.csv")
display(df_location)

# COMMAND ----------

help(df_trip.join)

# COMMAND ----------

df_joined = df_trip.join(df_location)
display(df_joined)

# COMMAND ----------

df_joined.count()

# COMMAND ----------

100300*300

# COMMAND ----------

df_joined.explain(True)

# COMMAND ----------

df_joined2 = df_trip.join(df_location,df_trip.PUlocationID==df_location.PUlocationID)
display(df_joined2)

# COMMAND ----------

df_joined2.count()

# COMMAND ----------

df_joined2.explain(True)

# COMMAND ----------

df_joined3 = df_location.join(df_trip,df_trip.PUlocationID==df_location.PUlocationID)
display(df_joined3)

# COMMAND ----------

df_joined3.explain(True)

# COMMAND ----------

df_innerjoin = df_location.join(df_trip,df_trip.PUlocationID==df_location.PUlocationID,"inner")
display(df_innerjoin)

# COMMAND ----------

df_innerjoin.count()

# COMMAND ----------

df_innerjoin.explain(True)

# COMMAND ----------

# translate this in spark sql without using temp views
# df_location.join(df_trip,df_trip.PUlocationID==df_location.PUlocationID,"inner")

# COMMAND ----------

df_sql = spark.sql("""SELECT * FROM {df_loc} l INNER JOIN {df_trip} t
ON l.PUlocationID = t.PUlocationID
""",df_loc=df_location,df_trip=df_trip)
display(df_sql)

# COMMAND ----------

df_leftjoin = df_location.join(df_trip,df_trip.PUlocationID==df_location.PUlocationID,"left")
display(df_leftjoin)

# COMMAND ----------

df_leftjoin.count()

# COMMAND ----------

df_leftjoin.explain(True)

# COMMAND ----------

df_leftanti = df_location.join(df_trip,df_trip.PUlocationID==df_location.PUlocationID,"leftanti")
display(df_leftanti)

# COMMAND ----------

df_leftanti.explain(True)

# COMMAND ----------

leftsemi = df_location.join(df_trip,df_trip.PUlocationID==df_location.PUlocationID,"leftsemi")
display(leftsemi)
