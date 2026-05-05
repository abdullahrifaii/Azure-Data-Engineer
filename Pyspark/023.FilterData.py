# Databricks notebook source
# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

df = read_csv_df(
    "/Volumes/cdudevcatalog/bronze/datavol/Orders/Orders.csv",
    infer_schema=True
)
display(df)

# COMMAND ----------

help(df.filter)

# COMMAND ----------

help(df.where)

# COMMAND ----------

df2=df.filter("Product=='Camera'")
display(df2)

# COMMAND ----------

df2.explain(True)

# COMMAND ----------

df3=df.filter("price>1000")
display(df3)

# COMMAND ----------

df4=df.filter("price >=1000 and price <=2000")
display(df4)

# COMMAND ----------

df5=df.filter("Product !='Camera'")
display(df5)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df6=df.where(col("product").isin("Camera","Monitor"))
display(df6)

# COMMAND ----------

df6.explain(True)

# COMMAND ----------

df7=df.filter(col("price").isNotNull())
display(df7)

# COMMAND ----------

df7=df.filter(col("price").isNull())
display(df7)

# COMMAND ----------

df8=df.filter(col("customer").startswith("A"))
display(df8)

# COMMAND ----------

df8=df.filter(col("customer").like("A%"))
display(df8)

# COMMAND ----------

df8=df.filter(col("customer").endswith("e"))
display(df8)

# COMMAND ----------

df8=df.filter(col("customer").like("%e"))
display(df8)

# COMMAND ----------

df9=df.filter(col("price").between(1000,2000))
display(df9)

# COMMAND ----------

df10 = df.filter("product=='Camera' or product=='Monitor'" )
display(df10)

# COMMAND ----------

df10 = df.filter((col("product") == "Camera") | (col("product") == "Monitor"))
display(df10)

# COMMAND ----------

df11 = df.filter((col("price") >= 1000) & (col("price") <= 2000))
display(df11)

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔥 Spark Interview Questions

# COMMAND ----------


1️⃣ Difference between filter() and where()

Answer: None. Both are same.



# COMMAND ----------

2️⃣ Is filter a narrow or wide transformation?

Answer:

Narrow transformation

(no shuffle).

# COMMAND ----------

3️⃣ Can filter improve performance?

Yes.

Because of predicate pushdown.


# COMMAND ----------

4️⃣ What is predicate pushdown?

Filter condition applied at data source level.

Example:

Parquet → Spark reads only required rows.
