# Databricks notebook source
# MAGIC %md
# MAGIC ## 📌 When to use select()?
# MAGIC
# MAGIC
# MAGIC - Column selection 
# MAGIC - Simple expressions 
# MAGIC - Renaming columns 
# MAGIC - Type-safe operations

# COMMAND ----------

# MAGIC %run /Workspace/Users/aboudeh.rifaii@gmail.com/Azure-Data-Engineer/Pyspark/Utilities

# COMMAND ----------

df = read_csv_df(
    "/Volumes/mycatalog/default/myvolume2/Orders.csv",
    infer_schema=True
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Columns from DataFrame

# COMMAND ----------

df2 = df.select("order_id","customer","order_date")
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Columns from DataFrame using SelectExpr
# MAGIC

# COMMAND ----------

df2 = df.selectExpr("order_id","customer","order_date")
display(df2)

# COMMAND ----------

df_metrics = df.selectExpr(
    "order_id",
    "customer as customer_name",
    "product",
    "quantity",
    "price",
    "round(quantity * price, 2) as total_amount",
    "order_date"
)
display(df_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Why this is powerful?
# MAGIC
# MAGIC - Shorter code 
# MAGIC - SQL developers feel at home 
# MAGIC - Very expressive

# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql.functions import expr

df_metrics = df.select(
    "order_id",
    expr("customer as customer_name"),
    "product",
    "quantity",
    "price",
    expr("round(quantity * price, 2) as total_amount"),
    "order_date"
)
display(df_metrics)

# COMMAND ----------

# DBTITLE 1,Select with col() - Example
from pyspark.sql.functions import col, round

df_with_col = df.select(
    col("order_id"),
    col("customer").alias("customer_name"),
    col("product"),
    col("quantity"),
    col("price"),
    round(col("quantity") * col("price"), 2).alias("total_amount"),
    col("order_date")
)
display(df_with_col)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📌 When to use expr()?
# MAGIC
# MAGIC - Complex expressions 
# MAGIC - Conditional logic 
# MAGIC - Window functions 
# MAGIC - Reusable expressions 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

help(col)

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

df_metrics = df.select(
    "order_id",
    expr("customer as customer_name"),
    "product",
    "quantity",
    "price",
    expr("round(quantity * price, 2) as total_amount"),
    "order_date"
)
display(df_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Scenario
# MAGIC
# MAGIC **Classify orders:**
# MAGIC
# MAGIC _HIGH_VALUE if final_amount > 5000
# MAGIC
# MAGIC else NORMAL_

# COMMAND ----------

# DBTITLE 1,Cell 16
final_df = df.selectExpr(
        "order_id",
        "customer as customer_name",
        "product",
        "quantity",
        "price",
        "order_date",
        "round(quantity * price ,2) as total_amount",
        "CASE WHEN total_amount > 5000 THEN 'HIGH_VALUE' ELSE 'NORMAL' END as customer_segment"
)
display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔥 Spark Interview Questions

# COMMAND ----------

1️⃣ Difference between select() and selectExpr()?

Answer:
select() uses Column API (type-safe), selectExpr() accepts SQL expressions as strings.

# COMMAND ----------

2️⃣ Why use expr() instead of selectExpr()?

Answer:
expr() is ideal when adding a single complex derived column using SQL syntax.

# COMMAND ----------

3️⃣ Which is fastest?

Answer:
All compile to the same logical plan; performance is the same. Choice is about readability and safety.

# COMMAND ----------

🧠 One-Line Interview Summary

“I use select() for schema-safe transformations, selectExpr() for SQL-style derivations, and expr() for complex conditional logic.”
