# Databricks notebook source
# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

df = read_csv_df(
    "/Volumes/cdudevcatalog/bronze/datavol/Orders/Orders.csv",
    infer_schema=True
)
display(df)

# COMMAND ----------

help(df.withColumn)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Add a new column

# COMMAND ----------

from pyspark.sql.functions import col,round

df2 = df.withColumn("TotalAmount", round(col("quantity") * col("price"), 2))
display(df2)

# COMMAND ----------

df2.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modify an Existing Column

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col, round

df3 = df.withColumn("price", round(col("price") * 1.10, 2))
display(df3)

# COMMAND ----------

df3.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add a literal value in new column
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

help(lit)

# COMMAND ----------



# COMMAND ----------

df4 = df.withColumn("CountryOfOrigin", lit("India"))
display(df4)

# COMMAND ----------

df4.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cast DataType
# MAGIC

# COMMAND ----------



# COMMAND ----------

df5 = df.withColumn("price", col("price").cast("double"))
display(df5)

# COMMAND ----------

df5.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add multiple columns
# MAGIC

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import lit, col, round

df6 = df.withColumn("CountryOfOrigin", lit("India")) \
        .withColumn("price", col("price").cast("double")) \
        .withColumn("TotalAmount", round(col("price") * col("quantity"), 2))
display(df6)

# COMMAND ----------

df6.explain(True)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import lit, col, round

df6 = df.select(
    "*",
    lit("India").alias("CountryOfOrigin"),
    col("price").cast("double").alias("price"),
    round(col("price").cast("double") * col("quantity"), 2).alias("TotalAmount")
)
display(df6)

# COMMAND ----------

df6.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔥 Spark Interview Questions

# COMMAND ----------

1️⃣ What is withColumn() in PySpark?

Answer:
It is a transformation used to add a new column or modify an existing column in a DataFrame. It returns a new DataFrame (immutability).

# COMMAND ----------

2️⃣ What happens if column name already exists?

It overwrites the column.

# COMMAND ----------

3️⃣ Difference between withColumn() and select()?
| withColumn                            | select                                      |
| ------------------------------------- | ------------------------------------------- |
| Adds or modifies one column at a time | Can select & create multiple columns        |
| Often chained                         | More optimized for multiple transformations |
| Keeps all columns by default          | Must specify columns explicitly             |


# COMMAND ----------

4️⃣ Why is chaining multiple withColumn() bad for performance?

Each withColumn() adds a new logical projection step.
When chained many times:

Execution plan becomes large

Optimizer overhead increases

Can slow down large pipelines

# COMMAND ----------

5️⃣ How does withColumn() work internally?

It creates a new logical plan node (Project).
Spark does not execute immediately (lazy evaluation).

Execution happens only when:

show()

write()

collect()

# COMMAND ----------

6️⃣ Is withColumn() transformation narrow or wide?
It is a narrow transformation unless used with:

Window functions

Aggregations

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔥 Very Important One-Liner Answer
# MAGIC
# MAGIC _If interviewer asks_:
# MAGIC
# MAGIC 👉 “When should you avoid withColumn?”
# MAGIC
# MAGIC Answer:
# MAGIC
# MAGIC When creating many derived columns at once — prefer select() for better optimization and cleaner execution plan.
