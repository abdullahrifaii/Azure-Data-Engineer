# Databricks notebook source
# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

df = read_csv_df(
    "/Volumes/cdudevcatalog/bronze/datavol/data/CustTransactions.csv",
    infer_schema=True
)
display(df)


# COMMAND ----------

help(df.distinct)

# COMMAND ----------

help(df.dropDuplicates)

# COMMAND ----------

df2 = df.distinct()
display(df2)

# COMMAND ----------

df2.explain(True)

# COMMAND ----------

df3 = df.dropDuplicates()
display(df3)

# COMMAND ----------

df3.explain(True)

# COMMAND ----------

df4 = df.dropDuplicates(["Product"])
display(df4)

# COMMAND ----------

df4.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔥 Spark Interview Questions

# COMMAND ----------

1️⃣ What is the difference between distinct() and dropDuplicates()?
Method	            Behavior
---------------------------------
distinct()	        Removes duplicate rows considering all columns
dropDuplicates()	Removes duplicates based on selected columns

Example:

df.distinct()
df.dropDuplicates(["customer_id"])



# COMMAND ----------

2️⃣ Is distinct() a transformation or action?

Answer:
It is a transformation (lazy execution).

# COMMAND ----------

3️⃣ Does removing duplicates cause shuffle in Spark?

Yes.

Because Spark must compare records across different partitions

# COMMAND ----------

4️⃣ What is the syntax for removing duplicates based on specific columns?
df.dropDuplicates(["customer_id"])


# COMMAND ----------

5️⃣ What happens if you run dropDuplicates() without specifying columns?

Spark removes exact duplicate rows across all columns.


# COMMAND ----------

6️⃣ When should you use distinct() instead of dropDuplicates()?

Use distinct() when:

You want unique rows across all columns

No specific key column exists.


# COMMAND ----------

7️⃣ When should you use dropDuplicates()?

Use when duplicates should be removed based on business key columns.

Example:

df.dropDuplicates(["order_id"])

# COMMAND ----------

8️⃣ Is dropDuplicates() deterministic?

No.

If multiple duplicates exist, Spark may return any one record.

# COMMAND ----------

9️⃣ How do you keep the latest record instead of random duplicate? (skip for now)

Use window function + filter.

Example:

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window = Window.partitionBy("customer_id").orderBy(col("update_time").desc())

df.withColumn("rn", row_number().over(window)).filter("rn = 1")

# COMMAND ----------

🔟 Can duplicates be removed in streaming pipelines?

Yes.

Using:

df.dropDuplicates(["event_id"])

Often combined with watermark.

🔹 Advanced Interview Questions

# COMMAND ----------

1️⃣1️⃣ Why is removing duplicates expensive?

Because it requires:

shuffle

data comparison across partitions

# COMMAND ----------

1️⃣2️⃣ How does Spark internally remove duplicates?

Spark uses:

Hash Aggregate

Execution plan may show:

HashAggregate
Exchange


# COMMAND ----------

1️⃣3️⃣ What optimization can reduce duplicate removal cost?

Options:

Partitioning data

Filtering earlier

Removing duplicates at source


# COMMAND ----------

1️⃣4️⃣ How can you check duplicate removal in execution plan?
df.explain(True)

Look for:

HashAggregate



# COMMAND ----------

🔹 Tricky Interview Questions
❓ Does distinct() always guarantee deterministic results?

Yes for row uniqueness, but order is not guaranteed.

❓ Can distinct() be used after select()?

Yes.

Example:

df.select("customer_id").distinct()
❓ Which is faster: distinct() or dropDuplicates()?

Usually similar, but:

dropDuplicates() is better when specific columns are used.
