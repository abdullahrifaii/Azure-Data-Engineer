# Databricks notebook source
# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

df = read_csv_df(
    "/Volumes/cdudevcatalog/bronze/datavol/Orders/Orders.csv",
    infer_schema=True
)
display(df)

# COMMAND ----------

help(df.orderBy)

# COMMAND ----------

help(df.sort)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sort in Ascending(Default)

# COMMAND ----------

df2 = df.orderBy("customer") #Default is ascending
display(df2)

# COMMAND ----------

df2.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sort In Descending

# COMMAND ----------

df3=df.orderBy("price",ascending=False)
display(df3)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df4=df.orderBy(col("price").desc())
display(df4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sort by multiple columns

# COMMAND ----------

df5=df.sort("Product","Price")
display(df5)

# COMMAND ----------

df5.explain(True)

# COMMAND ----------

help(df.sortWithinPartitions)

# COMMAND ----------

df6=df.sortWithinPartitions("price")
display(df6)

# COMMAND ----------

df6.explain(True)

# COMMAND ----------

df7=df.orderBy(2)
display(df7)

# COMMAND ----------

df7.explain(True)

# COMMAND ----------

df8=df.orderBy(3,5)
display(df8)

# COMMAND ----------

df8.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔥 Spark Interview Questions

# COMMAND ----------

1️⃣ Difference between orderBy() and sort()?

Answer:

They are exactly the same.

df.orderBy("salary")
df.sort("salary")

Both perform global sorting.

# COMMAND ----------

2️⃣ Does sorting trigger shuffle?

✅ Yes.

Global sorting requires shuffle across partitions.

# COMMAND ----------

3️⃣ When should you avoid sorting?

Sorting should be avoided when:

Dataset is huge

Sorting is unnecessary

Causes expensive shuffle

Instead:

use partitioning

use window functions carefully

# COMMAND ----------

4️⃣ How do you sort by multiple columns with different order?
df.orderBy(col("dept").asc(), col("salary").desc())

# COMMAND ----------

5️⃣ What is sortWithinPartitions()?

It sorts only within each partition, avoiding shuffle.

# COMMAND ----------

6️⃣ When should you use sortWithinPartitions()?

Use when:

Data already partitioned

No need for global ordering

Performance optimization

# COMMAND ----------

7️⃣ What is the difference between sortWithinPartitions() and orderBy()?
Method	                Behavior
------------------------------------
orderBy()	            Global sort
sortWithinPartitions()	Local partition sort

# COMMAND ----------

8️⃣ Can sorting be avoided in Spark?

Yes if:

order does not matter

using partition-based operations

using aggregations instead

Sorting is expensive.

# COMMAND ----------

9️⃣ Why is sorting expensive in Spark?

Because it causes:

Shuffle across nodes

Disk spill

Network IO

# COMMAND ----------

🔟 What happens internally when Spark sorts data?

Steps:

1️⃣ Shuffle data
2️⃣ Partition data
3️⃣ Local sort in each partition

# COMMAND ----------

1️⃣1️⃣ How does Spark sort large datasets?

Spark uses External Sort Algorithm.

Meaning:

Uses memory first

Spills to disk when memory exceeds

# COMMAND ----------

1️⃣2️⃣ How can you check if sorting caused shuffle?

Check execution plan:

df.explain(True)

Look for:

Exchange
Sort
