# Databricks notebook source
# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

df = read_csv_df(
    "/Volumes/cdudevcatalog/bronze/datavol/data/Orders_new.csv",
    infer_schema=True
)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col,round
df2 = df.withColumn("total_amount", round(col("quantity") * col("price"),2))
display(df2)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df3= df2.select(sum("total_amount"))
display(df3)

# COMMAND ----------

df3= df2.select(sum("total_amount"))
display(df3)

# COMMAND ----------

df3.explain(True)

# COMMAND ----------

df3= df2.select(avg("total_amount"))
display(df3)

# COMMAND ----------

df3= df2.select(min("total_amount"))
display(df3)

# COMMAND ----------

df3= df2.select(max("total_amount"))
display(df3)

# COMMAND ----------

df3= df2.select(count("total_amount"))
display(df3)

# COMMAND ----------

df3= df2.select(count_distinct("product"))
display(df3)

# COMMAND ----------

df3= df2.select(sum("total_amount"),avg("total_amount"))
display(df3)

# COMMAND ----------

df4=df2.selectExpr("sum(total_amount) as total_revenue")
display(df4)

# COMMAND ----------

help(df2.agg)

# COMMAND ----------

df5=df2.agg(sum("total_amount").alias("total_revenue"),avg("total_amount").alias("avg_revenue"))
display(df5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grouping

# COMMAND ----------

help(df2.groupBy)

# COMMAND ----------


df_prodwise_agg = df2.groupBy("product").sum("total_amount").alias("total_revenue")
display(df_prodwise_agg)

# COMMAND ----------

df_prodwise_agg.explain(True)

# COMMAND ----------

df_prodwise_agg2 = df2.groupBy("product").agg(round(sum("total_amount"),2).alias("total_revenue"))
display(df_prodwise_agg2)

# COMMAND ----------

df_prodwise_agg3 = df2.groupBy("product").agg(round(sum("total_amount"),2).alias("total_revenue"),round(avg("total_amount"),2).alias("avg_revenue"))
display(df_prodwise_agg3)

# COMMAND ----------

df_prodwise_agg4 = df2.groupBy("year","product").agg(round(sum("total_amount"),2).alias("total_revenue"))
display(df_prodwise_agg4)

# COMMAND ----------

df_prodwise_agg4.explain(True)

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",50)

# COMMAND ----------

df_prodwise_agg4 = df2.groupBy("year","product").agg(round(sum("total_amount"),2).alias("total_revenue"))
display(df_prodwise_agg4)

# COMMAND ----------

df_prodwise_agg4.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## # 🔥 Spark Interview Questions

# COMMAND ----------

1️⃣ What is groupBy() in PySpark?

Answer:
Used to group data based on one or more columns and perform aggregations.

df.groupBy("dept").sum("salary")



# COMMAND ----------

2️⃣ Difference between groupBy() and agg()?
groupBy	        agg
Groups data	    Applies aggregation
Used together	More flexible
df.groupBy("dept").agg({"salary":"sum"})

# COMMAND ----------

3️⃣ What are common aggregation functions?
sum()
avg()
count()
min()
max()


# COMMAND ----------

4️⃣ Is aggregation a transformation or action?

Transformation (lazy)

# COMMAND ----------

🔹 Intermediate Questions
5️⃣ What happens internally during aggregation?

Spark uses:

Partial Aggregate → Shuffle → Final Aggregate

# COMMAND ----------

6️⃣ Why does groupBy cause shuffle?

Because data must be redistributed by grouping keys.

# COMMAND ----------

7️⃣ What is partial aggregation?

Aggregation done locally on each partition before shuffle.

# COMMAND ----------

8️⃣ How do you perform multiple aggregations?
df.groupBy("dept").agg(
    sum("salary"),
    avg("salary"),
    count("*")
)

# COMMAND ----------

9️⃣ Difference between count() and countDistinct()?
count	countDistinct
Counts all rows	Counts unique values

# COMMAND ----------

🔟 How to rename aggregated columns?
from pyspark.sql.functions import sum

df.groupBy("dept").agg(
    sum("salary").alias("total_salary")
)


# COMMAND ----------

🔹 Advanced Interview Questions
1️⃣1️⃣ What is HashAggregate vs SortAggregate?
HashAggregate → faster (default)
SortAggregate → used when memory is insufficient

# COMMAND ----------

1️⃣2️⃣ How to avoid shuffle in aggregation?

Not fully avoidable, but can reduce by:

pre-aggregation
filtering early
partitioning data

# COMMAND ----------

1️⃣3️⃣ What is data skew in groupBy? (To be covered later)

When one key has huge data, causing slow tasks.

# COMMAND ----------

1️⃣4️⃣ How to handle skew in aggregation? (To be covered later)
Salting
Repartitioning
Skew hints

# COMMAND ----------

1️⃣5️⃣ What is approx_count_distinct()?

Faster approximate distinct count using HyperLogLog.

# COMMAND ----------

🔹 Scenario-Based Questions (Very Important)
1️⃣6️⃣ Scenario: Calculate total sales per product
df.groupBy("product").sum("sales")

# COMMAND ----------

1️⃣7️⃣ Scenario: Find average salary per department
df.groupBy("dept").avg("salary")

# COMMAND ----------

1️⃣8️⃣ Scenario: Count employees per department
df.groupBy("dept").count()

# COMMAND ----------

1️⃣9️⃣ Scenario: Multiple metrics together
df.groupBy("dept").agg(
    sum("salary"),
    avg("salary"),
    count("*")

# COMMAND ----------

2️⃣0️⃣ Scenario: Filter after aggregation
df.groupBy("dept").sum("salary") \
  .filter("sum(salary) > 10000")


# COMMAND ----------

2️⃣1️⃣ Scenario: Top department by revenue
df.groupBy("dept").sum("sales") \
  .orderBy("sum(sales)", ascending=False)

# COMMAND ----------

2️⃣2️⃣ Scenario: Group by multiple columns
df.groupBy("year","product").sum("sales")

# COMMAND ----------

2️⃣3️⃣ Scenario: Conditional aggregation
from pyspark.sql.functions import when, sum

df.groupBy("dept").agg(
    sum(when(col("salary") > 5000,1).otherwise(0))
)

