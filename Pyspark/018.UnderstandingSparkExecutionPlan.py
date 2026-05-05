# Databricks notebook source
# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

df = read_csv_df(
    "/Volumes/cdudevcatalog/bronze/datavol/Orders/Orders.csv",
    infer_schema=True
)
display(df)

# COMMAND ----------

df.explain()

# COMMAND ----------

df.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Columns from DataFrame

# COMMAND ----------

df2 = df.select("order_id","customer","order_date")
display(df2)

# COMMAND ----------

df2.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Columns from DataFrame using SelectExpr
# MAGIC

# COMMAND ----------

df2 = df.selectExpr("order_id","customer","order_date")
display(df2)

# COMMAND ----------

df2.explain(True)

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

df_metrics.explain(True)

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

df_metrics.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔥 Spark Interview Questions

# COMMAND ----------

1️⃣ What is a Spark Execution Plan?

Answer:
A Spark Execution Plan is Spark’s internal blueprint that shows how a query will be executed. It includes the logical plan, optimized logical plan, and physical plan with actual execution strategies.

# COMMAND ----------

2️⃣ How do you view the execution plan?

Answer:

df.explain()
df.explain(True)
df.explain("formatted")


In Databricks, you can also check the Spark UI → SQL tab.

# COMMAND ----------

3️⃣ What is the difference between Logical and Physical Plan?

Answer:
Logical Plan describes what needs to be done.
Physical Plan describes how Spark will execute it (join strategy, shuffle, aggregation method).

# COMMAND ----------

4️⃣ What does Exchange mean in a plan?

Answer:
Exchange indicates a shuffle operation, where data is redistributed across partitions. It usually creates a new stage.

# COMMAND ----------

5️⃣ What are Narrow vs Wide transformations?

Answer:

Narrow: No shuffle (map, filter, select)

Wide: Causes shuffle (join, groupBy, distinct)

# COMMAND ----------

6️⃣ What are the phases of Spark query execution?

Answer:

Unresolved Logical Plan

Analyzed Logical Plan

Optimized Logical Plan

Physical Plan

DAG → Stages → Tasks → Executors

# COMMAND ----------

7️⃣ What is Catalyst Optimizer?

Answer:
Catalyst is Spark SQL’s rule-based optimizer that transforms logical plans into optimized and physical plans using tree transformations.

# COMMAND ----------

8️⃣ What is Predicate Pushdown?

Answer:
It pushes filter conditions down to the data source level to reduce data scanned.

# COMMAND ----------

9️⃣ What is Column Pruning?

Answer:
Spark reads only required columns instead of the full dataset.

# COMMAND ----------

🔟Explain end-to-end Spark query lifecycle.

Answer:

SQL → Parsed Logical Plan

Catalyst Optimization

Physical Plan selection

DAG creation

Stage creation

Tasks distributed to Executors

# COMMAND ----------

1️⃣1️⃣ What is Relation in Spark plan?

Relation is the logical representation of the underlying data source such as CSV, Parquet, table, or JDBC.

# COMMAND ----------

1️⃣2️⃣ What is Project in Spark plan?

Project represents column selection or transformation operations like select(), drop(), and selectExpr().

🧠 One-Line Memory Trick

Relation = Data Source
Project = Column Selection
