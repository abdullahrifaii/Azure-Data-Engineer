# Databricks notebook source
data = [
    (1, "John", 50000),
    (2, "Aman", 60000),
    (3, "Sam", 70000)
]

rdd = sc.parallelize(data)
rdd.collect()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧩 Method 1: Using Quick Method

# COMMAND ----------

help(rdd.toDF)

# COMMAND ----------

df = rdd.toDF(["id", "name", "salary"])
df.show()


# COMMAND ----------

display(df)

# COMMAND ----------

df2= spark.createDataFrame(data, ["id", "name", "salary"])
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧩 Method 2: Create DataFrame from RDD using StructType (Recommended & Interview-Safe)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True)
])


# COMMAND ----------

df3 = spark.createDataFrame(rdd, schema)
display(df3)

# COMMAND ----------

rdd2= df.rdd
rdd2.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔥 Spark Interview Questions & Answers

# COMMAND ----------

1️⃣ What is a DataFrame in Spark?

Answer:
A DataFrame is a distributed collection of data organized into named columns, similar to a table in a database, optimized for large-scale data processing.

# COMMAND ----------

2️⃣ Is a DataFrame immutable?

Answer:
Yes. DataFrames are immutable. Any transformation creates a new DataFrame, leaving the original unchanged.

# COMMAND ----------

3️⃣ Are DataFrames lazy or eager?

Answer:
DataFrames use lazy evaluation. Transformations are executed only when an action is triggered.

# COMMAND ----------

4️⃣ How are DataFrames different from RDDs?

Answer:
DataFrames are schema-based, optimized, and high-level, while RDDs are low-level and require manual optimization.

# COMMAND ----------

5️⃣ What is schema in a DataFrame?

Answer:
Schema defines column names and data types, enabling Spark to validate data and optimize execution.

# COMMAND ----------

6️⃣ Are DataFrames built on top of RDDs?

Answer:
Yes. DataFrames are executed using RDDs internally, but they add an optimized, higher-level abstraction.

# COMMAND ----------

7️⃣ What is Catalyst Optimizer?

Answer:
Catalyst is Spark’s query optimizer that transforms logical plans into optimized physical execution plans.

# COMMAND ----------


8️⃣ What is Tungsten?

Answer:
Tungsten is Spark’s execution engine that improves memory management and CPU efficiency.

# COMMAND ----------


9️⃣ What are transformations and actions in DataFrames?

Answer:

Transformations: select, filter, groupBy (lazy)

Actions: show, count, collect (trigger execution)

# COMMAND ----------

🔟 What are narrow and wide transformations in DataFrames?

Answer:

Narrow: No shuffle (e.g., select, filter)

Wide: Requires shuffle (e.g., groupBy, join)

# COMMAND ----------


1️⃣1️⃣ Does DataFrame support SQL?

Answer:
Yes. DataFrames support SQL queries using spark.sql().

# COMMAND ----------


1️⃣2️⃣ How is a DataFrame distributed?

Answer:
A DataFrame is split into partitions and processed in parallel across executors.

# COMMAND ----------

1️⃣3️⃣ Can DataFrames handle unstructured data?

Answer:
DataFrames work best with structured or semi-structured data. For fully unstructured data, RDDs are preferred.

# COMMAND ----------

1️⃣4️⃣ What are common ways to create a DataFrame?

Answer:

From files (CSV, JSON, Parquet, Delta)

From tables

From RDDs

From databases using JDBC

# COMMAND ----------

1️⃣5️⃣ Why are DataFrames faster than RDDs?

Answer:
Because DataFrames use schema, Catalyst optimization, and Tungsten execution, reducing memory and CPU overhead.

# COMMAND ----------


1️⃣6️⃣ What happens when collect() is called?

Answer:
All data is brought to the driver, which can cause memory issues for large datasets.

# COMMAND ----------

1️⃣7️⃣ Is Dataset available in PySpark?

Answer:
No. Dataset is available only in Scala and Java, not in Python.

# COMMAND ----------

1️⃣8️⃣ When should you use DataFrames over RDDs?

Answer:
When working with structured data, analytics, joins, aggregations, and when performance matters.

# COMMAND ----------

1️⃣9️⃣ Why should we define schema explicitly?

Answer:
To improve performance, avoid incorrect data types, and enable better optimization.

# COMMAND ----------

2️⃣0️⃣ What is StructType?

Answer:
StructType is a Spark class used to define a DataFrame schema with column names, types, and nullability.

# COMMAND ----------

2️⃣1️⃣ What is the default nullability of StructField?

Answer:
True (nullable by default).

# COMMAND ----------

2️⃣2️⃣ Is schema inference good for production?

Answer:
No. It scans data and may infer incorrect types.

# COMMAND ----------

2️⃣3️⃣ Can schema be changed after DataFrame creation?

Answer:
No. DataFrames are immutable. Schema changes create a new DataFrame.
