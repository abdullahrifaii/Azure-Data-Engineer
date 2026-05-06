# Databricks notebook source
lst =[1,2,3,4,5]
print(lst)

# COMMAND ----------

type(lst)

# COMMAND ----------

sparkcon = spark.sparkContext
type(sparkcon)

# COMMAND ----------

help(sparkcon.parallelize)

# COMMAND ----------

rdd1 = sparkcon.parallelize(lst)


# COMMAND ----------

type(rdd1)

# COMMAND ----------

rdd1.collect()

# COMMAND ----------

# MAGIC %md  
# MAGIC ## 🔥Spark Interview Questions
# MAGIC

# COMMAND ----------

1️⃣ What is SparkContext?

Answer:
SparkContext is the entry point to Spark’s core functionality. It represents a connection to a Spark cluster and is responsible for RDD creation, task scheduling, and resource coordination.

# COMMAND ----------

2️⃣ Why is SparkContext needed?

Answer:
Without SparkContext, Spark cannot:

Connect to the cluster

Create RDDs

Submit jobs

Access Spark configuration

It acts as the driver’s gateway to the cluster.

# COMMAND ----------

3️⃣ Difference between SparkContext and SparkSession?

Answer:

SparkContext → low-level API (RDD)

SparkSession → unified entry point for DataFrame, SQL, Streaming

In Spark 2.x+, SparkSession internally contains SparkContext.

# COMMAND ----------

4️⃣ What is an RDD in Spark?

Answer:
An RDD (Resilient Distributed Dataset) is Spark’s core data structure. It is an immutable, distributed collection of objects that can be processed in parallel and is fault-tolerant through lineage.

# COMMAND ----------

5️⃣ What is lazy evaluation in RDD?

Answer:
RDD transformations are not executed immediately. They are recorded and executed only when an action is called. This allows Spark to optimize execution.

# COMMAND ----------

6️⃣ What is the difference between transformations and actions?

Answer:

Transformations create a new RDD (lazy)

Actions trigger execution and return results to the driver
