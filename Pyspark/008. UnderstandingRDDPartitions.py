# Databricks notebook source
lst = [1,2,3,4,5]
rdd1 = sc.parallelize(lst)
rdd1.collect()

# COMMAND ----------

rdd1.getNumPartitions()

# COMMAND ----------

rdd1.glom().collect()

# COMMAND ----------

sc.defaultParallelism

# COMMAND ----------

sc.defaultMinPartitions

# COMMAND ----------

rdd1.collect()

# COMMAND ----------

# MAGIC %md  
# MAGIC ## 🔥Spark Interview Questions
# MAGIC

# COMMAND ----------

1️⃣ Why is RDD called resilient?

Answer:
RDD is resilient because Spark can recompute lost partitions using lineage, instead of replicating data. This allows fault recovery without expensive storage overhead.

# COMMAND ----------

2️⃣ What do you mean by RDD immutability?

Answer:
Once an RDD is created, it cannot be modified. Any transformation applied to an RDD returns a new RDD, which helps Spark maintain consistency and fault tolerance.

# COMMAND ----------

3️⃣ How does Spark achieve fault tolerance in RDD?

Answer:
Through lineage. Spark keeps track of the sequence of transformations used to create an RDD, and if a partition is lost, it recomputes only that partition.

# COMMAND ----------

4️⃣ What is lazy evaluation in RDD?

Answer:
RDD transformations are not executed immediately. They are recorded and executed only when an action is called. This allows Spark to optimize execution.

# COMMAND ----------

5️⃣ What is a partition in RDD?

Answer:
A partition is a logical chunk of data in an RDD. Each partition is processed by one task, and multiple partitions allow parallel execution.

# COMMAND ----------

6️⃣ How many tasks are created for an RDD?

Answer:
The number of tasks equals the number of partitions in the RDD.

# COMMAND ----------

7️⃣ What is the difference between transformations and actions?

Answer:

Transformations create a new RDD (lazy)

Actions trigger execution and return results to the driver
