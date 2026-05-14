# Databricks notebook source
lst = list(range(1,11))
print(lst)

# COMMAND ----------

rdd1= sc.parallelize(lst)

# COMMAND ----------

rdd1.getNumPartitions()

# COMMAND ----------

rdd1.glom().collect()

# COMMAND ----------

help(rdd1.coalesce)

# COMMAND ----------

rdd2 = rdd1.coalesce(2)

# COMMAND ----------

rdd2.getNumPartitions()

# COMMAND ----------

rdd2.glom().collect()

# COMMAND ----------

help(rdd1.repartition)

# COMMAND ----------

rdd3=rdd1.repartition(6)
rdd3.glom().collect()

# COMMAND ----------

rdd4=rdd1.repartition(3)

# COMMAND ----------

rdd5=rdd1.coalesce(10)
rdd5.glom().collect()

# COMMAND ----------

# MAGIC %md  
# MAGIC ## 🔥Spark Interview Questions
# MAGIC

# COMMAND ----------

1️⃣ What are the types of transformations in Spark?

Answer:
There are two types of transformations in Spark:

Narrow transformations

Wide transformations

# COMMAND ----------

2️⃣ What is a narrow transformation?

Answer:
A narrow transformation is one where each output partition depends on only one input partition, so no shuffle is required.

# COMMAND ----------

3️⃣ What is a wide transformation?

Answer:
A wide transformation is one where output partitions depend on multiple input partitions, which causes a shuffle operation.

# COMMAND ----------

4️⃣ Which type of transformation creates a stage boundary?

Answer:
Wide transformations create stage boundaries because they require shuffle.

# COMMAND ----------

5️⃣ What is shuffle in Spark?

Answer:
Shuffle is the process where Spark redistributes data across partitions so that records with the same key are grouped together. It involves network and disk I/O.

# COMMAND ----------

6️⃣ Why is shuffle considered an expensive operation?

Answer:
Because it involves:

Network data transfer

Disk read/write

Serialization and deserialization

# COMMAND ----------

7️⃣ Does shuffle create a stage boundary?

Answer:
Yes. Every shuffle creates a new stage in Spark.



# COMMAND ----------

8️⃣ Is shuffle required for all transformations?

Answer:
No. Only wide transformations require shuffle. Narrow transformations do not.

# COMMAND ----------

9️⃣ What is the difference between coalesce() and repartition()?

Answer:
coalesce() reduces the number of partitions without shuffle (by default), while repartition() changes partitions by always performing a shuffle.

# COMMAND ----------

🔟 1️⃣1️⃣Which one is faster: coalesce() or repartition()?

Answer:
coalesce() is faster because it avoids shuffle. repartition() is slower due to network and disk I/O.

# COMMAND ----------

1️⃣1️⃣ can coalesce() increase the number of partitions?

Answer:
No. 

# COMMAND ----------

1️⃣2️⃣ Can repartition() reduce partitions?

Answer:
Yes. repartition() can both increase and decrease partitions.

# COMMAND ----------

1️⃣3️⃣ Does repartition() always cause shuffle?

Answer:
Yes. repartition() always triggers a shuffle.

# COMMAND ----------

1️⃣4️⃣ Which one provides better load balancing?

Answer:
repartition() provides better load balancing because data is evenly redistributed.

# COMMAND ----------

1️⃣5️⃣ How is repartition() implemented internally?

Answer:
repartition() is internally implemented as:

coalesce(numPartitions, shuffle=True)

# COMMAND ----------

1️⃣6️⃣ Which one is more expensive and why?

Answer:
repartition() is more expensive because it involves network and disk I/O due to shuffle.
