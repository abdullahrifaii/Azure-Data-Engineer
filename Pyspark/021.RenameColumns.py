# Databricks notebook source
# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

df = read_csv_df(
    "/Volumes/cdudevcatalog/bronze/datavol/Orders/Orders.csv",
    infer_schema=True
)
display(df)

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename single column

# COMMAND ----------

df2 = df.withColumnRenamed("order_id", "OrderId")
display(df2)

# COMMAND ----------

df2.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename multiple columns

# COMMAND ----------



# COMMAND ----------

help(df.withColumnsRenamed)

# COMMAND ----------

df3 = df.withColumnsRenamed({"order_id": "OrderId", "Order_Date": "OrderDate"})
display(df3)

# COMMAND ----------

df3.explain(True)

# COMMAND ----------



# COMMAND ----------

df4 = df.withColumnRenamed("order_id", "OrderId").withColumnRenamed("Order_Date", "OrderDate")
display(df4)

# COMMAND ----------

df4.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename columns using dictionary

# COMMAND ----------

mapping = {
 "order_id":"OrderId",
 "order_date":"OrderDate"
}

for old,new in mapping.items():
    df = df.withColumnRenamed(old,new)

# COMMAND ----------

dfnew = df.withColumnRenamed("ordernum","OrderNum")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Real Interview Question (Very Common)

# COMMAND ----------

# MAGIC %md
# MAGIC ### # Question:
# MAGIC
# MAGIC You have 100 columns in a dataframe and want to rename all to lowercase. How will you do it efficiently?

# COMMAND ----------

df = df.toDF(*[c.lower() for c in df.columns])

# COMMAND ----------

df = df.toDF(*[c.upper() for c in df.columns])

# COMMAND ----------



# COMMAND ----------

df_lower = rename_columns_case(df, "lower")

# COMMAND ----------

df_upper = rename_columns_case(df, "upper")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔥 Spark Interview Questions

# COMMAND ----------

1️⃣ How do you rename a column in PySpark?
df = df.withColumnRenamed("old_name", "new_name")


# COMMAND ----------

2️⃣ Does withColumnRenamed() modify the original DataFrame?

❌ No.
Spark DataFrames are immutable, so it returns a new DataFrame.



# COMMAND ----------


3️⃣ How do you rename multiple columns in PySpark?
df = df.withColumnRenamed("col1","new_col1") \
       .withColumnRenamed("col2","new_col2")


# COMMAND ----------

4️⃣ What happens if the column does not exist?

Spark does nothing — no error is thrown.

Example:

df.withColumnRenamed("not_existing", "new_name")

DataFrame remains unchanged.

🔹 Intermediate Interview Questions


# COMMAND ----------

5️⃣ What is the difference between alias() and withColumnRenamed()?
alias()	withColumnRenamed()
Temporary rename	Permanent rename
Used inside select()	Used on DataFrame
Does not change schema	Changes schema

Example:

df.select(col("salary").alias("emp_salary"))

# COMMAND ----------

6️⃣ How do you rename all columns to lowercase?
df = df.toDF(*[c.lower() for c in df.columns])

Very common in ETL pipelines.


# COMMAND ----------

7️⃣ How do you rename columns dynamically using a dictionary?
mapping = {
 "CustID":"customer_id",
 "CustName":"customer_name"
}

for old,new in mapping.items():
    df = df.withColumnRenamed(old,new)


# COMMAND ----------

8️⃣ How do you remove spaces from column names?

Example source file:

Customer Name
Order Amount

Solution:

df = df.toDF(*[c.replace(" ","_") for c in df.columns])
🔹 Advanced Interview Questions


# COMMAND ----------

9️⃣ Why should we avoid chaining too many withColumnRenamed()?

Reasons:

Creates long logical plan

Hard to maintain

Slower optimizer time

Better solution:

df = df.toDF(*new_column_list)


# COMMAND ----------

🔟 How do you rename nested columns in Spark? (skip for now, to be covered later)

Example schema:

customer.address.city

Use select:

df.select(col("customer.address.city").alias("city"))


# COMMAND ----------

1️⃣1️⃣ Can you rename columns during read?

Yes.

Example:

spark.read.csv(path).toDF("id","name","salary")


# COMMAND ----------

1️⃣2️⃣ How do you rename a column after aggregation? (skip for now, to be covered lated)

Example:

df.groupBy("dept").sum("salary")

Column becomes:

sum(salary)

Rename:

df.groupBy("dept").sum("salary") \
.withColumnRenamed("sum(salary)", "total_salary")

Better approach:

df.groupBy("dept").agg(sum("salary").alias("total_salary"))
🔹 Scenario-Based Questions (Most Important)


# COMMAND ----------

1️⃣3️⃣ You join two tables and get duplicate column names. How will you handle it?

Solution:

Rename columns before join.

df2 = df2.withColumnRenamed("id","customer_id")

# COMMAND ----------

1️⃣4️⃣ Your CSV file has 200 columns with spaces. How will you fix all column names?
df = df.toDF(*[c.replace(" ","_") for c in df.columns])


# COMMAND ----------

1️⃣5️⃣ Your source system changes column names frequently. How will you handle schema drift?

Use mapping dictionary or metadata-driven renaming.

# COMMAND ----------

🔹 Tricky Interview Questions
❓ Is column renaming a transformation or action?

It is a transformation (lazy).

❓ Does renaming trigger shuffle?

❌ No.
It only modifies metadata.

❓ Is column renaming expensive?

No — it is very cheap operation.
