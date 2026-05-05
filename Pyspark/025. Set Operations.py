# Databricks notebook source
In PySpark, set operations are used to combine or compare two DataFrames similar to SQL set operations. They treat DataFrames like sets of rows and operate on them.

The main set operations in PySpark are:

1️⃣ Union
2️⃣ UnionByName
3️⃣ Intersect
4️⃣ Except / Subtract

These correspond to SQL operations like UNION, INTERSECT, and EXCEPT.

# COMMAND ----------

# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

df1 = read_csv_df(
    "/Volumes/cdudevcatalog/bronze/datavol/data/Set1.csv",
    infer_schema=True
)
display(df1)


# COMMAND ----------

df2 = read_csv_df(
    "/Volumes/cdudevcatalog/bronze/datavol/data/Set2.csv",
    infer_schema=True
)
display(df2)


# COMMAND ----------

help(df1.union)

# COMMAND ----------

df_union = df1.union(df2)
display(df_union)

# COMMAND ----------

df_union.explain(True)

# COMMAND ----------

df_dedup = df1.union(df2).distinct()
display(df_dedup)

# COMMAND ----------

df_columnorder = read_csv_df(
    "/Volumes/cdudevcatalog/bronze/datavol/data/Set3.csv",
    infer_schema=True
)
display(df_columnorder)


# COMMAND ----------

df5 = df1.union(df_columnorder)
display(df5)

# COMMAND ----------

help(df1.unionByName)

# COMMAND ----------

df5 = df1.unionByName(df_columnorder)
display(df5)

# COMMAND ----------

df5.explain(True)

# COMMAND ----------

df_cols = df1.select("emp_id","name","product","txn_date").unionByName(df2,allowMissingColumns=True)
display(df_cols)

# COMMAND ----------

df_cols.explain(True)

# COMMAND ----------

df_Common = df1.intersect(df2)
display(df_Common)

# COMMAND ----------

df_Common.explain(True)

# COMMAND ----------

df_except = df1.exceptAll(df2)
display(df_except)

# COMMAND ----------

df_except1 = df2.exceptAll(df1)
display(df_except1)

# COMMAND ----------

df_except.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔥 Spark Interview Questions

# COMMAND ----------

1️⃣ Difference between union() and unionByName()
union	unionByName
matches columns by position	matches by name






# COMMAND ----------

2️⃣ Does union remove duplicates?

❌ No.

Union keeps duplicates.

To remove duplicates:

df.union(df2).distinct()


# COMMAND ----------

3️⃣ Are set operations narrow or wide transformations?

Most are wide transformations because they require shuffle.

# COMMAND ----------

4️⃣ When is intersect used?

To find common records between two datasets.

# COMMAND ----------

5️⃣ How do you find records present in source but missing in target?
df_source.except(df_target)
