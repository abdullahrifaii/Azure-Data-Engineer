# Databricks notebook source
# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

df = read_csv_df(
    "/Volumes/cdudevcatalog/bronze/datavol/Drop/Users.csv",
    infer_schema=True
)
display(df)

# COMMAND ----------

df2=df.drop("salary","temp_flag")
display(df2)

# COMMAND ----------

df2.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenarios
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.Drop Columns That Start With a Prefix
# MAGIC
# MAGIC 🎯 Drop columns starting with "temp_"

# COMMAND ----------

cols_to_drop = [c for c in df.columns if c.startswith("temp_")]

df_clean = df.drop(*cols_to_drop)

display(df_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 What’s happening?
# MAGIC
# MAGIC df.columns → list of column names
# MAGIC
# MAGIC List comprehension filters pattern
# MAGIC
# MAGIC * unpacks list into arguments

# COMMAND ----------

df_clean.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2: Drop Columns Containing a Keyword
# MAGIC
# MAGIC 🎯 Drop columns containing "meta"

# COMMAND ----------

cols_to_drop = [c for c in df.columns if "meta" in c]

df_clean = df.drop(*cols_to_drop)

display(df_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3: Drop Columns Using Regex (Advanced & Flexible)
# MAGIC
# MAGIC 🎯 Drop columns matching regex pattern

# COMMAND ----------

import re

pattern = r"^temp_|^meta_"

cols_to_drop = [c for c in df.columns if re.match(pattern, c)]

df_clean = df.drop(*cols_to_drop)

display(df_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔥 Spark Interview Questions

# COMMAND ----------

1️⃣ What does drop() do in PySpark?

Answer:

drop() removes one or more columns from a DataFrame.


It returns a new DataFrame (DataFrames are immutable).

Internally:

Spark creates a new logical plan

A Project node is generated

Only remaining columns are projected forward

# COMMAND ----------

2️⃣  Production Scenario: Why drop by pattern?

Example:

Raw ingestion contains:

temp_* → staging columns

meta_* → ingestion metadata

debug_* → pipeline debugging fields

Before loading into curated table, we drop:

df_clean = df.drop(*[c for c in df.columns if c.startswith(("temp_", "meta_"))])


Reason:

Reduce storage

Reduce shuffle width

Improve downstream performance

Avoid exposing internal metadata

# COMMAND ----------

3️⃣ What happens internally when you use drop()?

Internally Spark converts it to a Project logical plan.

Even though you wrote:

df.drop("temp_flag")


Spark rewrites it as:

SELECT id, name, salary, ...


So drop() = select(all_columns_except_dropped)
