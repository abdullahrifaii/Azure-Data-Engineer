# Databricks notebook source
# MAGIC %md
# MAGIC ## 🔥 What is SparkSQL?
# MAGIC
# MAGIC SparkSQL is a module in Apache Spark that allows you to run SQL queries on distributed data.
# MAGIC
# MAGIC 👉 Think of it as:
# MAGIC
# MAGIC **_“SQL engine + Big Data processing + Distributed execution”_**
# MAGIC
# MAGIC **It lets you query:**
# MAGIC
# MAGIC - DataFrames
# MAGIC - Tables (Hive / Delta)
# MAGIC - Files (CSV, Parquet, JSON)

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Why SparkSQL exists ?
# MAGIC
# MAGIC Traditional SQL engines:
# MAGIC
# MAGIC Work on single machine / limited scale
# MAGIC
# MAGIC **SparkSQL:**
# MAGIC
# MAGIC - Works on distributed cluster
# MAGIC - Handles TBs–PBs of data
# MAGIC - Optimized using Catalyst + Tungsten

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️** Core Components of SparkSQL**
# MAGIC **1. DataFrame API (foundation)**
# MAGIC
# MAGIC A DataFrame = distributed table with schema
# MAGIC
# MAGIC **2. SQL Interface**
# MAGIC
# MAGIC You can run SQL directly:
# MAGIC
# MAGIC **3. Catalog**
# MAGIC
# MAGIC Manages:
# MAGIC
# MAGIC Tables
# MAGIC Databases
# MAGIC Views
# MAGIC
# MAGIC **4. Hive Integration**
# MAGIC
# MAGIC SparkSQL can connect with:
# MAGIC
# MAGIC - Hive Metastore
# MAGIC - External tables

# COMMAND ----------

# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

# Load Orders CSV file into a DataFrame
# infer_schema=True automatically detects column data types
df = read_csv_df(
    "/Volumes/cdudevcatalog/bronze/datavol/data/Orders_new.csv",
    infer_schema=True
)

# Display the DataFrame in table format
display(df)

# COMMAND ----------

# Display documentation for the spark.sql() method
# Shows parameters, return type, and usage examples
help(spark.sql)

# COMMAND ----------

# Attempt to query DataFrame 'df' directly without registration
# WARNING: This will fail because 'df' is not a registered table or view
df2 = spark.sql("SELECT * FROM df WHERE Year = 2024")

# Display the results (if query succeeds)
display(df2)

# COMMAND ----------

# Query DataFrame using parameter binding (RECOMMENDED approach)
# {df_name} is a placeholder that gets replaced with the actual DataFrame
# df_name=df passes the DataFrame as a named parameter
df2 = spark.sql("SELECT * FROM {df_name} WHERE Year = 2024", df_name=df)

# Display the filtered results
display(df2)

# COMMAND ----------

# Aggregate using DataFrame API
# groupBy("product") - Group rows by the product column
# sum("quantity") - Calculate sum of quantity for each product
# alias() - Rename the aggregated column (note: should be on the sum result)
df_prodwise_agg = df.groupBy("product").sum("quantity").alias("total_quantity")

# Display aggregated results
display(df_prodwise_agg)

# COMMAND ----------



# COMMAND ----------

# Aggregate using SQL syntax (equivalent to previous cell)
# SELECT product, SUM(quantity) - Calculate sum per product
# GROUP BY product - Group rows by product column
# {df} - Parameter binding to reference the DataFrame
df_prodwise_agg2 = spark.sql("""
    SELECT product, SUM(quantity) AS total_quantity
    FROM {df}
    GROUP BY product
""", df=df)

# Display SQL aggregation results
display(df_prodwise_agg2)
