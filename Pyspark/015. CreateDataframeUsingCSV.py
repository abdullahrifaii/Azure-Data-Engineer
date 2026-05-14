# Databricks notebook source
help(spark.read)

# COMMAND ----------

help(spark.read.csv)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic CSV Read

# COMMAND ----------



# COMMAND ----------

df = spark.read.csv("/Volumes/cdudevcatalog/bronze/datavol/Employees.csv")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read CSV with Header

# COMMAND ----------



# COMMAND ----------

df = spark.read.csv(
    "/Volumes/cdudevcatalog/bronze/datavol/Employees.csv",
    header=True
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read CSV with Schema Inference

# COMMAND ----------



# COMMAND ----------

df = spark.read.csv(
    "/Volumes/cdudevcatalog/bronze/datavol/Employees.csv",
    header=True,
    inferSchema=True
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read CSV with Explicit Schema (BEST PRACTICE ⭐)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("experience_years", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("status", StringType(), True),
    StructField("join_date", StringType(), True)
])

df = spark.read.csv(
    "/Volumes/cdudevcatalog/bronze/datavol/Employees.csv",
    header=True,
    schema=schema
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Fast
# MAGIC ✅ Safe
# MAGIC ✅ Production-ready

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read CSV using DDL Schema (SQL Style)

# COMMAND ----------

schema = "id INT, name STRING, age INT, country STRING, department STRING, salary INT, experience_years INT, email STRING, status STRING, join_date STRING"

df = spark.read \
    .schema(schema) \
    .csv("/Volumes/cdudevcatalog/bronze/datavol/Employees.csv", header=True)
display(df)

# COMMAND ----------

schema = "id INT, name STRING, age INT, country STRING, department STRING, salary INT, experience_years INT, email STRING, status STRING, join_date STRING"

df = (spark.read
    .schema(schema)
    .csv("/Volumes/cdudevcatalog/bronze/datavol/Employees.csv", header=True)
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read folder containing multiple CSV Files

# COMMAND ----------

df = spark.read.csv("/Volumes/cdudevcatalog/bronze/employees/*.csv", header=True)
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Read specific csv files in a folder

# COMMAND ----------

df = spark.read.csv([
    "/Volumes/cdudevcatalog/bronze/employees/employees_1.csv",
    "/Volumes/cdudevcatalog/bronze/employees/employees_3.csv"
], header=True)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read CSV with Custom Options

# COMMAND ----------

display(spark.read.text("/Volumes/cdudevcatalog/bronze/employees/employees_delimited.csv"))

# COMMAND ----------

df = spark.read.options(
    header=True,
    inferSchema=True,
    delimiter="|"
).csv("/Volumes/cdudevcatalog/bronze/employees/employees_delimited.csv")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ▦ DataFrame Insights

# COMMAND ----------

df.count() # returns count of rows

# COMMAND ----------

df.columns # returns a list of columns

# COMMAND ----------

len(df.columns) # returns count of columns

# COMMAND ----------

df.printSchema() # prints dataframe schema

# COMMAND ----------

df.schema # returns dataframe schema as structtype

# COMMAND ----------

display(df.summary()) # returns summary about dataframe


# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔥 Spark Interview Questions

# COMMAND ----------

1️⃣ What happens if you don’t specify header=True while reading CSV?

Answer:

First row is treated as data, not column names

Spark auto-generates column names: _c0, _c1, _c2...

# COMMAND ----------

2️⃣ What is inferSchema and how does it work?

Answer:

df = spark.read.csv(
    "Employees.csv",
    header=True,
    inferSchema=True
)


Spark scans the data

Tries to guess data types (INT, STRING, DATE, etc.)

Adds extra job overhead

⚠️ Not recommended for large files or production

# COMMAND ----------

3️⃣ Why is schema inference NOT recommended in production?

Answer:

Requires extra Spark job

Slower on large datasets

Schema may differ across files

Less predictable

👉 Explicit schema = faster & safer

# COMMAND ----------

4️⃣ What is the BEST PRACTICE to read CSV in Spark?

Answer: Use an explicit schema

# COMMAND ----------

5️⃣ What are the benefits of using an explicit schema?

Answer:

No schema inference job

Better performance

Data validation upfront

Required for production pipelines

# COMMAND ----------

6️⃣ What is DDL schema and how is it different?
Answer:
DDL schema uses SQL-style definition:
    
✔ Cleaner
✔ Easier to write
✔ Same performance as StructType

# COMMAND ----------

7️⃣ How do you inspect DataFrame structure?

Answer:
    
df.printSchema()
df.schema
df.columns
len(df.columns)


# COMMAND ----------

8️⃣ What happens internally when reading CSV in Spark?

Answer:

File is split into partitions

Each partition is processed by executors

Data is converted into DataFrame backed by RDD

Optimized via Catalyst + Tungsten

# COMMAND ----------


