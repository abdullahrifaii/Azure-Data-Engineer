# Databricks notebook source
from pyspark.sql.types import StructType

def read_csv_df(
    path: str,
    header: bool = True,
    infer_schema: bool = False,
    schema: StructType | str | None = None,
    delimiter: str = ","
):
    """
    Reusable function to read CSV files in PySpark.

    Parameters:
    - path         : File path / folder / wildcard
    - header       : Whether CSV has header
    - infer_schema : Enable schema inference
    - schema       : StructType or DDL schema string
    - delimiter    : Column delimiter

    Returns:
    - DataFrame
    """

    reader = spark.read.options(
        header=header,
        inferSchema=infer_schema,
        delimiter=delimiter,
        
    )

    if schema:
        reader = reader.schema(schema)

    df = reader.csv(path)
    return df


# COMMAND ----------



# COMMAND ----------

df = read_csv_df( "/Volumes/mycatalog/default/myvolume2/Orders.csv")
display(df)


# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df2 = read_csv_df(
    "/Volumes/mycatalog/default/myvolume2/Orders.csv",
    infer_schema=True
)


# COMMAND ----------

schema = "id INT, name STRING, age INT, country STRING, department STRING, salary INT, experience_years INT, email STRING, status STRING, join_date STRING"


df3 = read_csv_df(
    "/Volumes/mycatalog/default/myvolume2/Orders.csv",
    infer_schema=True,
    schema=schema
)

display(df3)


# COMMAND ----------

df4 = read_csv_df(
    "/Volumes/cdudevcatalog/bronze/employees/employees_delimited.csv",
    delimiter="|",
)
display(df4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔥 Spark Interview Questions

# COMMAND ----------

1️⃣ Why is this function reusable?

Answer:
It abstracts configuration, avoids repetition, and enforces best practices across pipelines.

# COMMAND ----------

2️⃣ Why check schema before applying?

Answer:
To avoid schema inference when an explicit schema is provided.

# COMMAND ----------

3️⃣ How does this improve maintainability?

Answer:
Changes are made in one place instead of across multiple notebooks.

# COMMAND ----------

🧠 One-Line Interview Summary

“I use a reusable CSV reader function to enforce consistent schema handling, improve performance, and reduce code duplication.”
