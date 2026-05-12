# Databricks notebook source
# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

df = read_csv_df(
    "/Volumes/cdudevcatalog/bronze/datavol/data/Orders_new.csv",
    infer_schema=True
)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col,round
df2 = df.withColumn("total_amount", round(col("quantity") * col("price"),2))
display(df2)

# COMMAND ----------

help(df2.groupBy("product").pivot)

# COMMAND ----------

df_pivot = df2.groupBy("product").pivot("year").sum("total_amount")
display(df_pivot)

# COMMAND ----------

from pyspark.sql.functions import sum, round
df_pivot = df2.groupBy("product").pivot("year").agg(round(sum("total_amount"), 2))

display(df_pivot)

# COMMAND ----------

df_pivot.explain(True)

# COMMAND ----------

                📂 File Scan (CSV)
        product | quantity | price | year
                        │
                        ▼
            🧮 Compute total_amount
        total_amount = quantity * price
                        │
                        ▼
        🔹 Partial Aggregation (product, year)
           sum(total_amount)
                        │
                        ▼
        🔹 Shuffle (product, year)
        Exchange → group same keys together
                        │
                        ▼
        🔹 Final Aggregation (product, year)
           total sales per year
                        │
                        ▼
        🔹 Pivot Preparation
        Convert year values → columns
        (2024, 2025, 2026)
                        │
                        ▼
        🔹 Shuffle (by product)
                        │
                        ▼
        🔹 Final Pivot Aggregation
        pivotfirst → assign values to columns
                        │
                        ▼
        📊 Final Output

        product | 2024 | 2025 | 2026

# COMMAND ----------

df_pivot2 = df2.groupBy("product").pivot("year", [2024, 2025]).agg(round(sum("total_amount"), 2))

display(df_pivot2)

# COMMAND ----------

df_pivot2.explain(True)

# COMMAND ----------

help(df_pivot2.unpivot)

# COMMAND ----------

df_unpivot = df_pivot.unpivot("product", ["2024","2025","2026"],"year","sales")
display(df_unpivot)

# COMMAND ----------

df_unpivot.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## # 🔥 Spark Interview Questions

# COMMAND ----------

1️⃣ What is pivot in PySpark?

Converts row values into columns.


# COMMAND ----------


2️⃣ Does pivot require aggregation?

✅ Yes

pivot().sum()

# COMMAND ----------

3️⃣ Why is pivot expensive?

Because it:

causes shuffle
creates many columns
increases memory usage

# COMMAND ----------

4️⃣ What happens if pivot column has many values?

Creates many columns → performance issue

# COMMAND ----------

5️⃣ How to optimize pivot?
pivot("product", ["Laptop","Phone"])

👉 Limits columns


# COMMAND ----------


6️⃣ What is unpivot in PySpark?

Converts columns into rows

# COMMAND ----------

7️⃣ Does PySpark have direct unpivot?

yes as of a new version
previously
✔ Use stack()

# COMMAND ----------

8️⃣ Difference: pivot vs groupBy
pivot	groupBy
creates columns	creates rows
used for reporting	used for aggregation
🔹 Scenario-Based Interview Questions


# COMMAND ----------

9️⃣ Scenario: Sales report per product
df.groupBy("year").pivot("product").sum("sales")

# COMMAND ----------

🔟 Scenario: Normalize wide table
stack()

# COMMAND ----------

1️⃣1️⃣ Scenario: Pivot with multiple aggregations
df.groupBy("year").pivot("product").agg(sum("sales"), avg("sales"))

# COMMAND ----------

1️⃣2️⃣ Scenario: Missing values after pivot

👉 Fill nulls

df.fillna(0)

# COMMAND ----------

1️⃣3️⃣ Scenario: Dynamic pivot issue

👉 Too many unique values → performance issue




# COMMAND ----------

🔥 Advanced Interview Questions

1️⃣4️⃣ Does pivot cause shuffle?

✅ Yes

# COMMAND ----------

1️⃣5️⃣ Can pivot be done without groupBy?

❌ No

# COMMAND ----------

1️⃣6️⃣ What is wide vs narrow transformation here?

Pivot = wide transformation

# COMMAND ----------

1️⃣7️⃣ Can you pivot multiple columns?

Indirectly using composite keys


# COMMAND ----------

1️⃣8️⃣ What is the alternative to pivot?
groupBy + conditional aggregation

# COMMAND ----------

1️⃣9️⃣ How does Spark execute pivot internally?
groupBy
shuffle
aggregation

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

2️⃣0️⃣ When NOT to use pivot?
High cardinality columns
Large datasets
