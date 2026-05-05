# Databricks notebook source
# MAGIC %run /Workspace/PySpark/Utilities

# COMMAND ----------

df = read_csv_df(
    "/Volumes/cdudevcatalog/bronze/datavol/Orders/Orders.csv",
    infer_schema=True
)
display(df)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import when

df2 = df.withColumn("price_category", when(df["price"] > 4000, "high"))
display(df2)

# COMMAND ----------

df2.explain(True)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import when

df3 = df.withColumn("price_category", when(df["price"] > 4000, "high").otherwise("low"))
display(df3)

# COMMAND ----------

df3.explain(True)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import when, col

df3 = df.withColumn("price_category", when(col("price") > 4000, "high").otherwise("low"))
display(df3)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import when, col

df4 = df.withColumn(
    "price_category",
    when(col("price") > 4000, "high")
    .when(col("price") > 3000, "medium")
    .when(col("price") > 2000, "low")
    .otherwise("cheap")
)
display(df4)

# COMMAND ----------

df5 = df2.withColumn("price_category",when(col("price_category").isNull(),"NA").otherwise(col("price_category")))
display(df5)

# COMMAND ----------

df5.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔥 Spark Interview Questions

# COMMAND ----------

1️⃣ What is the equivalent of SQL CASE WHEN in PySpark?

Answer

when().otherwise()

Example

from pyspark.sql.functions import when, col

df.withColumn(
    "status",
    when(col("amount") > 1000, "High")
    .otherwise("Low")
)



# COMMAND ----------

2️⃣ What happens if otherwise() is not used?

Answer

Rows that don't match any condition become NULL.


# COMMAND ----------

3️⃣ Can you chain multiple conditions?

Yes.

when(cond1,val1).when(cond2,val2).otherwise(val3)


# COMMAND ----------

4️⃣ Can when() be used inside select()?

Yes.

df.select(
    when(col("salary") > 5000,"High").alias("salary_flag")
)


# COMMAND ----------

5️⃣ Is when() a transformation or action?

Transformation

Spark applies it lazily.


# COMMAND ----------

6️⃣ Does when() trigger shuffle?

No.

It is a narrow transformation.

# COMMAND ----------

7️⃣ How does Spark internally implement when()?

Spark converts it into CASE WHEN expression in Catalyst optimizer.

# COMMAND ----------

8️⃣ How do you replace NULL values using when()?
df.withColumn(
    "salary",
    when(col("salary").isNull(),0).otherwise(col("salary"))
)

# COMMAND ----------

9️⃣ Difference between when() and filter()?
when	filter
creates new column	filters rows

)


# COMMAND ----------

🔟 How to create multiple conditional columns?

Example

df.select(
    "*",
    when(col("salary") > 5000,"High").alias("flag1"),
    when(col("age") > 40,"Senior").alias("flag2")
)


# COMMAND ----------

1️⃣1️⃣ Can when() be used with groupBy()?

Yes.

Example

df.groupBy("dept").agg(
    sum(when(col("salary") > 5000,1).otherwise(0))

# COMMAND ----------

1️⃣2️⃣ How to count records based on condition?
from pyspark.sql.functions import sum

df.select(sum(when(col("amount") > 1000,1).otherwise(0)))

# COMMAND ----------

1️⃣3️⃣ Can when() be used in window functions?

Yes.

Example

df.withColumn(
    "flag",
    when(col("rank") == 1,"Top")
)

# COMMAND ----------

1️⃣4️⃣ How to avoid long when() chains?

Use lookup tables or joins.

Example:

Instead of

when(col("code")==1,"A")
.when(col("code")==2,"B")

Use mapping table.


# COMMAND ----------

1️⃣5️⃣ Can when() handle complex boolean conditions?

Yes.

Example

when((col("salary") > 5000) & (col("age") > 30),"Eligible")


# COMMAND ----------

1️⃣6️⃣ Can when() be used for data standardization?

Yes.

Example

when(col("country") == "US","United States")

# COMMAND ----------

1️⃣7️⃣ What happens if multiple when() conditions match?

Spark returns the first matching condition.

# COMMAND ----------

1️⃣8️⃣ Can when() be nested?

Yes.

Example

when(cond1,
    when(cond2,"A").otherwise("B")
)


# COMMAND ----------

1️⃣9️⃣ What datatype does when() return?

Depends on values returned.

Example

when(cond,1).otherwise(0)

Returns integer.

# COMMAND ----------

2️⃣0️⃣ Can when() return different datatypes?

No.

All branches must have compatible types.

# COMMAND ----------

2️⃣1️⃣ Can when() be used in Spark SQL?

Yes.

Example

CASE
WHEN salary > 5000 THEN 'High'
ELSE 'Low'
END


# COMMAND ----------

2️⃣2️⃣ What is the performance impact of many when() conditions?

Large condition chains may:

increase execution plan complexity

reduce readability

Better approach → lookup tables.

# COMMAND ----------

2️⃣3️⃣ How to categorize values using ranges?

Example

when(col("score") < 50,"Fail") \
.when(col("score") < 80,"Pass") \
.otherwise("Excellent")


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

2️⃣4️⃣ How to convert boolean to integer?
when(col("active") == True,1).otherwise(0)

# COMMAND ----------

2️⃣5️⃣ How to apply when() dynamically?

Using loops.

Example

for col_name in cols:
    df = df.withColumn(
        col_name,
        when(col(col_name).isNull(),"Unknown")
        .otherwise(col(col_name))
    )

# COMMAND ----------

What is when() in PySpark?

Equivalent of SQL CASE WHEN.

❓ Can you chain multiple conditions?

Yes.

when().when().otherwise()
❓ What happens if otherwise() is not used?

Result will be NULL.

❓ Can when() be used in select()?

Yes.

df.select(when(col("salary") > 5000, "High"))
