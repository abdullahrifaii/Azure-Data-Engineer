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

def rename_columns_case(df, case="lower"):
    if case == "lower":
        return df.toDF(*[c.lower() for c in df.columns])
    elif case == "upper":
        return df.toDF(*[c.upper() for c in df.columns])
    else:
        raise ValueError("case must be 'lower' or 'upper'")
