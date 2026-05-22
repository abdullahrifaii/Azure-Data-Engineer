# Databricks notebook source
import pyspark.sql.functions as F 
import datetime
import pandas as pd
import dateutil

# COMMAND ----------

ADLS_DEV_BASE_PATH = "abfss://oaonoperationsdev@90111adlsdev.dfs.core.windows.net/"
DELTALAKE_RAW_PATH = "DeltaLake/Raw/"

# COMMAND ----------

"""service_credential = dbutils.secrets.get(scope="adbdevscope",key="ClientSecret")
appid = dbutils.secrets.get(scope="adbdevscope",key="appid")
tenantid = dbutils.secrets.get(scope="adbdevscope",key="tenantid")"""

# COMMAND ----------

"""service_credential = dbutils.secrets.get(scope="adbdevscope",key="ClientSecret")
appid = dbutils.secrets.get(scope="adbdevscope",key="appid")
tenantid = dbutils.secrets.get(scope="adbdevscope",key="tenantid")

spark.conf.set("fs.azure.account.auth.type.90111adlsdev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.90111adlsdev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.90111adlsdev.dfs.core.windows.net", appid)
spark.conf.set("fs.azure.account.oauth2.client.secret.90111adlsdev.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.90111adlsdev.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantid}/oauth2/token")"""

# COMMAND ----------

def read_entity(manifestPath,entity):
    df = (spark.read.format("com.microsoft.cdm")
    .option("storage", "90111adlsdev.dfs.core.windows.net")
    .option("appid",appid)
    .option("appkey",service_credential) 
    .option("tenantid",tenantid)
    .option("manifestPath", f"oaonoperationsdev/oaon-sandbox.operations.dynamics.com/Tables/{manifestPath}/{manifestPath}.manifest.cdm.json")
    .option("entity", entity)
    #  .option("mode", "permissive")
    .load())
    return df

# COMMAND ----------

import json

def readEntity(manifest, entity):
    """
    Reads a CSV file without headers from a Databricks Volume,
    gets column names from the matching .cdm.json file,
    and returns a Spark DataFrame.

    Parameters
    ----------
    manifest : str
        Folder/manifest name, e.g. "Hr"
    entity : str
        File base name without extension, e.g. "WorkerTable"

    Returns
    -------
    pyspark.sql.DataFrame
    """

    base_path = "/Volumes/dataquality/bronze/dataquality/Datasets/"  # <-- change this to your real volume path

    csv_path = f"{base_path}/{manifest}/{entity}.csv"
    json_path = f"{base_path}/{manifest}/{entity}.cdm.json"

    # Read JSON file as text and parse it
    json_text = "\n".join(row.value for row in spark.read.text(json_path).collect())
    metadata = json.loads(json_text)

    # Extract column names from definitions[0].hasAttributes
    definitions = metadata.get("definitions", [])
    if not definitions:
        raise ValueError(f"No 'definitions' found in JSON file: {json_path}")

    definition = definitions[0]

    columns = [
        attr["name"]
        for attr in definition.get("hasAttributes", [])
        if isinstance(attr, dict) and "name" in attr
    ]

    if not columns:
        raise ValueError(f"No column names found in hasAttributes in JSON file: {json_path}")

    # Read CSV with no header
    df = (
        spark.read.format("csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .load(csv_path)
    )

    # Validate column count
    if len(df.columns) != len(columns):
        raise ValueError(
            f"Column mismatch for entity '{entity}': "
            f"CSV has {len(df.columns)} columns but JSON has {len(columns)} columns."
        )

    # Rename columns
    df = df.toDF(*columns)

    return df


# COMMAND ----------

def writeRawToDeltaLake(entityDf,deltaLakePath):
    entityDf.write.mode("overwrite").option("overwriteSchema","True").option("path",ADLS_DEV_BASE_PATH + deltaLakePath).save()

# COMMAND ----------

def readFromDeltaPath(entityName):
    df = (spark.read.format("delta")
      .option("path",f"{ADLS_DEV_BASE_PATH}/{DELTALAKE_RAW_PATH}{entityName}")
      .load()
      )
    return df


# COMMAND ----------

def saveDeltaTableToCatalog(df,catalog,schema,tableName):
    schema = schema.lower()
    tableName = tableName.lower()
    #spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{tableName}")


# COMMAND ----------


