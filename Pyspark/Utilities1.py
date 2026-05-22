# Databricks notebook source
# MAGIC %pip install azure-keyvault-secrets azure-identity

# COMMAND ----------

# %pip install azure-keyvault-secrets azure-identity


# 1. Get the service credential provider by name
credential = dbutils.credentials.getServiceCredentialsProvider("keyvaultcred")

# 2. Initialize the cloud SDK client
from azure.keyvault.secrets import SecretClient
secret_client = SecretClient(
    vault_url="https://kvcdudev2.vault.azure.net/", 
    credential=credential
)

# 3. Retrieve the secret value
sqluser = secret_client.get_secret("sqlusername").value
sqlpass = secret_client.get_secret("sqldbpassword").value

# Secret values are automatically redacted in notebook output


# COMMAND ----------

server_name = "jdbc:sqlserver://cdusqlserverdev.database.windows.net"
database_name = "cdudevsqldb"
url = server_name + ";" + "databaseName=" + database_name + ";"



# COMMAND ----------

def ReadTableFromDatabase(Tablename):
    try:
        df = (spark.read.format("jdbc")
        .option("url",url)
        .option("username",sqluser)
        .option("password",sqlpass)
        .option("dbtable",Tablename).load()
        )
    except Exception as e:
        raise Exception    
    return df

# COMMAND ----------

def QueryFromDatabase(sqlquery):
    df = (spark.read.format("jdbc")
    .option("url",url)
    .option("username",sqluser)
    .option("password",sqlpass)
    .option("query",sqlquery).load()
    )
    return df


# COMMAND ----------

def WriteDataframeToDatabase(dfName,Tablename):
    (dfName.write
    .format("jdbc")
    .option("url", url) 
    .option("dbtable", Tablename)
    .option("user", sqluser) 
    .option("password", sqlpass) 
    .save()
        )

# COMMAND ----------

def WriteDataframeToDatabaseOverwrite(dfName,Tablename):
    (dfName.write.format("jdbc")
        .option("url",url)
        .option("username",sqluser)
        .option("password",sqlpass)
        .mode("overwrite")
        .option("dbtable",Tablename).save()
        )

# COMMAND ----------

def WriteDataframeToDatabaseMode(dfName,Tablename,writemode):
    (dfName.write.format("jdbc")
        .option("url",url)
        .option("username",sqluser)
        .option("password",sqlpass)
        .mode(writemode)
        .option("dbtable",Tablename).save()
        )

# COMMAND ----------

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
