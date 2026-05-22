# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Set Variables

# COMMAND ----------

Entity = "SalesOrderLine"
manifest = "Sales"

# COMMAND ----------

# MAGIC %md ###Read From Delta Raw Path

# COMMAND ----------

SalesOrderLinedf = readEntity(manifest,Entity)
display(SalesOrderLinedf)


# COMMAND ----------

# MAGIC %md ###Save to Bronze schema

# COMMAND ----------

saveDeltaTableToCatalog(SalesOrderLinedf,"dataquality","Bronze",Entity)

# COMMAND ----------


