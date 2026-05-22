# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Set Variables

# COMMAND ----------

Entity = "CustTable"
manifest = "Sales"

# COMMAND ----------

# MAGIC %md ###Read From Delta Raw Path

# COMMAND ----------

custTableDf = readEntity(manifest,Entity)
display(custTableDf)


# COMMAND ----------

# MAGIC %md ###Save to Bronze schema

# COMMAND ----------

saveDeltaTableToCatalog(custTableDf,"dataquality","bronze",Entity)

# COMMAND ----------


