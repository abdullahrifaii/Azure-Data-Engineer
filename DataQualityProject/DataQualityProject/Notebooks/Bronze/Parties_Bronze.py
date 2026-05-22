# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Set Variables

# COMMAND ----------

Entity = "Parties"
manifest = "Purchase"

# COMMAND ----------

# MAGIC %md ###Read From Delta Raw Path

# COMMAND ----------

partiesdf = readEntity(manifest,Entity)
display(partiesdf)


# COMMAND ----------

# MAGIC %md ###Save to Bronze schema

# COMMAND ----------

saveDeltaTableToCatalog(partiesdf,"dataquality","Bronze",Entity)

# COMMAND ----------


