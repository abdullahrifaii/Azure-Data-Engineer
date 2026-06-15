# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Set Variables

# COMMAND ----------

Entity = "PurchaseOrder"
manifest = "Purchase"

# COMMAND ----------

# MAGIC %md ###Read From Delta Raw Path

# COMMAND ----------

partyAddressdf = readEntity(manifest,Entity)
display(partyAddressdf)


# COMMAND ----------

# MAGIC %md ###Save to Bronze schema

# COMMAND ----------

saveDeltaTableToCatalog(partyAddressdf,"dataquality","Bronze",Entity)

# COMMAND ----------


