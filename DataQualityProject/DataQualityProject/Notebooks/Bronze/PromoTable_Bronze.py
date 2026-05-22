# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Set Variables

# COMMAND ----------

Entity = "PromoTable"
manifest = "Sales"

# COMMAND ----------

# MAGIC %md ###Read From Delta Raw Path

# COMMAND ----------

PromoTabledf = readEntity(manifest, Entity)
display(PromoTabledf)


# COMMAND ----------

# MAGIC %md ###Save to Bronze schema

# COMMAND ----------

saveDeltaTableToCatalog(PromoTabledf,"dataquality","Bronze",Entity)

# COMMAND ----------


