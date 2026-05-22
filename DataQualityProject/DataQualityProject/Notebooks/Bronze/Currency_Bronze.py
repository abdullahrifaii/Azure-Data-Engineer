# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Set Variables

# COMMAND ----------

Entity = "Currency"
manifest = "Others"

# COMMAND ----------

# MAGIC %md ###Read From Delta Raw Path

# COMMAND ----------

costcenterdf = readEntity(manifest,Entity)
display(costcenterdf)

# COMMAND ----------

# MAGIC %md ###Save to Bronze schema

# COMMAND ----------

saveDeltaTableToCatalog(costcenterdf,"dataquality","bronze",Entity)

# COMMAND ----------


