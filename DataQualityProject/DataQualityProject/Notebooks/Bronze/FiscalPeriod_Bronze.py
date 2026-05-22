# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Set Variables

# COMMAND ----------

Entity = "FiscalPeriod"
manifest = "Others"

# COMMAND ----------

# MAGIC %md ###Read From Delta Raw Path

# COMMAND ----------

fiscalperioddf = readEntity(manifest,Entity)
display(fiscalperioddf)


# COMMAND ----------

# MAGIC %md ###Save to Bronze schema

# COMMAND ----------

saveDeltaTableToCatalog(fiscalperioddf,"dataquality","Bronze",Entity)

# COMMAND ----------


