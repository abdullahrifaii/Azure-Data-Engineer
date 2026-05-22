# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("txtcountry","")

# COMMAND ----------

dbutils.widgets.remove("txtcountry")

# COMMAND ----------

dbutils.widgets.text("txtcountry","India","Country")

# COMMAND ----------

country = dbutils.widgets.get("txtcountry")
print(country)

# COMMAND ----------

dbutils.widgets.combobox("cboCountry","",["india","uk","us"],"Country")

# COMMAND ----------

cnt = dbutils.widgets.get("cboCountry")
print(cnt)

# COMMAND ----------

dbutils.widgets.dropdown("dropCountry","india",["india","uk","us"],"Country2")

# COMMAND ----------

cnt = dbutils.widgets.get("dropCountry")
print(cnt)

# COMMAND ----------

dbutils.widgets.multiselect("multiCountry","india",["india","uk","us"],"Country3")

# COMMAND ----------

cnt = dbutils.widgets.get("multiCountry")
print(cnt)

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------


