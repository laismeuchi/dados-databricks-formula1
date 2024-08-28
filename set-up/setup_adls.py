# Databricks notebook source
# dbutils.secrets.listScopes()
# dbutils.secrets.list(scope='formula1-scope')
formula1_account_key = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-account-key')

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-tentant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlmeuchi.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlmeuchi.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlmeuchi.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlmeuchi.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlmeuchi.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlmeuchi.dfs.core.windows.net"))

# COMMAND ----------

dbutils.fs.head("abfss://demo@formula1dlmeuchi.dfs.core.windows.net/circuits.csv")


# COMMAND ----------

display(spark.read.csv("abfss://teste@formula1dlmeuchi.dfs.core.windows.net/circuits.csv"))
