# Databricks notebook source
from pyspark.sql.functions import col
import pyspark.sql.functions as f
import requests

# COMMAND ----------

# Target schema for CPE to publish table
dbutils.widgets.text('target_schema', 'cpe.sys_table')
target_schema = dbutils.widgets.get('target_schema')

# COMMAND ----------

# Set up auth
# TODO Change URL to account level scim API - not workspace like I have
# https://api-docs.databricks.com/rest/latest/account-scim-api.html
url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None) 
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
_token = {'Authorization': 'Bearer {0}'.format(token)}

# COMMAND ----------

catalogs = requests.get(url + '/api/2.1/unity-catalog/catalogs', headers=_token).json()

# COMMAND ----------

import json
data = {
"name": "kp_catalog_",
"owner": "riley.rustad@databricks.com",
"comment": "",
"isolation_mode": "OPEN"
}

r = requests.patch(url + '/api/2.1/unity-catalog/catalogs/kp_catalog', headers=_token,data=json.dumps(data))

# COMMAND ----------

r.json()

# COMMAND ----------

catalogs

# COMMAND ----------

import pandas as pd

# used pandas because spark couldn't infer the schema from straight json response
catalogs_df = (
  spark.createDataFrame(pd.DataFrame(catalogs['catalogs']))
)
catalogs_df.write.mode('overwrite').saveAsTable(f'{target_schema}.catalogs')

# COMMAND ----------


