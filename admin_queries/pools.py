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

pools = requests.get(url + '/api/2.0/instance-pools/list', headers=_token).json()

# COMMAND ----------

import pandas as pd

# used pandas because spark couldn't infer the schema from straight json response
pools_df = (
  spark.createDataFrame(pd.DataFrame(pools['instance_pools']))
  .select(
  'instance_pool_name',
  'instance_pool_id',
  'default_tags',
  'custom_tags'
  )
)
pools_df.write.mode('overwrite').saveAsTable(f'{target_schema}.pools')

# COMMAND ----------


