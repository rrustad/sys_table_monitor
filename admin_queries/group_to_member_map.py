# Databricks notebook source
from pyspark.sql.functions import col
import pyspark.sql.functions as f
import requests

# COMMAND ----------

# Target schema for CPE to publish table
dbutils.widgets.text('target_schema', 'cpe.sys_table')
target_schema = dbutils.widgets.get('target_schema')

# comma delimited string of tenant/project group prefixes to assign tenant to users
dbutils.widgets.text('tenant_prefixes', 'kp,tenant')
tenant_prefixes = dbutils.widgets.get('tenant_prefixes').split(',')
target_schema,tenant_prefixes

# COMMAND ----------

# Set up auth
# TODO Change URL to account level scim API - not workspace like I have
# https://api-docs.databricks.com/rest/latest/account-scim-api.html
url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None) 
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
_token = {'Authorization': 'Bearer {0}'.format(token)}

# list all onboarded tenants - could be parameterized if you want to set this up as a regular job
tenant_prefixes = ['kp', 'tenant']

# COMMAND ----------

groups = requests.get(url + '/api/2.0/preview/scim/v2/Groups', headers=_token).json()['Resources']

# COMMAND ----------

users = requests.get(url + '/api/2.0/preview/scim/v2/Users', headers=_token).json()

# COMMAND ----------

# Determine which users are in which group
group_to_member_map = (
  spark.createDataFrame(groups)
  .select(
    'displayName',
    col('id').alias('group_id'),
    f.explode(col('members')).alias('member')
  )
  .withColumn('member_id', f.expr('member["value"]'))
  .drop('member')
  # This assumes that there's a consistent naming convention that wouldn't have tenants somehow overlap
  .withColumn('tenant', f.split(col('displayName'),'_')[0])
  .filter(col('tenant').isin(tenant_prefixes))
)

group_to_member_map.write.saveAsTable(f'{target_schema}.group_to_member_map')

# COMMAND ----------

spark.table(f'{target_schema}.group_to_member_map').display()

# COMMAND ----------


