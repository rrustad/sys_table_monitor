# Databricks notebook source
# MAGIC %md
# MAGIC What tables do we want? What are the requirements
# MAGIC 1. CPE to serve back all system table information to tenants  
# MAGIC     1. for each tenant, give them access to the table
# MAGIC 2. Create subsets of the audit table that's easier for users to parse/make available for dashboarding
# MAGIC     1. when is an object in my catalog permissions changed?
# MAGIC     1. What is the cost of the workloads on each cluster
# MAGIC
# MAGIC
# MAGIC TODOs:
# MAGIC * Is there any reason to use workspace groups? or is it always accounts?

# COMMAND ----------

from pyspark.sql.functions import col
import pyspark.sql.functions as f
import requests
import json

# COMMAND ----------

# MAGIC %sql
# MAGIC -- -- Make sure my schema exists
# MAGIC -- create catalog cpe;
# MAGIC -- create schema cpe.sys_table

# COMMAND ----------

# DBTITLE 1,Setup
# determine where my schema is
dbutils.widgets.text('cpe_schema', 'cpe.sys_table')
cpe_schema = dbutils.widgets.get('cpe_schema')

# Set up auth
# Change URL to account level scim API - not workspace like I have
# https://api-docs.databricks.com/rest/latest/account-scim-api.html
url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None) 
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
_token = {'Authorization': 'Bearer {0}'.format(token)}

# list all onboarded tenants - could be parameterized if you want to set this up as a regular job
tenant_prefixes = ['kp', 'tenant']

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter system.access.audit
# MAGIC
# MAGIC Assumptions:
# MAGIC * Gives tenant operator and data owner access to all rows in  system.access.audit associated in tenant
# MAGIC * All requests have an associated identity of the person making the request
# MAGIC   * This view assumes that each user is only part of one tenant
# MAGIC   * If user is in more than one tenant, then tenant leaders of all tenants that the user is in can see all of their actions
# MAGIC   * It's hard to know what operations are intended for which tenant outside of cluster operations, which are tagged

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from system.access.audit

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
  .withColumn('member_id', f.regexp_replace(f.expr('member["$ref"]'),'Users/',''))
  .drop('member')
  # This assumes that there's a consistent naming convention that wouldn't have tenants somehow overlap
  .withColumn('tenant', f.split(col('displayName'),'_')[0])
  .filter(col('tenant').isin(tenant_prefixes))
)
group_to_member_map.display()

# COMMAND ----------

users_df = (
  spark.createDataFrame(users['Resources'])
  .select(
    col('id').alias('member_id'),
    col('userName')
  )
)
users_df.limit(5).display()

# COMMAND ----------

member_map = group_to_member_map.join(users_df, 'member_id')
member_map.display()

# COMMAND ----------

#todo: is this necessary?
member_map.write.mode('overwrite').saveAsTable(f'{cpe_schema}.member_map')

# COMMAND ----------

group_to_member_map.display()

# COMMAND ----------

# determines which roles in a tenant to give access to
roles_to_give_access = ['operator', 'data_owner']
# This would be you guys
# roles_to_give_access = ['_test']
audit_map = {}
for tenant in tenant_prefixes:
  member_list = [user['userName'] for user in member_map.filter(col('tenant') == tenant).select('userName').distinct().collect()]
  tenant_roles = [f'{tenant}_{role}' for role in roles_to_give_access]
  audit_map[tenant] = {
    'member_list':member_list,
    'tenant_roles':tenant_roles
  }
audit_map

# COMMAND ----------

filter_predicates = []
for tenant, _map in audit_map.items():
  role_test = " OR ".join([f"is_account_group_member('{role}')" for role in _map['tenant_roles']])
  tenant_member_list = '", "'.join(_map['member_list'])
  filter_predicates.append(f"""({role_test}) and user_identity['email'] in ("{tenant_member_list}")""")
print(filter_predicates[1])

# COMMAND ----------

print(f"""create or replace view {cpe_schema}.audit_events_by_tenant as 
select *
from system.access.audit
where
1=1
and {' OR '.join(filter_predicates)}
 """)

# COMMAND ----------

spark.sql(
f"""create or replace view {cpe_schema}.audit_events_by_tenant as 
select *
from system.access.audit
where
1=1
and {' OR '.join(filter_predicates)}
 """

)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cpe.sys_table.audit_events_by_tenant

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lineage Tables

# COMMAND ----------

catalogs = spark.table('system.information_schema.catalogs')
catalogs.display()

# COMMAND ----------

catalogs_dict = dict(catalogs.select('catalog_name', 'catalog_owner').collect())
catalogs_dict

# COMMAND ----------

filter_predicates = []
for catalog, owner in catalogs_dict.items():
  filter_predicates.append(f'(is_account_group_member("{owner}") and source_table_catalog == "{catalog}")')
filter_predicates

# COMMAND ----------

filter_predicates_s = "\n OR ".join(filter_predicates)
# print(filter_predicates_s)

# COMMAND ----------

for tenant in tenant_prefixes:
  print(tenant)

# COMMAND ----------

spark.sql(
f"""
create or replace view {cpe_schema}.column_lineage_by_tenant as 
select * from system.access.column_lineage
where 
{filter_predicates_s}
"""
)

# COMMAND ----------

spark.sql(
f"""
create or replace view {cpe_schema}.table_lineage_by_tenant as 
select * from system.access.table_lineage
where 
{filter_predicates_s}
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compute Tables

# COMMAND ----------

predicates = []
for tenant in tenant_prefixes:
  # KP TODO - alter this to to match your tagging pattern
  predicates.append(f'(is_account_group_member("{tenant}_data_owner") and custom_tags["kp_project_tag"] == "{tenant}")')
predicates

# COMMAND ----------

predicates_s = "\n OR ".join(predicates)

# COMMAND ----------

spark.sql(
f"""
create or replace view {cpe_schema}.billing_usage_by_tenant as
select * from system.billing.usage
where
{predicates_s}
"""
)

# COMMAND ----------

predicates = []
for tenant in tenant_prefixes:
  # KP TODO - alter this to to match your tagging pattern
  predicates.append(f'(is_account_group_member("{tenant}_data_owner") and tags["kp_project_tag"] == "{tenant}")')
predicates_s = "\n OR ".join(predicates)

# COMMAND ----------

spark.sql(
f"""
create or replace view {cpe_schema}.clusters_by_tenant as
select * from system.compute.clusters
where
{predicates_s}
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Warehouse_events table filter is TBD
# MAGIC Most of what people want to know is what does each thing cost - you would get that from them billing view.
# MAGIC
# MAGIC You're not charged per-query in dbsql - you'd just know basic info about the warehouse itself
# MAGIC
# MAGIC the info in this table is more about the up time and scaling of the db sql warehouses
# MAGIC
# MAGIC The events that come through are:
# MAGIC  * STARTING
# MAGIC  * RUNNING
# MAGIC  * SCALED_UP
# MAGIC  * SCALED_DOWN
# MAGIC  * STOPPING
# MAGIC  * STOPPED
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe system.compute.warehouse_events

# COMMAND ----------

# MAGIC %md
# MAGIC * system.access.audit - filter on identity
# MAGIC * system.access.column_lineage - filter on catalog
# MAGIC * system.access.table_lineage - filter on catalog
# MAGIC * system.billing.list_prices - no filter
# MAGIC * system.billing.usage - by cluster tag
# MAGIC * system.compute.clusters - by cluster tag
# MAGIC * system.compute.node_types - by cluster tag
# MAGIC * system.compute.warehouse_events - by cluster tag
# MAGIC * system.operational_data.audit_logs - N/A
# MAGIC * system.operational_data.billing_logs - N/A
