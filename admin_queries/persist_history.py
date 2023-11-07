# Databricks notebook source
from pyspark.sql.functions import col
import pyspark.sql.functions as f
import logging

# COMMAND ----------

# root = logging.getLogger()
# root.setLevel(logging.INFO)

# COMMAND ----------

# Target schema for CPE to publish table
dbutils.widgets.text('target_schema', 'cpe.sys_table')
target_schema = dbutils.widgets.get('target_schema')

# directory for checkpoints
dbutils.widgets.text('checkpoint_locations', 'dbfs:/tmp/riley.rustad@databricks.com/sys_tables/checkpoints')
checkpoint_locations = dbutils.widgets.get('checkpoint_locations')

dbutils.widgets.text('days_history', '90')
days_history = int(dbutils.widgets.get('days_history'))

# COMMAND ----------

def table_exitsts(table_path):
  catalog, schema, table = table_path.split('.')
  tables = [x['table_name'] for x in (spark.table(f'{catalog}.information_schema.tables')
    .filter(col('table_schema') == schema)
    .select('table_name')
    .collect()
    )]
  if table in tables:
    logging.info(f'Table {table_path} exists')
    return True
  else:
    logging.info(f"Table {table_path} doesn't exists")
    return False

# COMMAND ----------

# spark.sql(f'drop table {target_schema}.audit_history')

# COMMAND ----------

def update_immutable_hist_sys_table(target_schema, full_table_name, pk, ts, days_history=90):

  table_name = full_table_name.split('.')[-1]


  if not table_exitsts(f'{target_schema}.{table_name}_history'):

    df = spark.table(full_table_name)
    df.write.saveAsTable(f'{target_schema}.{table_name}_history')
    logging.info(f'logged entire table {full_table_name} into {target_schema}.{table_name}_history')

  else:
    max_hist_event_time = (
      spark.table(f'{target_schema}.{table_name}_history')
      .select(f.max(ts))
      .collect()[0][0]
      )
    
    df = (
      spark.table(full_table_name)
      # Get data from at least 1 day earlier than the latest update
      .filter(col(ts) > max_hist_event_time - f.expr('Interval 1 day'))
      )
    
    df.createOrReplaceTempView(f'{table_name}_updates')

    spark.sql(f"""
      MERGE INTO {target_schema}.{table_name}_history a USING {table_name}_updates u
    ON a.{pk} = u.{pk}
    WHEN NOT MATCHED THEN INSERT *
              """)
    logging.info(f'incrememntal update of {target_schema}.{table_name}_history finished')

    max_event_time = spark.table(f'{target_schema}.{table_name}_history').select(f.max(ts)).collect()[0][0]

    spark.sql(f"delete from {target_schema}.{table_name}_history where {ts} < to_timestamp('{max_event_time}') - Interval {days_history} days")
    logging.info(f'deleting history of {target_schema}.{table_name}_history up to {days_history} days ago')

# COMMAND ----------

update_immutable_hist_sys_table(target_schema, 'system.access.audit', 'event_id', 'event_time', days_history)

# COMMAND ----------

update_immutable_hist_sys_table(target_schema, 'system.billing.usage', 'record_id', 'usage_end_time', days_history)

# COMMAND ----------

#TODO: add composite pk
# update_immutable_hist_sys_table(target_schema, 'system.compute.clusters', 'record_id', 'usage_end_time', days_history)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history cpe.sys_table.audit_history

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cpe.sys_table.audit_history

# COMMAND ----------

col_lin = spark.table('system.access.column_lineage')

# COMMAND ----------

col_lin.count() == col_lin.select('entity_id','entity_run_id','event_time','created_by').distinct().count()

# COMMAND ----------

#TODO: someday in the future, they may enable change data feed
# audit = (
#   spark.readStream.format("delta")
#   .option("readChangeFeed", "true")
#   .table("system.access.audit")
#   .writeStream
#   .option("checkpointLocation", f"{checkpoint_locations}/audit")
#   .trigger(availableNow=True)
#   .toTable(f'{target_schema}.audit_history')
# )

# COMMAND ----------



# COMMAND ----------

spark.table('system.access.audit')

# COMMAND ----------

spark.table(f'{target_schema}.audit_history').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail system.access.audit

# COMMAND ----------

spark.sql(f'drop table {target_schema}.audit_history')

# COMMAND ----------


