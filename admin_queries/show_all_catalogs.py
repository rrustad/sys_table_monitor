# Databricks notebook source
# Target schema for CPE to publish table
dbutils.widgets.text('target_schema', 'cpe.sys_table')
target_schema = dbutils.widgets.get('target_schema')

# COMMAND ----------

spark.sql(f"""
create or replace view {target_schema}.catalogs as
select * from system.information_schema.catalogs
          """)
