# Databricks notebook source
tables = spark.sql("show tables in master_workspace.internal_tpch").select('tableName').collect()
dfs = {}
for table in tables:
    table_name = table['tableName']
    dfs[table_name] = spark.read.format("delta").table(f"master_workspace.internal_tpch.{table_name}")

# COMMAND ----------

display(dfs['orders'])

# COMMAND ----------

display(dfs['lineitem'])
