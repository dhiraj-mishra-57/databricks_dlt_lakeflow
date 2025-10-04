# Databricks notebook source
spark.sql("drop schema if exists master_workspace.internal_tpch cascade");

# COMMAND ----------

spark.sql("create database if not exists master_workspace.internal_tpch");

# COMMAND ----------

display(spark.sql("SHOW TABLES IN samples.tpch"));

# COMMAND ----------

tables = spark.sql("show tables in samples.tpch").select('tableName').collect()
display(tables)

# COMMAND ----------

for table in tables:
    table_name = table["tableName"]
    if table_name == '_sqldf':
        continue
    df = spark.sql(f"select * from samples.tpch.{table_name} limit 1000")
    df.write.format('delta').mode('overwrite').saveAsTable(f"master_workspace.internal_tpch.{table_name}")
