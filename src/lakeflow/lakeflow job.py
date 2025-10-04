# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### load bronze tables

# COMMAND ----------

@dp.table(name="bronze_customer")
def bronze_customer():
    return spark.readStream.table("master_workspace.internal_tpch.customer")

@dp.table(name="bronze_orders")
def bronze_orders():
    return spark.readStream.table("master_workspace.internal_tpch.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### load silver tables

# COMMAND ----------

@dp.table(name="silver_orders")
def silver_orders():
  return spark.readStream.table("bronze_orders")

@dp.table(name="silver_customer")
def silver_customer():
  return spark.readStream.table("bronze_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ### load gold tables

# COMMAND ----------

@dp.table(name="customer_orders")
def customer_orders():
    customer = spark.readStream.table("silver_customer")
    orders = spark.readStream.table("silver_orders")
    return (customer.
                     join(orders, orders.o_custkey == customer.c_custkey).
                     groupBy("orderdate").
                     agg(F.countDistinct("custKey").alias("unique_customers"))
                     )
