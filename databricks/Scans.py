# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT batch_store_id, batch_scan_datetime, batch_item_upc, batch_unit_qty, batch_scan_id 
# MAGIC from delta.`s3a://onehouse-customer-bucket-9739baaf/datalake/acme_datalake_default/batched_scans`

# COMMAND ----------

df = spark.read.format("delta").load("s3a://onehouse-customer-bucket-9739baaf/datalake/acme_datalake_default/batched_scans")
df.createOrReplaceTempView("scans")
df = spark.read.format("delta").load("s3a://onehouse-customer-bucket-9739baaf/datalake/acme_datalake_default/retail_stores2")
df.createOrReplaceTempView("retail_stores")

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT batch_store_id, batch_scan_datetime, batch_item_upc, batch_unit_qty, batch_scan_id 
# MAGIC from scans

# COMMAND ----------

# MAGIC %sql
# MAGIC select store_id, city, state, timezone FROM retail_stores

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT timezone, count(*)
# MAGIC from scans
# MAGIC INNER JOIN retail_stores ON scans.batch_store_id = retail_stores.store_id
# MAGIC GROUP BY timezone

# COMMAND ----------


