-- Databricks notebook source
-- MAGIC %python
-- MAGIC df_item_master = spark.read.format("delta").load("s3://onehouse-customer-bucket-9739baaf/datalake/acme_retail_bronze/public_item_master/")
-- MAGIC df_item_master.createOrReplaceTempView("item_master")
-- MAGIC df_retail_stores = spark.read.format("delta").load("s3://onehouse-customer-bucket-9739baaf/datalake/acme_retail_bronze/retail_stores/")
-- MAGIC df_retail_stores.createOrReplaceTempView("retail_stores")
-- MAGIC df_item_categories = spark.read.format("delta").load("s3://onehouse-customer-bucket-9739baaf/datalake/acme_retail_bronze/public_item_categories/")
-- MAGIC df_item_categories.createOrReplaceTempView("item_categories")
-- MAGIC df_batched_scans = spark.read.format("delta").load("s3://onehouse-customer-bucket-9739baaf/datalake/acme_retail_bronze/batched_scans/")
-- MAGIC df_batched_scans.createOrReplaceTempView("batched_scans")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC # Perform joins and aggregations
-- MAGIC result_df = (
-- MAGIC     df_batched_scans.join(df_retail_stores, df_batched_scans.batch_store_id == df_retail_stores.store_id, 'inner')
-- MAGIC     .join(df_item_master, df_batched_scans.batch_item_upc == df_item_master.item_upc, 'inner')
-- MAGIC     .join(df_item_categories, df_item_master.category_code == df_item_categories.category_code, 'inner')
-- MAGIC     .withColumn("year", F.year(F.from_unixtime(F.unix_timestamp("batch_scan_datetime", "yyyy-MM-dd HH:mm:ss"))))
-- MAGIC     .withColumn("month", F.month(F.from_unixtime(F.unix_timestamp("batch_scan_datetime", "yyyy-MM-dd HH:mm:ss"))))
-- MAGIC     .withColumn("year_month", F.trunc(F.from_unixtime(F.unix_timestamp("batch_scan_datetime", "yyyy-MM-dd HH:mm:ss")), "MM"))
-- MAGIC     .groupBy(
-- MAGIC         "year", "month", "year_month", 
-- MAGIC         df_item_categories["category_code"], "category_name", 
-- MAGIC         "store_id", "city", "state", "timezone"
-- MAGIC     )
-- MAGIC     .agg(
-- MAGIC         F.sum("batch_unit_qty").alias("sum_qty"),
-- MAGIC         F.avg("item_price").cast("decimal(10,2)").alias("avg_price"),
-- MAGIC         (F.sum("batch_unit_qty") * F.avg("item_price")).cast("decimal(10,2)").alias("sum_amount")
-- MAGIC     )
-- MAGIC )
-- MAGIC
-- MAGIC # Show the DataFrame
-- MAGIC result_df.show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC # Cast DecimalType columns to FloatType
-- MAGIC result_df = result_df.withColumn("avg_price", col("avg_price").cast("float"))
-- MAGIC result_df = result_df.withColumn("sum_amount", col("sum_amount").cast("float"))
-- MAGIC
-- MAGIC # Convert the result DataFrame to a Pandas DataFrame
-- MAGIC result_pd = result_df.toPandas()
-- MAGIC
-- MAGIC # Aggregate the data by 'category_name' and 'timezone', summing the 'sum_amount'
-- MAGIC agg_pd = result_pd.groupby(['category_name', 'timezone'])['sum_amount'].sum().reset_index()
-- MAGIC
-- MAGIC # Convert sum_amount to float
-- MAGIC agg_pd['sum_amount'] = agg_pd['sum_amount'].astype(float)
-- MAGIC
-- MAGIC # Create the bubble plot
-- MAGIC plt.figure(figsize=(12, 8))
-- MAGIC scatter = plt.scatter(agg_pd['timezone'], agg_pd['category_name'], s=agg_pd['sum_amount'], c=agg_pd['sum_amount'], cmap='viridis', alpha=0.5)
-- MAGIC
-- MAGIC # Add a color bar
-- MAGIC plt.colorbar(scatter, label='Total Amount')
-- MAGIC
-- MAGIC # Slant the timezone labels for better readability
-- MAGIC plt.xticks(rotation=45)
-- MAGIC
-- MAGIC plt.xlabel('Timezone')
-- MAGIC plt.ylabel('Category')
-- MAGIC plt.title('Bubble Plot of Total Amount by Category and Timezone')
-- MAGIC plt.show()

-- COMMAND ----------

SELECT 
    YEAR(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(batch_scan_datetime, 'yyyy-MM-dd HH:mm:ss')) AS TIMESTAMP)) AS year,
    MONTH(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(batch_scan_datetime, 'yyyy-MM-dd HH:mm:ss')) AS TIMESTAMP)) AS month,
    TRUNC(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(batch_scan_datetime, 'yyyy-MM-dd HH:mm:ss')) AS TIMESTAMP), 'MM') AS year_month,
    categories.category_code,
    categories.category_name,
    stores.store_id,
    stores.city,
    stores.state,
    stores.timezone,
    SUM(batch_unit_qty) AS sum_qty,
    CAST(AVG(item_master.item_price) AS DECIMAL(10,2)) AS avg_price,
    CAST(SUM(batch_unit_qty * item_master.item_price) AS DECIMAL(10,2)) AS sum_amount
FROM 
    batched_scans
INNER JOIN 
    retail_stores AS stores ON batch_store_id = stores.store_id
INNER JOIN 
    item_master ON batch_item_upc = item_master.item_upc
INNER JOIN 
    item_categories AS categories ON item_master.category_code = categories.category_code
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9;

