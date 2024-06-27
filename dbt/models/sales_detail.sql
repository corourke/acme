{{
  config(
    materialized='incremental',
    file_format='hudi',
    incremental_strategy='merge',
    unique_key='scan_id',
    location_root='s3a://adhoc-938cf009/andy_testing/dbt_test/',
    partition_by='state',
    options={
      'type': 'mor',
      'primaryKey': 'scan_id',
      'precombineKey': '_hoodie_commit_time'
    }
  )
}}

-- Prepare scans with store location and item category for sales analysis
WITH stores as (
  SELECT store_id, city, state, timezone 
  FROM acme_demo.retail_stores_rt
),

final as (
  SELECT 
    scans.scan_id,
    CAST(to_timestamp(scans.scan_datetime, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) AS scan_datetime,
    item_id,
    scans.item_upc,
    scans.unit_qty,
    --scans.unit_price,
    CAST(items.item_price AS DECIMAL(10, 2)) AS unit_price,
    --CAST((unit_qty * unit_price) as decimal(10,2)) AS net_sale,
    CAST((unit_qty * items.item_price) AS DECIMAL(10, 2)) AS net_sale,
    category_code,
    category_name,
    scans.store_id,
    stores.city,
    stores.state,
    stores.timezone as region
  FROM acme_demo.scans_rt AS scans
  INNER JOIN stores on scans.store_id = stores.store_id
  INNER JOIN {{ref('items_categories')}} ON (scans.item_upc = items_categories.item_upc)
) 

SELECT * FROM final