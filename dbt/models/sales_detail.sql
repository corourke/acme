-- Prepare scans with store location and item category for sales analysis
WITH stores as (
  SELECT store_id, city, state, timezone 
  FROM acme_retail_bronze.retail_stores_rt
),

final as (
  SELECT 
    scans.scan_id,
    CAST(parse_datetime(scans.scan_datetime, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP(6)) AS scan_datetime,
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
  FROM acme_retail_bronze.scans_rt AS scans
  INNER JOIN stores on scans.store_id = stores.store_id
  INNER JOIN {{ref('items_categories')}} ON (scans.item_upc = items_categories.item_upc)
) 

SELECT * FROM final