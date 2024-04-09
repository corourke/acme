-- Adds store location information to each scan
WITH stores as (
  SELECT store_id, city, state, timezone 
  FROM acme_retail_bronze.retail_retail_stores_rt
),

final as (
  SELECT 
    scans.scan_id,
    scans.scan_datetime,
    scans.item_upc,
    scans.unit_price,
    scans.unit_qty, 
    scans.store_id,
    stores.city,
    stores.state,
    stores.timezone as region
  FROM scans_rt AS scans
  INNER JOIN stores on scans.store_id = stores.store_id
) 

SELECT * FROM final