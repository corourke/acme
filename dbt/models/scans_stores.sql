-- Expand store to location
/* 
Regarding CAST, see:
Incorrect timestamp precision in UNLOAD and CTAS queries for Iceberg tables
https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html#engine-versions-reference-0003-timestamp-changes:~:text=Incorrect%20timestamp%20precision%20in%20UNLOAD%20and%20CTAS%20queries%20for%20Iceberg%20tables 
*/
with scans as (
  SELECT 
    batch_scan_id as scan_id, 
    batch_store_id as store_id, 
    CAST(date_parse(batch_scan_datetime, '%Y-%m-%d %H:%i:%s') AS timestamp(6)) as scan_datetime, 
    batch_item_upc as item_upc, 
    batch_unit_price as unit_price, 
    batch_unit_qty as unit_qty
  FROM acme_retail_bronze.batched_scans_rt
),

stores as (
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
  FROM scans
  INNER JOIN stores on scans.store_id = stores.store_id
) 

SELECT * FROM final