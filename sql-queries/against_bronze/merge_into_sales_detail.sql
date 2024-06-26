MERGE INTO acme_retail_silver.sales_detail AS target USING (
  SELECT
    scans.scan_id,
    CAST(
      parse_datetime (scans.scan_datetime, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP(6)
    ) AS scan_datetime,
    scans.item_upc,
    --scans.unit_price,
    CAST(items.item_price AS DECIMAL(10, 2)) AS unit_price,
    scans.unit_qty,
    --CAST((unit_qty * unit_price) as decimal(10,2)) AS net_sale,
    CAST((unit_qty * items.item_price) AS DECIMAL(10, 2)) AS net_sale,
    categories.category_code,
    categories.category_name,
    stores.store_id,
    stores.city,
    stores.state,
    stores.timezone AS region
  FROM
    acme_retail_bronze.retail_scans_rt AS scans
    INNER JOIN acme_retail_bronze.retail_stores_ro AS stores ON scans.store_id = stores.store_id
    INNER JOIN acme_retail_bronze.retail_item_master_rt AS items ON scans.item_upc = items.item_upc
    INNER JOIN acme_retail_bronze.retail_item_categories_ro AS categories ON items.category_code = categories.category_code
) AS source ON target.scan_id = source.scan_id WHEN MATCHED THEN
UPDATE
SET
  scan_datetime = source.scan_datetime,
  item_upc = source.item_upc,
  unit_price = source.unit_price,
  unit_qty = source.unit_qty,
  net_sale = source.net_sale,
  category_code = source.category_code,
  category_name = source.category_name,
  store_id = source.store_id,
  city = source.city,
  state = source.state,
  region = source.region WHEN NOT MATCHED THEN INSERT (
    scan_id,
    scan_datetime,
    item_upc,
    unit_price,
    unit_qty,
    net_sale,
    category_code,
    category_name,
    store_id,
    city,
    state,
    region
  )
VALUES
  (
    source.scan_id,
    source.scan_datetime,
    source.item_upc,
    source.unit_price,
    source.unit_qty,
    source.net_sale,
    source.category_code,
    source.category_name,
    source.store_id,
    source.city,
    source.state,
    source.region
  );