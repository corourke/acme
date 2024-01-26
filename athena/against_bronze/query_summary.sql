SELECT 
    DATE_TRUNC('month', CAST(parse_datetime(batch_scan_datetime, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)) AS year_month,
    categories.category_code,
    categories.category_name,
    stores.store_id,
    stores.timezone as region,
    SUM(batch_unit_qty) AS sum_qty,
    CAST(AVG(items.item_price) AS DECIMAL(10,2)) AS avg_price,
    CAST(SUM(batch_unit_qty * items.item_price) AS DECIMAL(10,2)) AS sum_amount
FROM batched_scans3_rt
INNER JOIN 
    retail_retail_stores_rt AS stores ON batch_store_id = stores.store_id
INNER JOIN 
    retail_item_master_rt as items ON batch_item_upc = items.item_upc
INNER JOIN 
    retail_item_categories_rt AS categories ON items.category_code = categories.category_code
GROUP BY 1, 2, 3, 4, 5;