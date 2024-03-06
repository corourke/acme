-- Hourly sales summary
SELECT 
    DATE_TRUNC('hour', CAST(parse_datetime(batch_scan_datetime, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)) AS sales_day_hour,
    retail_stores.timezone as region,
    retail_stores.state,
    retail_stores.store_id,
    item_master.category_code,
    item_categories.category_name,
    SUM(batch_unit_qty) net_units,
    SUM(batch_unit_qty * item_master.item_price) AS net_sales,
    'I' AS row_status, -- (D)aily total, (H)ourly total, (I)ntermediate total
    CURRENT_TIMESTAMP AS row_timestamp
FROM batched_scans_rt AS scans
INNER JOIN retail_retail_stores_ro AS retail_stores ON batch_store_id = store_id
INNER JOIN retail_item_master_rt AS item_master ON batch_item_upc = item_upc -- normally retail_item_master_rt
INNER JOIN retail_item_categories_ro AS item_categories ON item_master.category_code = item_categories.category_code
GROUP BY 1, 2, 3, 4, 5, 6, 9, 10
ORDER BY 1, 5, 6, 2, 3