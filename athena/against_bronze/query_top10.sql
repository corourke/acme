WITH combined_data AS (
  SELECT 
    CAST(date_parse(scans.batch_scan_datetime, '%Y-%m-%d %H:%i:%s') AS timestamp(6)) as scan_datetime, 
    scans.batch_item_upc as item_upc,
    scans.batch_unit_qty as unit_qty,
    stores.city,
    stores.state,
    stores.timezone,
    items.category_code,
    categories.category_name,
    items.item_price
  FROM acme_retail_bronze.batched_scans3_rt scans
  INNER JOIN acme_retail_bronze.retail_retail_stores_rt stores ON scans.batch_store_id = stores.store_id
  INNER JOIN acme_retail_bronze.retail_item_master_rt items ON scans.batch_item_upc = items.item_upc
  INNER JOIN acme_retail_bronze.retail_item_categories_rt categories ON items.category_code = categories.category_code
),

sales_by_month AS (
  SELECT 
    DATE_TRUNC('month', scan_datetime) as year_month,
    category_code,
    category_name,
    timezone as region,
    SUM(unit_qty) as sum_qty, 
    CAST(AVG(item_price) as decimal(10,2)) as avg_price, 
    CAST(SUM(unit_qty * item_price) as decimal(10,2)) as sum_amount
  FROM combined_data
  GROUP BY 1, 2, 3, 4
)

-- Top 10 sales categories
SELECT 
  category_code, 
  category_name, 
  SUM(sum_qty) as total_qty, 
  SUM(sum_amount) as total_amount
FROM sales_by_month
GROUP BY category_code, category_name
ORDER BY total_amount DESC
LIMIT 10