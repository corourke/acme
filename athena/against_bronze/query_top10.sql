-- Standalone top 10 query, also see the dbt version with intermediate tables
WITH sales_by_month AS (
SELECT 
    DATE_TRUNC('month', CAST(parse_datetime(scans.batch_scan_datetime, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)) AS year_month,
    scans.batch_item_upc as item_upc,
    scans.batch_unit_qty as unit_qty,
    retail_stores.state,
    retail_stores.timezone as region,
    item_master.category_code,
    item_categories.category_name,
    SUM(batch_unit_qty) net_units,
    SUM(batch_unit_qty * CAST(item_master.item_price AS DECIMAL(10,2))) AS net_sales
FROM batched_scans_rt AS scans
INNER JOIN retail_retail_stores_ro AS retail_stores ON batch_store_id = store_id
INNER JOIN retail_products_rt AS item_master ON batch_item_upc = item_upc -- normally retail_item_master_rt
INNER JOIN retail_item_categories_ro AS item_categories ON item_master.category_code = item_categories.category_code
GROUP BY 1, 2, 3, 4, 5, 6, 7
)

-- Doing this in two parts so that we can create different queries
-- Better to use silver tables if possible

-- Top 10 sales categories
SELECT 
  category_code, 
  category_name, 
  SUM(net_units) as net_units, 
  SUM(net_sales) as net_sales
FROM sales_by_month
GROUP BY category_code, category_name
ORDER BY net_sales DESC
LIMIT 10