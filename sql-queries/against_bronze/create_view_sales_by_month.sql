CREATE OR REPLACE VIEW acme_retail_silver.sales_by_month AS (
  SELECT
    EXTRACT( YEAR FROM scan_datetime ) YEAR,
    EXTRACT( MONTH FROM scan_datetime ) MONTH,
    DATE_TRUNC ('month', scan_datetime) AS year_month,
    item_upc,
    category_code,
    category_name,
    store_id,
    city,
    "state",
    region,
    SUM(unit_qty) net_units,
    SUM(net_sale) AS net_sales
  FROM acme_retail_silver.sales_detail
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
);