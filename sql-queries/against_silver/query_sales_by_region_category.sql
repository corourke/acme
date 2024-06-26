-- Sales by Region, Category for pivot table
SELECT
  region,
  category_name,
  SUM(net_units) as net_units,
  SUM(net_sales) as net_sales
FROM
  acme_retail_silver.sales_by_month
GROUP BY
  region,
  category_name
ORDER BY
  net_sales DESC