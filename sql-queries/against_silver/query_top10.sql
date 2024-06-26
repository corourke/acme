-- Top 10 sales categories
SELECT
  year_month,
  category_name,
  SUM(net_units) as net_units,
  SUM(net_sales) as net_sales
FROM
  acme_retail_silver.sales_by_month
GROUP BY
  year_month,
  category_name
ORDER BY
  year_month,
  net_sales DESC,
  category_name
LIMIT
  10