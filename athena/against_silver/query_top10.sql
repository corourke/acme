-- Top 10 sales categories
SELECT year_month, category_name, SUM(sum_qty) as total_qty, SUM(sum_amount) as total_amount
FROM acme_retail_silver.sales_by_month
GROUP BY year_month, category_name
ORDER BY year_month, total_amount DESC, category_name
LIMIT 10