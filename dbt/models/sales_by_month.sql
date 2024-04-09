-- Roll up sales by month, location, category

SELECT 
    EXTRACT(YEAR from scan_datetime) year, 
    EXTRACT(MONTH from scan_datetime) month,
    DATE_TRUNC('month', scan_datetime) as year_month,
    region,
    city, 
    "state", 
    store_id, 
    category_code, 
    category_name, 
    sum(unit_qty) net_units,
    sum(net_sale) as net_sales
FROM {{ref('sales_detail')}}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9
