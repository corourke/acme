-- Roll up sales by month, category, store
SELECT 
    EXTRACT(YEAR from scan_datetime) year, 
    EXTRACT(MONTH from scan_datetime) month,
    DATE_TRUNC('month', scan_datetime) as year_month,
    scans_stores.region,
    scans_stores.state, 
    scans_stores.city, 
    scans_stores.store_id, 
    category_code, 
    category_name, 
    sum(unit_qty) net_units,
    CAST(sum(unit_qty * unit_price) as decimal(10,2)) net_sales
FROM {{ref('scans_stores')}}
INNER JOIN {{ref('items_categories')}} ON (scans_stores.item_upc = items_categories.item_upc)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9
