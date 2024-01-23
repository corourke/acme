-- Roll up sales by month, category, store
SELECT 
    EXTRACT(YEAR from scan_datetime) year, 
    EXTRACT(MONTH from scan_datetime) month,
    DATE_TRUNC('month', scan_datetime) as year_month,
    category_code, category_name, scans_stores.store_id, city, state, timezone,
    SUM(unit_qty) sum_qty, 
    CAST(avg(item_price) as decimal(10,2)) avg_price, 
    CAST(sum(unit_qty * item_price) as decimal(10,2)) sum_amount
FROM {{ref('scans_stores')}}
INNER JOIN {{ref('items_categories')}} ON (scans_stores.item_upc = items_categories.item_upc)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9