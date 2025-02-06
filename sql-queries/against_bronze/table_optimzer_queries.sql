-- Query 1: Find the Total Sales for a Given Store Over the Holiday Period
SELECT store_id, SUM(price * quantity) AS total_sales
FROM scans_raw
WHERE store_id = 100
  AND timestamp BETWEEN '2024-12-01' AND '2024-12-31';

-- Query 2: Get the Most Recent Scan for Each Store
SELECT store_id, MAX(timestamp) AS last_scan_time
FROM scans_raw
GROUP BY store_id;

-- Query 3: Aggregate Sales by Product Category (Joining with item_master)
SELECT ic.category_name, SUM(s.price * s.quantity) AS category_sales
FROM scans_raw s
JOIN item_master im ON s.item_id = im.item_id
JOIN item_categories ic ON im.category_id = ic.category_id
GROUP BY ic.category_name;
