-- Promotions table (random selection)
CREATE TABLE promotions AS (
WITH targets AS (
  SELECT 
    ROW_NUMBER() OVER (ORDER BY RANDOM()) as row_num1, 
    item_id, 
    item_master.category_code,
    item_master.item_price old_price,
    (item_master.item_price * (1 - (RANDOM()*100)) ) new_price,
    NOW() AS start_date 
    FROM retail_item_master_rt AS item_master
    LIMIT 150
),
regions AS (
SELECT 
    ROW_NUMBER() OVER (ORDER BY RANDOM()) as row_num2,
    retail_stores.timezone as region,
    retail_stores.state
    FROM retail_stores AS retail_stores
    ORDER BY RANDOM() LIMIT 100
)


SELECT regions.*, targets.*  
FROM targets
JOIN regions ON targets.row_num1 = regions.row_num2
);