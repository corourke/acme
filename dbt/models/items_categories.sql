-- Expand category code to name and description
WITH items AS (
    SELECT category_code, item_id, item_price, item_upc, repl_qty 
    FROM acme_retail_bronze.public_item_master_rt
),

categories AS (
    SELECT category_code, category_name, category_description 
    FROM acme_retail_bronze.public_item_categories_rt
),

final AS (
    SELECT  item_id, item_price, item_upc, items.category_code, category_name
    FROM items
    INNER JOIN categories ON (items.category_code = categories.category_code)
)

SELECT * FROM final
