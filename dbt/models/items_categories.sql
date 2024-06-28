{{
  config(
    materialized='incremental',
    file_format='hudi',
    incremental_strategy='merge',
    unique_key='item_id',
    location_root='s3a://adhoc-938cf009/andy_testing/dbt_test/',
    options={
      'type': 'mor',
      'primaryKey': 'item_id',
      'precombineKey': '_hoodie_commit_time'
    }
  )
}}

-- Flatten the item_master and item_categories
WITH items AS (
    SELECT category_code, item_id, item_upc, repl_qty, item_price
    FROM acme_demo.public_item_master_rt
),

categories AS (
    SELECT category_code, category_name, category_description 
    FROM acme_demo.public_item_categories_rt
),

final AS (
    SELECT  item_id, item_upc, items.category_code, category_name, item_price
    FROM items
    INNER JOIN categories ON (items.category_code = categories.category_code)
)

SELECT * FROM final
