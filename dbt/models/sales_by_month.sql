{{
  config(
    materialized='incremental',
    file_format='hudi',
    incremental_strategy='merge',
    unique_key='store_id',
    location_root='s3a://adhoc-938cf009/andy_testing/dbt_test/',
    partition_by='state',
    options={
      'type': 'mor',
      'primaryKey': 'store_id',
      'precombineKey': '_hoodie_commit_time'
    }
  )
}}

-- Roll up sales by month, location, category

SELECT 
    EXTRACT(YEAR from scan_datetime) year, 
    EXTRACT(MONTH from scan_datetime) month,
    DATE_TRUNC('month', scan_datetime) as year_month,
    item_upc,
    category_code, 
    category_name,
    store_id, 
    city,
    "state",
    region, 
    sum(unit_qty) net_units,
    sum(net_sale) as net_sales
FROM {{ref('sales_detail')}}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
