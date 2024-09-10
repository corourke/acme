-- Prepare scans with store location and item category
CREATE TABLE
  acme_retail_silver.sales_detail (
    scan_id STRING,
    scan_datetime TIMESTAMP(6),
    item_upc STRING,
    unit_price DECIMAL(10, 2),
    unit_qty INT,
    net_sale DECIMAL(10, 2),
    category_code INT,
    category_name STRING,
    store_id INT,
    city STRING,
    state STRING,
    region STRING
  ) PARTITIONED BY (region) 
  LOCATION 's3://acme-retail-data/silver_tables/' 
  TBLPROPERTIES ('table_type' = 'iceberg')