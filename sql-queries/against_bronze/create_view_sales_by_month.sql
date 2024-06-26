CREATE
OR REPLACE VIEW "sales_by_month" AS (
  SELECT
    DATE_TRUNC (
      'month',
      CAST(
        parse_datetime (scans.scan_datetime, 'yyyy-MM-dd HH:mm:ss') AS date
      )
    ) year_month,
    scans.item_upc,
    stores.city,
    stores.state,
    stores.timezone region,
    categories.category_code,
    categories.category_name,
    SUM(unit_qty) net_units,
    CAST(
      SUM(unit_qty * items.item_price) AS decimal(10, 2)
    ) net_sales
    --CAST(SUM(unit_qty * unit_price)AS decimal(10,2)) net_sales
  FROM
    acme_retail_bronze.retail_scans_rt scans
    INNER JOIN acme_retail_bronze.retail_stores_ro stores ON scans.store_id = stores.store_id
    INNER JOIN acme_retail_bronze.retail_item_master_rt items ON scans.item_upc = items.item_upc
    INNER JOIN acme_retail_bronze.retail_item_categories_ro categories ON items.category_code = categories.category_code
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7
)