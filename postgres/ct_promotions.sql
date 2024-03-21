-- Set up Promotions table (from random selection)

SET SEARCH_PATH TO retail;

-- TABLE: retail.promotions
DROP TABLE IF EXISTS retail.promotions;

CREATE TABLE IF NOT EXISTS retail.promotions
(
    id serial NOT NULL,
    region character varying(32) COLLATE pg_catalog."default",
    state character varying(2) COLLATE pg_catalog."default",
    item_id integer,
    category_code integer,
    old_price numeric(10,2),
    new_price numeric(10,2),
    start_date timestamp with time zone,
    end_date timestamp with time zone,
    CONSTRAINT promotions_pk PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS retail.promotions
    OWNER to cdc_user;

GRANT ALL ON TABLE retail.promotions TO cdc_user;


WITH targets AS (
  SELECT 
    ROW_NUMBER() OVER (ORDER BY RANDOM()) as row_num1, 
    item_id, 
    item_master.category_code,
    item_master.item_price old_price,
    (item_master.item_price * (1 - (RANDOM()*100)) ) new_price,
    NOW() AS start_date 
    FROM item_master
    LIMIT 150
),
regions AS (
SELECT 
    ROW_NUMBER() OVER (ORDER BY RANDOM()) as row_num2,
    retail_stores.timezone as region,
    retail_stores.state
    FROM retail_stores
    ORDER BY RANDOM() LIMIT 100
)

INSERT INTO retail.promotions ( region, state, item_id, category_code, old_price, new_price, start_date )
SELECT regions.region, regions.state, targets.item_id, targets.category_code, targets.old_price, targets.new_price, targets.start_date  
FROM targets
JOIN regions ON targets.row_num1 = regions.row_num2;