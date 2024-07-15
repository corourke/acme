-- Set up Promotions table (from random selection)

SET SEARCH_PATH TO retail;

-- TABLE: retail.promotions
DROP TABLE IF EXISTS retail.promotions;

CREATE TABLE IF NOT EXISTS retail.promotions
(
    promotion_id serial PRIMARY KEY,
    promotion_name varchar(100),
    region varchar(32), -- If NULL, applies to all regions
    item_id integer NOT NULL,    
    start_date date NOT NULL,
    end_date date NOT NULL, 
    new_price numeric(10,2), -- if NULL use the discount_pct
    discount_pct integer
)

ALTER TABLE promotions
ADD CONSTRAINT fk_promotions_item_id FOREIGN KEY (item_id) REFERENCES item_master(item_id);

ALTER TABLE IF EXISTS retail.promotions
    OWNER to cdc_user;

GRANT ALL ON TABLE retail.promotions TO cdc_user;


-- create a few sample records
INSERT INTO retail.promotions ( item_id, region, discount_pct, start_date, end_date )
SELECT
    item_id,
    s.region,
    floor(POWER(RANDOM(),3)*(20-5)+5) discount_pct,
    NOW() AS start_date,
    NOW() + INTERVAL '7 days' AS end_date
  FROM item_master
  TABLESAMPLE bernoulli (1)
  CROSS JOIN LATERAL (
    SELECT timezone as region
    FROM stores
    TABLESAMPLE bernoulli (1)
    LIMIT 1
  ) s;

