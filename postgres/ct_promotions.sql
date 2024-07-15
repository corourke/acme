-- Set up Promotions table (from random selection)
SET
  SEARCH_PATH TO retail;

-- TABLE: retail.promotions
DROP TABLE IF EXISTS retail.promotions;

CREATE TABLE
  IF NOT EXISTS retail.promotions (
    promotion_id serial PRIMARY KEY,
    promotion_name VARCHAR(100),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    discount_type VARCHAR(10) NOT NULL, -- 'PCT_OFF', 'AMT_OFF', 'NEW_PRICE', 'BOGO'
    discount_value NUMERIC(10, 2) NOT NULL, -- percentage off, amount off, or new price
    region VARCHAR(32), -- If NULL, applies to all regions
    category_code INTEGER, -- If NULL, applies to all categories
    item_id INTEGER, -- if null, applies to all items
    conversions INTEGER DEFAULT 0
  );

ALTER TABLE promotions ADD CONSTRAINT fk_promotions_category_code FOREIGN KEY (category_code) REFERENCES item_categories (category_code);

ALTER TABLE IF EXISTS retail.promotions OWNER TO cdc_user;

GRANT ALL ON TABLE retail.promotions TO cdc_user;

-- create a few sample records
INSERT INTO
  retail.promotions (
    promotion_name,
    start_date,
    end_date,
    discount_type,
    discount_value,
    category_code
  )
SELECT
  CONCAT ('Save on ', category_name) AS promotion_name,
  NOW () AS start_date,
  NOW () + INTERVAL '7 days' AS end_date,
  'PCT_OFF' AS discount_type,
  FLOOR(POWER(RANDOM (), 3) * (10 -5) + 5) AS discount_value,
  category_code
FROM
  item_categories TABLESAMPLE bernoulli (30);