-- Table 1: Categories
CREATE TABLE item_categories (
    category_code INTEGER PRIMARY KEY, 
    category_name VARCHAR(255), 
    category_description VARCHAR(255),
    ytd_sales INTEGER, 
    avg_price NUMERIC, 
    _frequency NUMERIC
);

-- Table 2: Item Master
CREATE TABLE item_master (
    category_code INTEGER, 
    item_id INTEGER PRIMARY KEY,
    item_price DECIMAL(10,2), 
    item_upc VARCHAR(15) UNIQUE, 
    repl_qty INTEGER,
    _frequency INTEGER
);
CREATE INDEX idx_item_upc ON item_master (item_upc);
ALTER TABLE IF EXISTS item_master
    ADD CONSTRAINT fk_item_category FOREIGN KEY (category_code)
    REFERENCES item_categories (category_code) MATCH SIMPLE
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
    NOT VALID;

-- Table 3: Stores
CREATE TABLE stores (
    store_id INTEGER PRIMARY KEY, 
    address VARCHAR(255),
    city VARCHAR (80),
    state VARCHAR (2),
    zipcode VARCHAR(10),
    longitude NUMERIC,
    latitude NUMERIC, 
    timezone VARCHAR(32),
    region VARCHAR(32)
);

-- Table 4: Point-of-sale Scans
-- This table is actually in the datalake, but showing it here for reference
CREATE TABLE scans (
    scan_id VARCHAR(48) PRIMARY KEY,
    store_id INTEGER,
    scan_datetime TIMESTAMP,
    item_upc VARCHAR(15),
    unit_qty INTEGER,
    unit_price DECIMAL(10,2)
);
ALTER TABLE IF EXISTS scans
    ADD CONSTRAINT fk_store_id FOREIGN KEY (store_id)
    REFERENCES stores (store_id) MATCH SIMPLE
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
    NOT VALID;
ALTER TABLE IF EXISTS scans
    ADD CONSTRAINT fk_item_upc FOREIGN KEY (item_upc)
    REFERENCES item_master (item_upc) MATCH SIMPLE
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
    NOT VALID;
