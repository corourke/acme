-- Inventory tables -- these will actually live in Hudi tables, but supplying 
-- them here for testing and reference. 

CREATE TABLE inventory_counts (
    store_id INTEGER NOT NULL,          -- FK to stores table
    item_id INTEGER NOT NULL,           -- FK to item_master table
    qty_in_stock INTEGER NOT NULL,      -- Quantity in stock
    qty_on_order INTEGER NOT NULL,      -- Quantity on order
    inv_updated DATE NOT NULL           -- Date of the inventory count
);

CREATE TABLE inventory_trx (
    trx_id VARCHAR(48) PRIMARY KEY,     -- UUID
    trx_type VARCHAR(1) NOT NULL,       -- Type of transaction (e.g., 'Restock', 'Sale')
    inv_date DATE NOT NULL,             -- Date of the transaction
    store_id INTEGER NOT NULL,          -- FK to stores table
    item_id INTEGER NOT NULL,           -- FK to item_master table
    unit_qty INTEGER NOT NULL,          -- Quantity of units involved in the transaction
    trx_timestamp TIMESTAMP NOT NULL    -- Timestamp of the transaction
);

-- Add foreign key constraints to inventory_counts table
ALTER TABLE inventory_counts
ADD CONSTRAINT fk_inventory_counts_store
FOREIGN KEY (store_id) REFERENCES stores(store_id);

ALTER TABLE inventory_counts
ADD CONSTRAINT fk_inventory_counts_item
FOREIGN KEY (item_id) REFERENCES items(item_id);

-- Add foreign key constraints to inventory_trx table
ALTER TABLE inventory_trx
ADD CONSTRAINT fk_inventory_trx_store
FOREIGN KEY (store_id) REFERENCES stores(store_id);

ALTER TABLE inventory_trx
ADD CONSTRAINT fk_inventory_trx_item
FOREIGN KEY (item_id) REFERENCES items(item_id);


