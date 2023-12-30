\copy item_categories from 'item_categories.csv' DELIMITER ',' CSV Header;

\copy item_master from 'item_master.csv' DELIMITER ',' CSV Header;

\copy retail_stores  from 'retail_stores.csv' DELIMITER ',' CSV Header;