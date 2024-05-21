## Random price updater

Simulates price fluctuations and the addition of new items, and removal of old items within a PostgreSQL database
(specifically the `item_master` table defined elsewhere in this project).

The purpose is to add live activity to the demo, and showcase Hudi table mutation handling, including clustering,
compaction, and cleaning, by inducing Postgres table inserts, updates and deletes.

The updates simulate the real-time price changes a large retailer might do around the holidays.

Items that are added or removed have an item_id greater than or equal to 50000 in order to preserve referential
integrity for the scan records.

### Program Logic

- Reads the database connection information from a properties file.
- Uses a shutdown hook to perform cleanup when the program is interrupted.
- Connects to the Postgres database, periodically checks the connection, and reconnects if it is disconnected.
- Transactions are only generated during business hours.
- Every 15 to 120 seconds:
  - Reads the item_categories table to select a random category to update.
  - Reads the item_master table to find the next item_id >=50000 to use for new items.
  - Reads the item_master table to find existing UPC codes to avoid generating duplicates.
  - The program writes to the item_master table to update prices, insert new items, and delete items.
    - Chooses a category_code at random, then randomly select between 20 and 60 items with that category code, and update the price by +/- 0-10% for those items.
    - Generates and inserts 5-20 new items with item_id > 50000 using the same category_code.
    - Deletes between 5 and 15 randomly selected items with item_id > 50000

### Setup

Create the config.properties file:

```
database.host=acme-db.cc64n2qunrtu.us-west-2.rds.amazonaws.com
database.port=5432
database.user=postgres
database.name=acme
database.password=<postgres_user_password>
```

Make the program with `./make.sh`
Start the process using `./start.sh`

### Functions

1. Reads needed configuration parameters from a properties file
2. Connects to a Postgres database
3. Reads in one random category_code from item_categories.
4. Randomly modifies the price for a subset of items within the chosen category. Prices can increase or decrease by up to 10%.
5. Randomly inserts a small number of new items. Ensures that the new item IDs and UPC codes are unique.
6. Sleeps for a random period of time between 30 and 90 minutes. Does not make updates at night.
7. Listens for an OS interrupt to release resources and shutdown gracefully.

### Potential Enhancements

- **Realistic UPCs:** More accurate UPCs, would include a check digit calculation and integration with GS1 standards.
- **Item Data:** Replace the placeholder data for new records with more realistic data.

* **Configurable:** It might be useful to make this program more configurable, to handle more varied
  demo data tasks as opposed to being a single-purpose program.
