## Random price updater

Simulates price fluctuations and the addition of new items within a PostgreSQL database (specifically the `item_master` table defined elsewhere in this project).

The purpose is to simulate real-time price changes as a large retailer might do around the holidays. The new item generation is meant to showcase new records alongside updated records in the datalake.

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
