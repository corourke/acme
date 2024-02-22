Random price updater

### Functions

1. Reads needed configuration parameters from a properties file -- parameters include: database host, database port, database user, database name, user password, update rate.
2. Connects to a Postgres database
3. Reads in one random category_code from item_categories.
4. Selects a random number of item_master table rows where the category_code matches the previously selected category_code, updates the item_price column in the item_master table by multiplying the existing value by a random value between .9 and 1.1.
5. Sleeps for a random period of time between 30 and 90 minutes. Does not make updates at night.
6. Listens for an OS interrupt to release resources and shutdown gracefully.
