### Requirements

1. Reads needed configuration parameters from a properties file -- parameters may include: database host, database port, database user, database name, user password, update rate.
2. Connects to a Postgres database
3. Reads in one category_code from a table of item_categories using a SQL statement that employs a 'ORDER BY RANDOM() LIMIT 1' clause to effect the random selection.
4. For a random number between 90 and 300 of item_master table rows where the category_code matches the previously selected category_code, update the item_price column in the item_master table by multiplying the existing value by a random value between .9 and 1.1.
5. Sleep for a random period of time between 30 and 90 minutes. Do not make updates at night between the hours of 6pm and 9am local time.
6. Listen for an OS interrupt and release resources and shutdown gracefully.
