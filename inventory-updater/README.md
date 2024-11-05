Start with:

```
mvn clean package;
java -Djava.util.logging.config.file=src/main/resources/logging.properties -jar inventory_updater-1.0-jar-with-dependencies.jar
```

## Inventory Updater

The InventoryUpdater is a Java application designed to create data files for use in the Acme demo. It reads configuration settings from a `config.properties` file, reads in lookup data from the PostgreSQL database, processes inventory transactions from the `retail_scans` Kafka topic, maintains in-memory stock and order quantities, and writes files to S3.

These data files are meant to populate Hudi tables outside of Onehouse for use with Table Optimizer and LakeView. It generates JSON files for two tables:

- InventoryTransactions -- Has inserts and a few deletes and is doing about 35,000 rows per minute. This should be set up as a Hudi MOR table.
- InventoryCounts -- Has inserts and upserts, with a new file being created every 5 minutes. This should be set up as a Hudi COW table.

The application is broken down into three main Classes:

InventoryUpdater

- Loads configuration settings from a config.properties file to establish database connections and other necessary parameters.
- Connects to the PostgreSQL database and reads the item_master table and loads the items into memory. This is used to map UPC codes to itemIDs.

InventoryCounter

- Initializes a Kafka consumer to subscribe to the retail_scans topic.
- Parses each scan into a Transaction object and looks up the itemID from the UPC code
- Maintains an in-memory quantity in stock and quantity on order for each product at a specific store
- Creates transactions for sales, restocks and orders. It updates the inventory counts accordingly.
- Periodically writes the inventory transactions and inventory counts to local files and then uploads them to an S3 bucket.
- Maintains a list of transactions marked for deletion and periodically creates transactions with the is_deleted flag set.

S3FileUploader

- Initializes an S3Client using AWS SDK for Java. It reads configuration properties such as the AWS region, bucket name, and folder path from config.properties. The client is configured to use credentials from the default AWS profile set up with the AWS CLI.
- Uploades files to an S3 bucket.
