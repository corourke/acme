Requirements for a data generator written in Java that simulates point-of-sale transactions and sends batches of those transactions to Kafka.

The purpose of this program is to generate simplated point-of-sale (checkout) transactions in large quantities for analytical processing. It follows my experience with a large retailer with hundreds of stores throughout the US. The data team needed to bring near real-time sales data together for immediate analysis around the holidays in a pricing 'war room' situation.

This list encapsulates the functional and operational requirements for the point-of-sale transaction data generator, aiming to ensure realistic simulation, efficient data handling, and proper integration with Kafka for data ingestion.

1. **Data Generation**:

   - The program will be written in Java.
   - Generate simulated point-of-sale transaction data including fields such as UPC codes, store number, date and time, quantity, price paid, globally unique batch number, and globally unique transaction number.
   - UPC codes are to be sourced from a list of products with fields for standard price (to be used as the basis for the transaction price) and frequency (indicating the relative frequency of sales for that product).
   - Store numbers are to be sourced from a list of stores. The stores table includes a US time zone for each store, also known as the region, which should inform the simulation of the time of day for transaction generation.
   - Generate transactions for a semi-random number of scans per store per hour,based on the time of day.
   - Use UUIDs for ensuring the global uniqueness of batch numbers and transaction numbers.

2. **Output Format**:

   - The output format is JSON. Each batch of transactions will be one JSON payload.
   - Transactions should be batched by region approximately every 1000 scans or after 10 minutes, whichever comes first.
   - Each batch of transactions will be first saved to the file system for safety, and then sent to a Kafka cluster.
   - The file names will include a timestamp, and the number of the region in which the transaction occurred.

3. **Startup & Shutdown**

- Read both the product database (with UPC codes) and the stores table into memory at runtime from CSV files.
- Kafka configuration should be stored in a YAML configuration file and read in during startup.
- Keys and other secrets should be read in via a safe mechanism.
- Listen for an OS shutdown signal and ensure threads terminate gracefully, sending any remaining transaction batches to Kafka before shutdown.

4. **Concurrency & Realism**:

   - Simulate all stores in the database simultaneously, with the potential to use threading, especially to differentiate by time zones within the United States.
   - Generate transactions in a realistic order, potentially by spinning up a thread for each time zone.
   - Adjust the transaction rate for each store based on predefined activity levels for different times of day, ensursuring that the rate reflects realistic store activity levels.
     - Introduce a structure to store average activity levels for each hour of the day.
     - Adjust the transaction generation rate (and sleep intervals) dynamically by comparing current transaction output rates against these benchmarks.
   - Ensure that any shared resources accessed by the threads (like counters or shared data structures) are properly synchronized to avoid concurrency issues.

5. **User Interface**:

   - The program will include a minimal UI for monitoring the data generation.
   - The UI will include:
     - By region:
       - current transaction output rate
       - cumulative transaction count
       - current time within time zone
     - Total cumulative data output volume in bytes, total cumulative transactions generated.
     - A button for shutting the program down.

6. **External Dependencies**:

   - A Kafka instance or cluster where the data will be sent.
   - Configuration file for the Kafka API.
   - The Confluent Kafka Java API.
   - Lists or databases of UPC codes and store numbers to enable relational joins during analysis.
   - A build tool to track dependencies.