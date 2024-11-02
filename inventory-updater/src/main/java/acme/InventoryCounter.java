package acme;

import java.util.Properties;
// import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import acme.objects.InventoryTransaction;
import acme.objects.Product;
import acme.objects.Transaction;
import acme.objects.InventoryCount;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;

public class InventoryCounter implements Runnable {
    private KafkaConsumer<String, String> consumer;
    private boolean running = true;
    private long lastWriteTime = System.currentTimeMillis(); // Track the last write time
    private List<Product> products; // list of producst item_master for item_id lookup
    private List<InventoryTransaction> inventoryTransactions = new ArrayList<>(); // cache of trx
    private List<InventoryCount> inventoryCounts = new ArrayList<InventoryCount>(); // inventory counts
    private static final String TOPIC = "retail_scans";
    private static final int MAX_INV_TRX_ROWS = 5000; // Max # of rows in file
    private static final int MAX_WRITE_INTERVAL_SECONDS = 300; // Max time in seconds before forcing a write
    private static final int MIN_INV_LEVEL = 1; // Define the constant

    public InventoryCounter(Properties props, List<Product> products) {

        // Initialize Kafka configuration
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", props.getProperty("bootstrap.servers"));
        kafkaProps.put("security.protocol", props.getProperty("security.protocol"));
        kafkaProps.put("sasl.mechanism", props.getProperty("sasl.mechanism"));
        kafkaProps.put("sasl.jaas.config", props.getProperty("sasl.jaas.config"));
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "inventory-counter-group");
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        this.consumer = new KafkaConsumer<>(kafkaProps);
        this.consumer.subscribe(Collections.singletonList(TOPIC));
        this.products = products;
    }

    @Override
    public void run() {
        running = true;
        int messageCount = 0; // Initialize a counter for consumed messages
        Transaction scan;
        InventoryTransaction inventoryTransaction;
        Random random = new Random(); // Initialize Random instance

        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    // System.out.printf("Consumed message: key = %s, value = %s, offset = %d%n",
                    // record.key(), record.value(), record.offset()); // TODO: remove this

                    // Parse JSON and create Transaction object
                    try {
                        scan = Transaction.fromJson(record.value());
                        // System.out.printf("Converted transaction: %s\n", scan.toJSON(null));

                        messageCount++; // Increment the counter for each consumed message
                    } catch (Exception e) {
                        System.err.printf("Failed to parse record value: %s%n", record.value());
                        e.printStackTrace();
                        running = false;
                        break;
                    }

                    // Lookup the item_id from the item_upc in the products list
                    final Transaction currentScan = scan;
                    Product product = products.stream()
                            .filter(p -> p.getItemUPC().equals(currentScan.getItemUPC()))
                            .findFirst().orElse(null);
                    if (product == null) {
                        System.err.printf("\nProduct not found for UPC: %s%n", scan.getItemUPC());
                        running = false; // TODO: remove this as we want to keep processing
                        continue;
                    }

                    // Check to see if the item_id/store_id combination has an InventoryCount
                    // If there is no InventoryCount, create one
                    InventoryCount inventoryCount = inventoryCounts.stream()
                            .filter(ic -> ic.getItemId() == product.getItemId()
                                    && ic.getStoreId() == currentScan.getStoreId())
                            .findFirst()
                            .orElse(null);

                    if (inventoryCount == null) {
                        // Create new inventory count if none exists
                        // InventoryCount(int itemId, int storeId, int qty_in_stock, int qty_on_order)
                        inventoryCount = new InventoryCount(
                                product.getItemId(),
                                scan.getStoreId(),
                                (product.getRepl_qty() == 1 ? 6 : product.getRepl_qty()), // initial count logic
                                0 // initial on_order is 0
                        );
                        inventoryCounts.add(inventoryCount);
                    }
                    // System.out.printf("inventory count: %s\n", inventoryCount.toJson(null));

                    // Handle product on order
                    // If the product qty_on_order is greater than 0, then pick a random number
                    // between 1 and 4
                    if (inventoryCount.getQtyOnOrder() > 0) {
                        int randomNumber = random.nextInt(4) + 1; // Generate a random number between 1 and 4
                        // If the the number is 1 or 2 do nothing. If the number is 3 or 4 create a
                        // Restock.
                        if (randomNumber == 3 || randomNumber == 4) {
                            int restockQty = (randomNumber == 3) ? product.getRepl_qty()
                                    : inventoryCount.getQtyOnOrder();
                            if (inventoryCount.getQtyInStock() < 0) {
                                restockQty += (inventoryCount.getQtyInStock() * -1);
                            }
                            // Create a Restock InventoryTransaction
                            // System.out.printf("Restock: %d\n", restockQty);
                            InventoryTransaction restockTransaction = new InventoryTransaction(
                                    "Restock", // trxType
                                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), // invDateString
                                    scan.getStoreId(), // storeId
                                    product.getItemId(), // itemId
                                    restockQty // unitQty
                            );
                            // Increment the product qty_in_stock by the quantity of the Restock
                            // InventoryTransaction
                            inventoryCount.setQtyInStock(inventoryCount.getQtyInStock() + restockQty);
                            // Decrement the product qty_on_order by the quantity of the Restock
                            // InventoryTransaction
                            inventoryCount.setQtyOnOrder(inventoryCount.getQtyOnOrder() - restockQty);
                            // Add the Restock InventoryTransaction to the inventoryTransactions list
                            inventoryTransactions.add(restockTransaction);
                        }
                    }

                    // Create a SalesInventoryTransaction object and decrement the qty_in_stock
                    inventoryTransaction = new InventoryTransaction(
                            "Sale", // trxType
                            scan.getScanDatetime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), // invDateString
                            scan.getStoreId(), // storeId
                            product.getItemId(), // itemId
                            scan.getUnitQty() // unitQty
                    );
                    inventoryTransactions.add(inventoryTransaction);

                    // Decrement the scan.unitQty from the product's qty_in_stock
                    inventoryCount.setQtyInStock(inventoryCount.getQtyInStock() - scan.getUnitQty());

                    // If the qty_in_stock is less than repl_qty, create an Order
                    // Inventory Transaction then increment the qty_on_order
                    if (inventoryCount.getQtyInStock() < product.getRepl_qty()) {
                        inventoryTransaction = new InventoryTransaction(
                                "Order", // trxType
                                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), // invDateString
                                scan.getStoreId(), // storeId
                                product.getItemId(), // itemId
                                product.getRepl_qty() // unitQty
                        );
                        inventoryTransactions.add(inventoryTransaction);
                        inventoryCount
                                .setQtyOnOrder(inventoryCount.getQtyOnOrder() + inventoryTransaction.getUnitQty());
                    }

                    // If the qty_in_stock is less than 1, its a stockout, create a Transfer
                    // Inventory Transaction
                    if (inventoryCount.getQtyInStock() < MIN_INV_LEVEL) {
                        int transferQty = MIN_INV_LEVEL - inventoryCount.getQtyInStock();
                        System.out.printf("TFR inventory count: %s\n", transferQty);
                        // Create a Transfer Inventory Transaction, incrementing the QtyInStock by the
                        // quantity below the minimum threshold
                        inventoryTransaction = new InventoryTransaction(
                                "Transfer", // trxType
                                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), // invDateString
                                scan.getStoreId(), // storeId
                                product.getItemId(), // itemId
                                transferQty // unitQty
                        );
                        inventoryTransactions.add(inventoryTransaction);
                        inventoryCount
                                .setQtyInStock(inventoryCount.getQtyInStock() + inventoryTransaction.getUnitQty());
                    }

                    // When cache grows or time elapses, write to a file
                    long currentTime = System.currentTimeMillis();
                    long elapsedTime = (currentTime - lastWriteTime) / 1000; // Convert to seconds

                    if (inventoryTransactions.size() >= MAX_INV_TRX_ROWS || elapsedTime >= MAX_WRITE_INTERVAL_SECONDS) {
                        writeInventoryTransactionsToFile();
                        lastWriteTime = currentTime; // Update the last write time
                    }
                } // end of for loop
                  // Commit offsets after processing the batch
                consumer.commitSync();
                // Update the count on the sameline
                System.out.printf("\rConsumed scan messages count: %d, inventoryCount: %d",
                        messageCount,
                        inventoryCounts.size());
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            running = false;
        } finally {
            consumer.close();
            System.out.println("Shutdown InventoryCounter.");
        }

    }

    private void writeInventoryTransactionsToFile() {
        // Create a file name based on the current date and time
        String fileName = "inventory_transactions_"
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddhhmmss")) + ".json";
        // Write the inventoryTransactions to the file
        try (FileWriter fileWriter = new FileWriter(fileName)) {
            for (InventoryTransaction transaction : inventoryTransactions) {
                fileWriter.write(transaction.toJson(Boolean.FALSE) + "\n");
            }
        } catch (IOException e) {
            System.err.println("Error writing inventory transactions to file: " + e.getMessage());
        }
        // Clear the list after writing to file
        inventoryTransactions.clear();
    }
}