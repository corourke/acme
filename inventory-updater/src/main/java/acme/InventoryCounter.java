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
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.ConcurrentHashMap;

public class InventoryCounter implements Runnable {
    private static final Logger logger = Logger.getLogger(InventoryCounter.class.getName());
    private KafkaConsumer<String, String> consumer;
    private S3FileUploader s3Uploader;
    private boolean running = true;
    private long lastWriteTime = System.currentTimeMillis(); // Track the last write time
    private long lastInventoryWriteTime = System.currentTimeMillis(); // Track the last inventory write time

    private Map<String, Product> products; // Change from List<Product> to Map<String, Product>
    private List<InventoryTransaction> inventoryTransactions = new ArrayList<>(); // cache of trx
    private List<InventoryTransaction> transactionsToDelete = new ArrayList<>(); // cache of trx to delete
    private Map<String, InventoryCount> inventoryCountsMap = new ConcurrentHashMap<>(); // inventory counts

    private static final String TOPIC = "retail_scans";
    private static final int MAX_INV_TRX_ROWS = 50000; // Max # of rows in file
    private static final int MAX_WRITE_INTERVAL_SECONDS = 300; // Max time in seconds before forcing a write
    private static final int MIN_INV_LEVEL = 1; // Define the constant

    public InventoryCounter(Properties props, Map<String, Product> products) {

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
        kafkaProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "30000"); // 30 seconds
        kafkaProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2500");

        this.consumer = new KafkaConsumer<>(kafkaProps);
        this.consumer.subscribe(Collections.singletonList(TOPIC));
        this.s3Uploader = new S3FileUploader(props); // Initialize S3FileUploader
        this.products = products;

    }

    @Override
    public void run() {
        running = true;
        int messageCount = 0;
        Random random = new Random();

        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    Transaction scan = parseTransaction(record);
                    if (scan == null)
                        continue;

                    // Look up the product (item_master) using the UPC code
                    Product product = TimeLogger.log("findProduct", () -> findProduct(scan));
                    if (product == null)
                        continue;

                    // Create or find an existing inventory record (storeId, itemId)
                    InventoryCount inventoryCount = TimeLogger.log("findOrCreateInventoryCount",
                            () -> findOrCreateInventoryCount(scan, product));

                    // If the item has been on order, handle restocking
                    TimeLogger.log("handleRestock", () -> handleRestock(inventoryCount, product, random));

                    // Reduce inventory on hand due to the sale (Yes, this creates a ton of rows and
                    // isn't realistic)
                    TimeLogger.log("processSaleTransaction",
                            () -> processSaleTransaction(scan, product, inventoryCount));

                    // If we are running low on inventory, reorder
                    TimeLogger.log("handleReorders", () -> handleReorders(product, inventoryCount));

                    messageCount++;
                }
                // If we have buffered up enough transactions, write them out
                if (shouldWriteToFile()) {
                    TimeLogger.log("handleTransactionsToDelete", () -> handleTransactionsToDelete());
                    writeAndUploadTransactions();
                }
                consumer.commitSync();
                System.out.printf("Messages consumed: %d, buffered: %d, inventory records: %d, delete list: %d\r",
                        messageCount,
                        inventoryTransactions.size(), inventoryCountsMap.size(), transactionsToDelete.size());
                // Write the inventoryCounts to S3 after max time has elapsed
                if (System.currentTimeMillis() - lastInventoryWriteTime >= MAX_WRITE_INTERVAL_SECONDS * 1000) {
                    writeAndUploadInventoryCounts();
                }
                Thread.sleep(2000);
            }
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "InventoryCounter interrupted: " + e.getMessage());
        } finally {
            consumer.close();
            s3Uploader.close();
            running = false;
            logger.log(Level.INFO, "Shutdown InventoryCounter.");
        }
    }

    private Transaction parseTransaction(ConsumerRecord<String, String> record) {
        try {
            return Transaction.fromJson(record.value());
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to parse record value: " + record.value());
            running = false;
            return null;
        }
    }

    private Product findProduct(Transaction scan) {
        Product product = products.get(scan.getItemUPC());
        if (product == null) {
            logger.log(Level.SEVERE, "No itemID found for UPC: " + scan.getItemUPC());
            running = false;
        }
        return product;
    }

    private InventoryCount findOrCreateInventoryCount(Transaction scan, Product product) {
        String key = product.getItemId() + "-" + scan.getStoreId(); // Create a composite key
        return inventoryCountsMap.computeIfAbsent(key, k -> {
            return new InventoryCount(
                    product.getItemId(),
                    scan.getStoreId(),
                    (product.getRepl_qty() == 1 ? 6 : product.getRepl_qty()), // QtyInStock
                    0); // QtyOnOrder
        });
    }

    private void handleRestock(InventoryCount inventoryCount, Product product, Random random) {
        // If there are items on order, randomly decide if restocking
        if (inventoryCount.getQtyOnOrder() > 0 && random.nextInt(4) >= 2) {
            // Randomly determine the restock quantity
            int restockQty = (random.nextInt(2) == 0) ? product.getRepl_qty() : inventoryCount.getQtyOnOrder();
            createAndAddTransaction("Restock", restockQty, inventoryCount, product);
        }
    }

    private void processSaleTransaction(Transaction scan, Product product, InventoryCount inventoryCount) {
        createAndAddTransaction("Sale", scan.getUnitQty(), inventoryCount, product);
        if (inventoryCount.getQtyInStock() < MIN_INV_LEVEL) {
            int transferQty = MIN_INV_LEVEL - inventoryCount.getQtyInStock();
            createAndAddTransaction("TransferIn", transferQty, inventoryCount, product);
            // Also create a TransferOut transaction that we will save for deletion later
            createAndAddTransaction("TransferOut", transferQty, inventoryCount, product);
        }
    }

    private void handleReorders(Product product, InventoryCount inventoryCount) {
        if (inventoryCount.getQtyInStock() < product.getRepl_qty()) {
            createAndAddTransaction("Order", product.getRepl_qty(), inventoryCount, product);
        }
    }

    private void handleTransactionsToDelete() {
        // Remove the first 20% of transactions in the delete list (arbitrary)
        int numToDelete = (int) (transactionsToDelete.size() * 0.2);

        if (numToDelete > 0) {
            List<InventoryTransaction> toRemove = new ArrayList<>();
            transactionsToDelete.subList(0, numToDelete).forEach(t -> {
                // Create a new transaction with isDeleted set to true
                InventoryTransaction deletedTransaction = new InventoryTransaction(
                        t.getTrxType(),
                        t.getInvDate(),
                        t.getStoreId(),
                        t.getItemId(),
                        t.getUnitQty());
                deletedTransaction.setDeleted(true);
                inventoryTransactions.add(deletedTransaction);
                toRemove.add(t);
            });
            logger.log(Level.INFO, String.format("Deleted %d of %d transactions from the delete list.",
                    toRemove.size(), transactionsToDelete.size()));
            transactionsToDelete.removeAll(toRemove);
        }
    }

    private void createAndAddTransaction(String type, int qty, InventoryCount inventoryCount, Product product) {
        InventoryTransaction transaction = new InventoryTransaction(
                type,
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                inventoryCount.getStoreId(),
                product.getItemId(),
                qty);
        inventoryTransactions.add(transaction);

        if (type.equals("Restock")) {
            inventoryCount.setQtyInStock(inventoryCount.getQtyInStock() + qty);
            inventoryCount.setQtyOnOrder(inventoryCount.getQtyOnOrder() - qty);
        } else if (type.equals("TransferIn")) {
            inventoryCount.setQtyInStock(inventoryCount.getQtyInStock() + qty);
        } else if (type.equals("Sale")) {
            inventoryCount.setQtyInStock(inventoryCount.getQtyInStock() - qty);
        } else if (type.equals("Order")) {
            inventoryCount.setQtyOnOrder(inventoryCount.getQtyOnOrder() + qty);
        } else if (type.equals("TransferOut")) {
            // No impact to inventory counts
            // Save this transaction for deletion later
            transactionsToDelete.add(transaction);
        }
    }

    /** File writing and S3 upload methods **/

    private boolean shouldWriteToFile() {
        long currentTime = System.currentTimeMillis();
        long elapsedTime = (currentTime - lastWriteTime) / 1000;
        return inventoryTransactions.size() > 0
                && (inventoryTransactions.size() >= MAX_INV_TRX_ROWS || elapsedTime >= MAX_WRITE_INTERVAL_SECONDS);
    }

    private void writeAndUploadTransactions() {
        String fileName = writeInventoryTransactionsToFile();
        if (fileName != null) {
            lastWriteTime = System.currentTimeMillis();
            uploadFileAsync(fileName);
        }
    }

    private String writeInventoryTransactionsToFile() {
        if (inventoryTransactions.size() == 0) {
            return null;
        }
        // Create a file name based on the current date and time
        String fileName = "/tmp/inventory_transactions_"
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddhhmmss")) + ".json";

        // Write the inventoryTransactions to the file
        logger.log(Level.INFO, String.format("Writing %d transactions",
                inventoryTransactions.size()));
        try (FileWriter fileWriter = new FileWriter(fileName)) {
            for (InventoryTransaction transaction : inventoryTransactions) {
                fileWriter.write(transaction.toJson() + "\n");
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error writing inventory transactions to file: " + e.getMessage());
            return null;
        }
        // Clear the list after writing to file
        inventoryTransactions.clear();
        // Return the generated file name
        return fileName;
    }

    private void writeAndUploadInventoryCounts() {
        String fileName = writeInventoryCountsToFile();
        if (fileName != null) {
            lastInventoryWriteTime = System.currentTimeMillis();
            uploadFileAsync(fileName);
        }
    }

    // write inventory count records that have been modified since the last write
    // If no records have changed, remove the zero-length file and return null
    private String writeInventoryCountsToFile() {
        // Create a file name based on the current date and time
        String fileName = "/tmp/inventory_counts_"
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddhhmmss")) + ".json";
        int recordCount = 0;

        // Write the inventoryCounts to the file
        // Note that we do not clear the inventoryCounts list after writing to file
        try (FileWriter fileWriter = new FileWriter(fileName)) {
            LocalDateTime lastInventoryWriteDateTime = Instant.ofEpochMilli(lastInventoryWriteTime)
                    .atZone(ZoneId.systemDefault())
                    .toLocalDateTime();
            for (InventoryCount count : inventoryCountsMap.values()) {
                if (count.getLastUpdated().isAfter(lastInventoryWriteDateTime)) {
                    fileWriter.write(count.toJson() + "\n");
                    recordCount++;
                }
            }
            if (recordCount == 0) {
                // Delete zero-length files and return null
                java.nio.file.Files.delete(Paths.get(fileName));
                return null;
            } else {
                logger.log(Level.INFO, String.format("Wrote %d inventory counts", recordCount));
                return fileName;
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error writing inventory counts to file: " + e.getMessage());
            return null;
        }
    }

    private void uploadFileAsync(String fileName) {
        CompletableFuture<Boolean> uploadResult = s3Uploader.uploadFileAsync(fileName);
        uploadResult.thenAccept(success -> {
            running = success; // if upload fails, stop processing
        });
    }

}
