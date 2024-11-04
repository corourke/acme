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
import java.util.logging.Logger;
import java.util.logging.Level;

public class InventoryCounter implements Runnable {
    private static final Logger logger = Logger.getLogger(InventoryCounter.class.getName());
    private KafkaConsumer<String, String> consumer;
    private S3FileUploader s3Uploader;
    private boolean running = true;
    private long lastWriteTime = System.currentTimeMillis(); // Track the last write time
    private long lastInventoryWriteTime = System.currentTimeMillis(); // Track the last inventory write time

    private List<Product> products; // list of producst item_master for item_id lookup
    private List<InventoryTransaction> inventoryTransactions = new ArrayList<>(); // cache of trx
    private List<InventoryTransaction> transactionsToDelete = new ArrayList<>(); // cache of trx to delete
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

                    Product product = findProduct(scan);
                    if (product == null)
                        continue;

                    InventoryCount inventoryCount = findOrCreateInventoryCount(scan, product);
                    handleRestock(inventoryCount, product, random);
                    processSaleTransaction(scan, product, inventoryCount);
                    handleReorders(product, inventoryCount);

                    if (shouldWriteToFile()) {
                        handleTransactionsToDelete();
                        if (!writeAndUploadTransactions()) {
                            running = false;
                        }
                    }
                    // Write the inventoryCounts to a file and upload to S3 after
                    // MAX_WRITE_INTERVAL_SECONDS has elapsed
                    if (System.currentTimeMillis() - lastInventoryWriteTime >= MAX_WRITE_INTERVAL_SECONDS * 1000) {
                        writeAndUploadInventoryCounts();
                    }
                    messageCount++;
                }
                consumer.commitSync();
                System.out.printf("\rConsumed scan messages count: %d, inventoryCount: %d", messageCount,
                        inventoryCounts.size());
                Thread.sleep(1000);
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
            logger.log(Level.SEVERE, "Failed to parse record value: {0}", record.value());
            return null;
        }
    }

    private Product findProduct(Transaction scan) {
        return products.stream()
                .filter(p -> p.getItemUPC().equals(scan.getItemUPC()))
                .findFirst()
                .orElseGet(() -> {
                    logger.log(Level.SEVERE, "Product not found for UPC: {0}", scan.getItemUPC());
                    return null;
                });
    }

    private InventoryCount findOrCreateInventoryCount(Transaction scan, Product product) {
        return inventoryCounts.stream()
                .filter(ic -> ic.getItemId() == product.getItemId() && ic.getStoreId() == scan.getStoreId())
                .findFirst()
                .orElseGet(() -> createInventoryCount(scan, product));
    }

    private InventoryCount createInventoryCount(Transaction scan, Product product) {
        InventoryCount inventoryCount = new InventoryCount(
                product.getItemId(),
                scan.getStoreId(),
                (product.getRepl_qty() == 1 ? 6 : product.getRepl_qty()), // QtyInStock
                0); // QtyOnOrder
        inventoryCounts.add(inventoryCount);
        return inventoryCount;
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
        // Remove the first 20% of transactions in the delete list
        int numToDelete = (int) (transactionsToDelete.size() * 0.2);
        logger.log(Level.INFO, String.format("\nPreparing to delete %d of %d transactions from the delete list.",
                numToDelete, transactionsToDelete.size()));

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
            transactionsToDelete.removeAll(toRemove);
            logger.log(Level.INFO, String.format("\nDeleted %d transactions from the delete list.", toRemove.size()));
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

    private boolean shouldWriteToFile() {
        long currentTime = System.currentTimeMillis();
        long elapsedTime = (currentTime - lastWriteTime) / 1000;
        return inventoryTransactions.size() >= MAX_INV_TRX_ROWS || elapsedTime >= MAX_WRITE_INTERVAL_SECONDS;
    }

    private boolean writeAndUploadTransactions() {
        String fileName = writeInventoryTransactionsToFile();
        lastWriteTime = System.currentTimeMillis();
        return fileName != null && s3Uploader.uploadFile(fileName);
    }

    private String writeInventoryTransactionsToFile() {
        // Create a file name based on the current date and time
        String fileName = "/tmp/inventory_transactions_"
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddhhmmss")) + ".json";

        // Write the inventoryTransactions to the file
        logger.log(Level.INFO, "\nWriting inventory transactions to file: " + fileName);
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

    private boolean writeAndUploadInventoryCounts() {
        String fileName = writeInventoryCountsToFile();
        lastInventoryWriteTime = System.currentTimeMillis();
        return fileName != null && s3Uploader.uploadFile(fileName);
    }

    private String writeInventoryCountsToFile() {
        // Create a file name based on the current date and time
        String fileName = "/tmp/inventory_counts_"
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddhhmmss")) + ".json";

        // Write the inventoryCounts to the file
        // Note that we do not clear the inventoryCounts list after writing to file
        logger.log(Level.INFO, "\nWriting inventory counts to file: " + fileName);
        try (FileWriter fileWriter = new FileWriter(fileName)) {
            for (InventoryCount count : inventoryCounts) {
                fileWriter.write(count.toJson() + "\n");
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error writing inventory counts to file: " + e.getMessage());
            return null;
        }
        // Return the generated file name
        return fileName;
    }

}
