package corourke.datagen;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Data genearator
 */

public class App {
    public static void main(String[] args) {
        System.out.println("Starting...");

        // Load configurations
        Properties config = new Properties();
        try {
            config.load(new FileInputStream("config.properties"));
        } catch (IOException e) {
            System.out.println("Unable to load configuration file. Please check the file path and try again.");
            System.exit(1);
        }

        String itemMasterPath = config.getProperty("itemMasterPath");
        String retailStoresPath = config.getProperty("retailStoresPath");
        String outputDirectory = config.getProperty("outputDirectory");

        if (!Files.exists(Paths.get(itemMasterPath))
                || !Files.exists(Paths.get(retailStoresPath))) {
            System.out.println("One or more input files do not exist. Please check the file paths and try again.");
            System.exit(1);
        }

        // Read in the lookup tables
        CSVReader<Product> productReader = new CSVReader<>();
        List<Product> products = productReader.read(itemMasterPath, record -> new Product(
                Integer.parseInt(record.get("ITEM_ID")), // ITEM_ID
                new BigDecimal(record.get("ITEM_PRICE")), // ITEM_PRICE
                record.get("ITEM_UPC"), // ITEM_UPC
                Integer.parseInt(record.get("_FREQUENCY")) // _FREQUENCY
        ));
        CSVReader<Store> storeReader = new CSVReader<>();
        List<Store> stores = storeReader.read(retailStoresPath, record -> new Store(
                Integer.parseInt(record.get("store_id")), // store_id
                record.get("state"), // state
                record.get("timezone") // timezone
        ));
        System.out.println("Products read: " + products.size());
        System.out.println("Stores read: " + stores.size());

        // Start the simple web server for UI
        // StatusWebServer statusWebServer = new StatusWebServer();
        // statusWebServer.startServer(8080); // Example port

        // Initialize Kafka producer
        // KafkaProducerWrapper kafkaProducer = new
        // KafkaProducerWrapper(config.getKafkaConfig());

        // Make a list of the unique timezones in the stores list
        List<String> timezones = stores.stream().map(Store::getTimezone).distinct().collect(Collectors.toList());
        // Initialize and start the transaction generators
        Map<String, Thread> generatorThreads = new HashMap<>();
        for (String timezone : timezones) {
            TransactionGenerator generator = new TransactionGenerator(products, stores, timezone, outputDirectory);
            Thread generatorThread = new Thread(generator); // Wrap the generator in a Thread
            generatorThreads.put(timezone, generatorThread);
            generatorThread.start(); // Start the generator thread
        }

        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            timezones.forEach(timezone -> {
                Thread generatorThread = generatorThreads.get(timezone);
                generatorThread.interrupt(); // Attempt to stop the generator thread
            });
            // kafkaProducer.close();
            // statusWebServer.stopServer();
            System.out.println("Shutdown complete.");
        }));

        // The main thread can continue to monitor or manage the application, if
        // necessary
    }
}
