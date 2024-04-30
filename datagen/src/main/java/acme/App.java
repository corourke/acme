package acme;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Data genearator. Generates retail scan transactions related to a list of
 * products and stores
 * and sends them to a Kafka topic.
 */

public class App {
    public static void main(String[] args) {
        // Disable Jansi library as it has a bug that causes the console to hang
        System.setProperty("log4j.skipJansi", "true");
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

        // Initialize Kafka configuration
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", config.getProperty("bootstrap.servers"));
        kafkaProps.put("security.protocol", config.getProperty("security.protocol"));
        kafkaProps.put("sasl.mechanism", config.getProperty("sasl.mechanism"));
        kafkaProps.put("sasl.jaas.config", config.getProperty("sasl.jaas.config"));
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        if (!Files.exists(Paths.get(itemMasterPath))
                || !Files.exists(Paths.get(retailStoresPath))) {
            System.out.println("One or more input files do not exist. Please check the file paths and try again.");
            System.exit(1);
        }

        // Read in the lookup tables. One potential improvement would be to query
        // the database, but this would add an additional dependency. If this is done,
        // be sure to query products with ID < 50000 to avoid conflicts with the price
        // updater.
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

        // Make a list of the unique timezones in the stores list
        List<String> timezones = stores.stream().map(Store::getTimezone).distinct().collect(Collectors.toList());
        // Initialize and start the transaction generators
        System.out.println("Starting...");
        Map<String, Thread> generatorThreads = new HashMap<>();
        // Map<String, TransactionGenerator> generators = new HashMap<>();

        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            generatorThreads.forEach((timezone, generatorThread) -> {
                generatorThread.interrupt(); // Attempt to stop the generator thread
            });
            // generators.forEach((timezone, generator) -> {
            // generator.stopRunning(); // Stop the generator
            // });
            // statusWebServer.stopServer();
        }));

        for (String timezone : timezones) {
            TransactionGenerator generator = new TransactionGenerator(config, kafkaProps, products, stores, timezone);
            Thread generatorThread = new Thread(generator); // Wrap the generator in a Thread
            generatorThreads.put(timezone, generatorThread);
            // generators.put(timezone, generator);
            generatorThread.start(); // Start the generator thread
            // Delay for 7 seconds to kind of keep reporting from interleaving
            try {
                Thread.sleep(7000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // The main thread can continue to monitor or manage the application, if
        // necessary
    }
}
