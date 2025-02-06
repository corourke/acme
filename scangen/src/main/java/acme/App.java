package acme;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Data generator. Generates retail scan transactions related to a list of
 * products and stores and sends them to a Kafka topic.
 */

public class App {
    static Connection dbConnection = null;
    static Logger logger = Logger.getLogger(App.class.getName());
    static Properties config = new Properties();

    public static void main(String[] args) {

        // Disable Jansi library as it has a bug that causes the console to hang
        System.setProperty("log4j.skipJansi", "true");

        // Load properties file
        try {
            config.load(new FileInputStream("config.properties"));
        } catch (IOException e) {
            System.out.println("Unable to load configuration file. Please check the file path and try again.");
            System.exit(1);
        }

        // Connect to the database
        try {
            dbConnection = createConnection();
            logger.log(Level.INFO, "Connected to the PostgreSQL server successfully.");
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Database connection failed", e);
            System.exit(1);
        }

        // Load lookup tables from the Postgres database
        List<Product> products = loadProductsFromDB();
        System.out.println("Products read: " + products.size());
        List<Store> stores = loadStoresFromDB();
        System.out.println("Stores read: " + stores.size());
        closeConnection();

        // Initialize Kafka configuration
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", config.getProperty("bootstrap.servers"));
        kafkaProps.put("security.protocol", config.getProperty("security.protocol"));
        kafkaProps.put("sasl.mechanism", config.getProperty("sasl.mechanism"));
        kafkaProps.put("sasl.jaas.config", config.getProperty("sasl.jaas.config"));
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Make a list of the unique timezones in the stores list
        List<String> timezones = stores.stream().map(Store::getTimezone).distinct().collect(Collectors.toList());

        // Initialize and start the transaction generators
        System.out.println("Starting generator threads...");
        Map<String, Thread> generatorThreads = new HashMap<>();

        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            generatorThreads.forEach((timezone, generatorThread) -> {
                generatorThread.interrupt(); // Attempt to stop the generator thread
            });
        }));

        for (String timezone : timezones) {
            TransactionGenerator generator = new TransactionGenerator(config, kafkaProps, products, stores, timezone);
            Thread generatorThread = new Thread(generator); // Wrap the generator in a Thread
            generatorThreads.put(timezone, generatorThread);
            // generators.put(timezone, generator);
            generatorThread.start(); // Start the generator thread
            // Delay for 7 seconds to keep reporting from interleaving
            try {
                Thread.sleep(7000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // The main thread can continue to monitor or manage the application, if
        // necessary
    }

    // Create a connection to the database, nothin fancy
    private static Connection createConnection() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:postgresql://" + config.getProperty("database.host") + ":" +
                        config.getProperty("database.port") + "/" + config.getProperty("database.name"),
                config.getProperty("database.user"),
                config.getProperty("database.password"));
    }

    private static void closeConnection() {
        System.out.println("Closing the database connection...");
        try {
            if (dbConnection != null && dbConnection.isValid(5)) {
                dbConnection.close();
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Failed to close the database connection", e);
        }
    }

    private static List<Product> loadProductsFromDB() {
        List<Product> products = new ArrayList<>();
        String productQuery = "SELECT ITEM_ID, ITEM_PRICE, ITEM_UPC, _FREQUENCY FROM retail.item_master WHERE ITEM_ID < 50000";
        try (Statement stmt = dbConnection.createStatement();
                ResultSet rs = stmt.executeQuery(productQuery)) {
            while (rs.next()) {
                int itemId = rs.getInt("ITEM_ID");
                BigDecimal itemPrice = rs.getBigDecimal("ITEM_PRICE");
                String itemUpc = rs.getString("ITEM_UPC");
                int frequency = rs.getInt("_FREQUENCY");
                products.add(new Product(itemId, itemPrice, itemUpc, frequency));
            }
        } catch (SQLException e) {
            System.err.println("Error loading products from DB: " + e.getMessage());
            System.exit(1);
        }
        return products;
    }

    private static List<Store> loadStoresFromDB() {
        List<Store> stores = new ArrayList<>();
        String storeQuery = "SELECT store_id, state, timezone FROM retail.stores";
        try (Statement stmt = dbConnection.createStatement();
                ResultSet rs = stmt.executeQuery(storeQuery)) {
            while (rs.next()) {
                int storeId = rs.getInt("store_id");
                String state = rs.getString("state");
                String timezone = rs.getString("timezone");
                stores.add(new Store(storeId, state, timezone));
            }
        } catch (SQLException e) {
            System.err.println("Error loading stores from DB: " + e.getMessage());
            System.exit(1);
        }
        return stores;
    }

}
