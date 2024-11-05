package acme;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import acme.objects.Product;

import java.util.logging.Level;
import java.util.logging.Logger;

public class InventoryUpdater {
    private static Properties props = new Properties();
    private static Connection DBconnection;
    private static Map<String, Product> products;

    private static final Logger logger = Logger.getLogger(InventoryUpdater.class.getName());

    public static void main(String[] args) {
        // Disable Jansi library as it has a bug that causes the console to hang
        System.setProperty("log4j.skipJansi", "true");

        // Load properties
        try {
            props.load(new FileInputStream("config.properties"));
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to load properties file", e);
            System.exit(1);
        }

        // Register JDBC driver
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Failed to load JDBC driver", e);
            return;
        }

        // Read in the products table
        DBconnection = null;
        products = new HashMap<>();
        try {
            DBconnection = createConnection();
            logger.log(Level.INFO, "Connected to the PostgreSQL server successfully.");
            // Load products into memory
            PreparedStatement pstmt = DBconnection.prepareStatement("SELECT * FROM item_master where item_id < 50000");
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                products.put(rs.getString("item_upc"),
                        new Product(
                                rs.getInt("item_id"),
                                rs.getBigDecimal("item_price"),
                                rs.getString("item_upc"),
                                rs.getInt("repl_qty")));
            }
            logger.log(Level.INFO, String.format("Loaded %d products", products.size()));
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "SQL Error: " + e.getMessage());
        } finally {
            closeConnection();
        }

        InventoryCounter counter = new InventoryCounter(props, products);
        Thread counterThread = new Thread(counter);
        counterThread.start();

    }

    // Create a connection to the database, nothin fancy
    private static Connection createConnection() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:postgresql://" + props.getProperty("database.host") + ":" +
                        props.getProperty("database.port") + "/" + props.getProperty("database.name"),
                props.getProperty("database.user"), props.getProperty("database.password"));
    }

    private static void closeConnection() {
        logger.log(Level.INFO, "Closing the database connection...");
        try {
            if (DBconnection != null && DBconnection.isValid(5)) {
                DBconnection.close();
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Failed to close the database connection", e);
        }
    }

}
