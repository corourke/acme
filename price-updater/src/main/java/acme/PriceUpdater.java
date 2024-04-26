// simulate price fluctuations and the addition of new items within a PostgreSQL database 
// (specifically the item_master table)

package acme;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.HashSet;

public class PriceUpdater {
    private Properties props = new Properties();
    private Connection DBconnection;
    private Random rand = new Random();
    private static final Logger logger = Logger.getLogger(PriceUpdater.class.getName());
    private int nextItemId = 50000;
    private Set<String> existingUPCs = new HashSet<>();

    public static void main(String[] args) {
        PriceUpdater updater = new PriceUpdater();
        Runtime.getRuntime().addShutdownHook(new Thread(updater::shutdown));
        logger.setLevel(Level.INFO);
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%4$s: %2$s %1$tb %1$td, %1$tY %1$tl:%1$tM:%1$tS %1$Tp  - %5$s%6$s%n");

        updater.run();
    }

    private void shutdown() {
        System.out.println("Shutting down...");
        // Any cleanup code can go here
    }

    public PriceUpdater() {
        try {
            // Load properties
            props.load(new FileInputStream("config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void run() {
        // Register JDBC driver
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        // The main part of the program, exits on error or when interrupted
        DBconnection = null;
        try {
            DBconnection = createConnection();
            logger.log(Level.INFO, "Connected to the PostgreSQL server successfully.");
            // Main loop
            while (!Thread.currentThread().isInterrupted()) {
                // Check time
                if (isBusinessHours()) {
                    // Check that connection is still valid
                    if (!DBconnection.isValid(5)) { // 5 seconds timeout
                        logger.log(Level.WARNING, "Database connection is no longer valid, reconnecting.");
                        DBconnection = createConnection();
                    }
                    // Do the next set of updates
                    int categoryCode = selectRandomCategory(DBconnection);
                    if (categoryCode != 0) {
                        logger.log(Level.INFO, String.format("Doing a price update for category: %d", categoryCode));
                        updateItemPrices(DBconnection, categoryCode);
                    }
                    // Insert a few new dummy items
                    insertNewItems(DBconnection, categoryCode);

                    // Delete a few items
                    deleteItems(DBconnection);

                    // Sleep for a random time
                    sleepRandomTime();
                } else {
                    Thread.sleep(600000); // Sleep for 10 minutes before checking time again
                }
            }
        } catch (SQLException e) {
            System.out.println("SQL Error: " + e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Create a connection to the database, nothin fancy
    private Connection createConnection() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:postgresql://" + props.getProperty("database.host") + ":" +
                        props.getProperty("database.port") + "/" + props.getProperty("database.name"),
                props.getProperty("database.user"), props.getProperty("database.password"));
    }

    // Select a random category_code from the item_categories table
    private int selectRandomCategory(Connection conn) throws SQLException {
        String sql = "SELECT category_code FROM item_categories ORDER BY RANDOM() LIMIT 1";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery();
        if (rs.next()) {
            return rs.getInt("category_code");
        }
        return 0;
    }

    // Update the prices of a random set of items in the given category
    private void updateItemPrices(Connection conn, int categoryCode) throws SQLException {
        int updateCount = 20 + rand.nextInt(81); // Random number between 20 and 100
        String sql = "WITH updated AS (SELECT item_id FROM item_master WHERE category_code = ? ORDER BY RANDOM() LIMIT "
                + updateCount + ") " +
                "UPDATE item_master SET item_price = item_price * (0.9 + (0.2 * RANDOM())) FROM updated WHERE item_master.item_id = updated.item_id";

        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setInt(1, categoryCode);
        pstmt.executeUpdate();
        logger.log(Level.INFO, String.format("Number of prices updated: %d", updateCount));
    }

    // Insert a few new dummy items in the given category
    private void insertNewItems(Connection conn, int categoryCode) throws SQLException {
        int numNewItems = 10 + rand.nextInt(11); // Random number between 10 and 20
        String upc;
        nextItemId = findNextItemId(conn);
        loadExistingUPCs(conn); // Load existing UPCs into memory
        logger.log(Level.INFO, String.format("Next item_id: %d", nextItemId));

        do {
            upc = generateUPC();
        } while (existingUPCs.contains(upc)); // Efficient in-memory check
        existingUPCs.add(upc); // Keep track of the new UPC

        String sql = "INSERT INTO item_master "
                + "(category_code, item_id, item_price, item_upc, repl_qty, _frequency) "
                + "VALUES (?, ?, 1.00, ?, 1, 1)";

        PreparedStatement pstmt = conn.prepareStatement(sql);
        for (int i = 1; i <= numNewItems; i++) {
            // Generate new UPC
            do {
                upc = generateUPC();
            } while (existingUPCs.contains(upc)); // Efficient in-memory check
            existingUPCs.add(upc); // Keep track of the new UPC
            // Insert the new item row
            pstmt.setInt(1, categoryCode);
            pstmt.setInt(2, nextItemId + i); // Ensure unique item_id values
            pstmt.setString(3, upc);
            pstmt.executeUpdate();
        }

        logger.log(Level.INFO, String.format("Number of new items inserted: %d", numNewItems));
    }

    private void deleteItems(Connection conn) throws SQLException {
        int deleteCount = 5 + rand.nextInt(6); // Random number between 5 and 10
        String sql = "DELETE from item_master where item_id in ("
                + " SELECT item_id from item_master WHERE item_id >= 50000"
                + " ORDER BY RANDOM() LIMIT 10)";

        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.execute();
        logger.log(Level.INFO, String.format("Number of items deleted: %d", deleteCount));
    }

    // Helper function to generate a random 11-digit UPC (without check digit)
    private String generateUPC() {
        StringBuilder sb = new StringBuilder();
        sb.append("2"); // Start with '2' so we can easily spot them
        for (int i = 1; i < 11; i++) { // Notice the loop starts at 1
            sb.append(rand.nextInt(10));
        }
        return sb.toString();
    }

    // Load existing UPCs into memory
    private void loadExistingUPCs(Connection conn) throws SQLException {
        existingUPCs.clear(); // Reset the set before loading
        PreparedStatement pstmt = conn.prepareStatement("SELECT item_upc FROM item_master");
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()) {
            existingUPCs.add(rs.getString("item_upc"));
        }
    };

    // Helper function to find the next item_id (over 50000)
    private int findNextItemId(Connection conn) throws SQLException {
        int maxItemId = 50000;
        String sql = "SELECT MAX(item_id) max_item_id FROM item_master";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery();
        if (rs.next()) {
            maxItemId = rs.getInt("max_item_id");
        }
        if (maxItemId < 50000) {
            maxItemId = 50000;
        }
        return maxItemId;
    }

    // Check if it is business hours
    private boolean isBusinessHours() {
        LocalTime now = LocalTime.now();
        boolean isBusinessHours = !(now.isAfter(LocalTime.of(19, 0)) || now.isBefore(LocalTime.of(9, 0)));
        logger.log(Level.INFO, String.format("Business hours? %s", isBusinessHours));
        return isBusinessHours;

    }

    // Sleep for a random time between 30 and 120 minutes
    private void sleepRandomTime() {
        int sleepTime = 30 + rand.nextInt(91); // Random time between 30 and 90 minutes
        try {
            Thread.sleep(sleepTime * 60000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
