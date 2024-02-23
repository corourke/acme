package acme;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PriceUpdater {
    private Properties props = new Properties();
    private Random rand = new Random();
    private static final Logger logger = Logger.getLogger(PriceUpdater.class.getName());
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");

    public static void main(String[] args) {
        PriceUpdater updater = new PriceUpdater();
        Runtime.getRuntime().addShutdownHook(new Thread(updater::shutdown));
        logger.setLevel(Level.INFO);
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
        Connection conn = null;
        try {
            conn = createConnection();
            logger.log(Level.INFO, "{0} - Connected to the PostgreSQL server successfully.", sdf.format(new Date()));
            // Main loop
            while (!Thread.currentThread().isInterrupted()) {
                // Check time
                if (isBusinessHours()) {
                    // Check that connection is still valid
                    if (!conn.isValid(5)) { // 5 seconds timeout
                        logger.log(Level.WARNING, "{0} - Database connection is no longer valid, reconnecting.",
                                sdf.format(new Date()));
                        conn = createConnection();
                    }
                    // Do the next set of updates
                    int categoryCode = selectRandomCategory(conn);
                    if (categoryCode != 0) {
                        logger.log(Level.INFO, "{0} - Doing a price update for category: {1}",
                                new Object[] { sdf.format(new Date()), categoryCode });
                        updateItemPrices(conn, categoryCode);
                    }
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
        int updateCount = 100 + rand.nextInt(501); // Random number between 100 and 600
        String sql = "WITH updated AS (SELECT item_id FROM item_master WHERE category_code = ? ORDER BY RANDOM() LIMIT "
                + updateCount + ") " +
                "UPDATE item_master SET item_price = item_price * (0.9 + (0.2 * RANDOM())) FROM updated WHERE item_master.item_id = updated.item_id";
        // System.out.println("Executing SQL: " + sql.replace("?",
        // String.valueOf(categoryCode)));

        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setInt(1, categoryCode);
        pstmt.executeUpdate();
        logger.log(Level.INFO, "{0} - Number of prices updated: {1}",
                new Object[] { sdf.format(new Date()), updateCount });
    }

    // Check if it is business hours
    private boolean isBusinessHours() {
        LocalTime now = LocalTime.now();
        boolean isBusinessHours = !(now.isAfter(LocalTime.of(19, 0)) || now.isBefore(LocalTime.of(9, 0)));
        logger.log(Level.INFO, "{0} - Business hours? {1}", new Object[] { sdf.format(new Date()), isBusinessHours });
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
