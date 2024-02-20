package corourke.datagen;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.*;
import java.time.format.DateTimeFormatter;

import org.json.JSONArray;
import org.json.JSONObject;

public class TransactionGenerator implements Runnable {
  private static final int BATCH_SIZE_THRESHOLD = 1000;
  private String outputDirectory;
  private List<Product> products;
  private List<Store> stores;
  private int offsetHours;
  private String timezone = null;
  private boolean running = false;
  private Random random = new Random();

  public TransactionGenerator(List<Product> products, List<Store> stores, String timezone, String outputDirectory) {
    this.products = products;
    this.stores = stores.stream() // make a list of stores in this timezone
        .filter(store -> store.getTimezone().equals(timezone))
        .collect(Collectors.toList());
    this.timezone = timezone;
    this.outputDirectory = outputDirectory;
    // Extract the offset hours from the timezone string like "PST (GMT-08)"
    Pattern pattern = Pattern.compile("GMT([+-]\\d{2})");
    Matcher matcher = pattern.matcher(timezone);
    if (matcher.find()) {
      this.offsetHours = Integer.parseInt(matcher.group(1));
    }
    System.out.println("TransactionGenerator constructor called, timezone: " + timezone + " offset: " + offsetHours);

  }

  @Override
  public void run() {
    running = true;
    while (running) {
      try {
        generateTransactions();
        Thread.sleep(60000); // Sleep for 1 minute
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        running = false;
        System.out.println("Generator interrupted, timezone: " + timezone);
      }
    }
  }

  public void stop() {
    running = false;
  }

  private void generateTransactions() {
    List<Transaction> batch = new ArrayList<>();
    LocalDateTime localDateTime = LocalDateTime.now(ZoneId.of("UTC")).plusHours(offsetHours);
    LocalTime localTime = localDateTime.toLocalTime();
    System.out.println(
        "Generating transactions for timezone: " + timezone + " at time: " + localTime);

    // For each store, figure out how many transactions to generate
    for (Store store : stores) {
      int baseTransactions = calculateBaseTransactions(localTime);
      Random random = new Random();
      double adjustment = 0.8 + (1.2 - 0.8) * random.nextDouble();
      baseTransactions = Math.max((int) (baseTransactions * adjustment), 0);
      int transactionsCount = adjustTransactionsForTimezone(baseTransactions, timezone);

      for (int i = 0; i < transactionsCount; i++) {
        Product product = selectProduct();
        BigDecimal price = product.getItemPrice();
        int quantity = random.nextInt(10) < 9 ? 1 : 2 + random.nextInt(3); // Mostly 1, occasionally 2-4
        // Randomize the transaction time within the minute
        int randomSeconds = random.nextInt(60);
        LocalDateTime randomizedDateTime = localDateTime.plusSeconds(randomSeconds);

        Transaction transaction = new Transaction(
            UUID.randomUUID(),
            store.getStoreId(),
            randomizedDateTime,
            product.getItemUPC(),
            quantity,
            price);
        batch.add(transaction);

        // Check if batch size threshold is reached
        if (batch.size() >= BATCH_SIZE_THRESHOLD) {
          // Send batch for processing, e.g., saving to a file or sending to Kafka
          processBatch(batch);
          batch.clear();
        }
      }
    }

    // Process any remaining transactions in the batch
    if (!batch.isEmpty()) {
      processBatch(batch);
    }
  }

  private void processBatch(List<Transaction> batch) {
    System.out.println("Processing batch of " + batch.size() + " transactions, in timezone: " + timezone);

    // Generate a unique batch ID as a UUID
    UUID batchId = UUID.randomUUID();

    // Create a JSON array to hold the transactions
    JSONArray transactionsArray = new JSONArray();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Convert each Transaction in the batch to a JSONObject and add it to the array
    // Example:
    // {"scan_id":"01HQ2E19PWJVH1H89DG1J1JJW8","store_id":8157,"scan_datetime":"2024-02-20
    // 04:57:42","item_upc":"13965265859","unit_qty":2, "unit_price":34.88}
    for (Transaction transaction : batch) {
      JSONObject transactionObject = new JSONObject();
      transactionObject.put("scan_id", transaction.getScanId().toString());
      transactionObject.put("store_id", transaction.getStoreId());
      // Format the LocalDateTime as a string and put it in the JSONObject
      String formattedDatetime = transaction.getScanDatetime().format(formatter);
      transactionObject.put("scan_datetime", formattedDatetime);
      transactionObject.put("item_upc", transaction.getItemUPC());
      transactionObject.put("unit_qty", transaction.getUnitQty());
      transactionObject.put("unit_price", transaction.getUnitPrice().toString());
      transactionsArray.put(transactionObject);
    }

    // Create a JSONObject for the batch and put the batch ID and transactions array
    // into it
    // Example:
    // {"batch":[
    // {"scan_id":"01HQ2E19PWJVH1H89DG1J1JJW8","store_id":8157,"scan_datetime":"2024-02-20
    // 04:57:42","item_upc":"13965265859","unit_qty":2, "unit_price":34.88},
    // {"scan_id":"01HQ2E19PX56KMDR7PYS96DQD8","store_id":2021,"scan_datetime":"2024-02-20
    // 04:57:36","item_upc":"11952032591","unit_qty":2,"unit_price":273.88}
    // ],"batch_id":"01HQ2E19Q5YR81354MMAMQH986"}
    JSONObject batchObject = new JSONObject();
    batchObject.put("batch_id", batchId.toString());
    batchObject.put("batch", transactionsArray);

    // Get the current timestamp
    LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));

    // Output the JSON batch object to a local file, name the file with the current
    // timestamp and the region (3 letter timezone identifier)
    String filename = "batch_" + now.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")) + "_"
        + timezone.substring(0, 3) + ".json";
    try {
      Files.write(Paths.get(outputDirectory, filename), batchObject.toString().getBytes());
      System.out.println("Batch written to file: " + filename);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  private int calculateBaseTransactions(LocalTime time) {
    int hour = time.getHour();
    int baseTransactions;

    // Determine base number of transactions based on time of day
    if (hour >= 8 && hour < 11) {
      baseTransactions = 5;
    } else if (hour >= 11 && hour < 13) {
      baseTransactions = 40;
    } else if (hour >= 13 && hour < 16) {
      baseTransactions = 10;
    } else if (hour >= 16 && hour < 19) {
      baseTransactions = 20;
    } else if (hour >= 19 && hour < 22) {
      baseTransactions = 5;
    } else {
      return 0; // Store closed or not opened yet
    }
    return baseTransactions;
  }

  private int adjustTransactionsForTimezone(int baseTransactions, String timezone) {
    // Adjust number of transactions based on store timezone, whole numbers only
    int adjustedTransactions;
    if ("HST (GMT-10)".equals(timezone) || "AKST (GMT-09)".equals(timezone)) {
      adjustedTransactions = (int) (baseTransactions * 0.4);
    } else if ("MST (GMT-07)".equals(timezone)) {
      adjustedTransactions = (int) (baseTransactions * 0.8);
    } else {
      adjustedTransactions = baseTransactions; // Assumes EST-5 or similar traffic without further adjustment
    }
    // Ensure the result is not less than 0
    return Math.max(adjustedTransactions, 0);
  }

  private Product selectProduct() {
    // Selection logic based on product frequency
    // Example placeholder logic
    return products.get(random.nextInt(products.size())); // Replace with actual logic
  }

}
