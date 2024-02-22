package acme;

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
import corourke.utils.WeightedRandomListSelector;

/**
 * Generate transactions for a list of stores in a given timezone
 */
public class TransactionGenerator implements Runnable {
  // Setup variables
  private static final int BATCH_SIZE_THRESHOLD = 5000;
  private Random random = new Random();

  // TODO: Put all these in shared appState
  private ApplicationState appState;
  private List<Product> products;
  private List<Store> stores;
  private WeightedRandomListSelector<Product> product_weights;

  // Status variables
  String timezone = null;
  boolean running = false;
  LocalDateTime localDateTime;
  LocalTime localTime;
  int offsetHours;
  int transactionOutput;

  public TransactionGenerator(ApplicationState appState, List<Product> products, List<Store> stores, String timezone) {
    this.appState = appState;
    this.products = products;
    this.product_weights = new WeightedRandomListSelector<>();
    this.product_weights.preComputeCumulativeWeights(products);
    // make a list of stores in this timezone
    this.stores = stores.stream()
        .filter(store -> store.getTimezone().equals(timezone))
        .collect(Collectors.toList());
    this.timezone = timezone;
    // Extract the offset hours from the timezone string like "PST (GMT-08)"
    Pattern pattern = Pattern.compile("GMT([+-]\\d{2})");
    Matcher matcher = pattern.matcher(timezone);
    if (matcher.find()) {
      this.offsetHours = Integer.parseInt(matcher.group(1));
    }
    System.out.println("Generator starting, timezone: " + timezone + " offset: " + offsetHours);
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
    localDateTime = LocalDateTime.now(ZoneId.of("UTC")).plusHours(offsetHours);
    localTime = localDateTime.toLocalTime();

    // For each store, figure out how many transactions to generate
    int inProcessTransactionCount = 0;
    for (Store store : stores) {
      int transactionTarget = getTransactionTarget();
      inProcessTransactionCount += transactionTarget;

      for (int i = 0; i < transactionTarget; i++) {
        Product product = product_weights.selectNextWeightedItem(products);
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

    transactionOutput = inProcessTransactionCount;
    LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
    System.out.println(now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) +
        " Region: " + timezone + " at: " + localTime + " produced " + transactionOutput + " transactions");

  }

  private void processBatch(List<Transaction> batch) {
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
    // { "batch_id":"01HQ2E19Q5YR81354MMAMQH986",
    // "batch":[
    // { "scan_id":"01HQ2E19PWJVH1H89DG1J1JJW8",
    // "store_id":8157, "scan_datetime":"2024-02-20 04:57:42",
    // "item_upc":"13965265859","unit_qty":2, "unit_price":34.88 }
    // ]}
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
      Files.write(Paths.get(appState.getOutputDirectory(), filename), batchObject.toString().getBytes());
      System.out.println(now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + " Batch for region: "
          + timezone + " size: " + batch.size() + " written to: " + filename);

    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  // Calculate the number of transactions to generate based on the time of day
  private int getTransactionTarget() {
    int baseTransactions = calculateBaseTransactions(localTime);
    double randomFactor = 0.8 + (1.2 - 0.8) * random.nextDouble();
    baseTransactions = Math.max((int) (baseTransactions * randomFactor), 0);
    return adjustTransactionsForTimezone(baseTransactions, timezone);
  }

  private int calculateBaseTransactions(LocalTime time) {
    int hour = time.getHour();
    int baseTransactions;

    // Determine base number of transactions based on time of day
    // Numbers are per store, per minute
    if (hour >= 10 && hour < 11) {
      baseTransactions = 10;
    } else if (hour >= 11 && hour < 13) {
      baseTransactions = 80;
    } else if (hour >= 13 && hour < 16) {
      baseTransactions = 20;
    } else if (hour >= 16 && hour < 19) {
      baseTransactions = 40;
    } else if (hour >= 19 && hour < 22) {
      baseTransactions = 14;
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

}
