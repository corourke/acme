package acme.objects;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import org.json.JSONObject;

public class InventoryTransaction {
    private final UUID trxId;
    private final String trxType;
    private final LocalDate invDate;
    private final int storeId;
    private final int itemId;
    private final int unitQty;
    private final LocalDateTime trxTimestamp;

    // Constructor for InventoryTransaction with automatic UUID and timestamp
    // generation
    public InventoryTransaction(String trxType, String invDateString, int storeId, int itemId, int unitQty) {
        this.trxId = UUID.randomUUID(); // Generate a new UUID
        this.trxType = trxType;
        this.invDate = LocalDate.parse(invDateString, DateTimeFormatter.ofPattern("yyyy-MM-dd")); // Parse the date
        this.storeId = storeId;
        this.itemId = itemId;
        this.unitQty = unitQty;
        this.trxTimestamp = LocalDateTime.now(); // Set the current timestamp
    }

    // Constructor for InventoryTransaction
    public InventoryTransaction(UUID trxId, String trxType, LocalDate invDate, int storeId, int itemId, int unitQty,
            LocalDateTime trxTimestamp) {
        this.trxId = trxId;
        this.trxType = trxType;
        this.invDate = invDate;
        this.storeId = storeId;
        this.itemId = itemId;
        this.unitQty = unitQty;
        this.trxTimestamp = trxTimestamp;
    }

    // Method to convert Transaction to JSON String
    public String toJson(Boolean isDeleted) {
        JSONObject json = new JSONObject();
        json.put("trx_id", this.getTrxId().toString());
        json.put("trx_type", this.getTrxType());
        json.put("inv_date", this.getInvDate().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        json.put("store_id", this.getStoreId());
        json.put("item_id", this.getItemId());
        json.put("unit_qty", this.getUnitQty());
        json.put("trx_timestamp", this.getTrxTimestamp().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        if (isDeleted != null) {
            json.put("_hoodie_is_deleted", isDeleted);
        }
        return json.toString();
    }

    // Getters for each field
    public UUID getTrxId() {
        return trxId;
    }

    public String getTrxType() {
        return trxType;
    }

    public LocalDate getInvDate() {
        return invDate;
    }

    public int getStoreId() {
        return storeId;
    }

    public int getItemId() {
        return itemId;
    }

    public int getUnitQty() {
        return unitQty;
    }

    public LocalDateTime getTrxTimestamp() {
        return trxTimestamp;
    }
}