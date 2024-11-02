package acme.objects;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import org.json.JSONObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Transaction {
  private final UUID scanId;
  private final int storeId;
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
  private final LocalDateTime scanDatetime;
  private final String itemUPC;
  private final int unitQty;
  private final BigDecimal unitPrice;

  // Parameterized constructor with annotations
  @JsonCreator
  public Transaction(
      @JsonProperty("store_id") int storeId,
      @JsonProperty("item_upc") String itemUPC,
      @JsonProperty("unit_qty") int unitQty,
      @JsonProperty("scan_datetime") LocalDateTime scanDatetime,
      @JsonProperty("scan_id") UUID scanId,
      @JsonProperty("unit_price") BigDecimal unitPrice) {
    this.storeId = storeId;
    this.itemUPC = itemUPC;
    this.unitQty = unitQty;
    this.scanDatetime = scanDatetime;
    this.scanId = scanId;
    this.unitPrice = unitPrice;
  }

  public static Transaction fromJson(String jsonString) {
    JSONObject jsonObject = new JSONObject(jsonString);

    int storeId = jsonObject.getInt("store_id");
    String itemUPC = jsonObject.getString("item_upc");
    int unitQty = jsonObject.getInt("unit_qty");
    String scanDatetimeStr = jsonObject.getString("scan_datetime");
    LocalDateTime scanDatetime = LocalDateTime.parse(scanDatetimeStr,
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    UUID scanId = UUID.fromString(jsonObject.getString("scan_id"));
    BigDecimal unitPrice = BigDecimal.valueOf(jsonObject.getDouble("unit_price"));

    return new Transaction(storeId, itemUPC, unitQty, scanDatetime, scanId, unitPrice);
  }

  // Method to convert Transaction to JSON String
  public String toJSON(Boolean isDeleted) {
    JSONObject json = new JSONObject();
    json.put("scan_id", this.getScanId().toString());
    json.put("store_id", this.getStoreId());
    json.put("scan_datetime", this.getScanDatetime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    json.put("item_upc", this.getItemUPC());
    json.put("unit_qty", this.getUnitQty());
    json.put("unit_price", this.getUnitPrice());
    if (isDeleted != null) {
      json.put("_hoodie_is_deleted", isDeleted);
    }
    return json.toString();
  }

  public UUID getScanId() {
    return scanId;
  }

  public int getStoreId() {
    return storeId;
  }

  public LocalDateTime getScanDatetime() {
    return scanDatetime;
  }

  public String getItemUPC() {
    return itemUPC;
  }

  public int getUnitQty() {
    return unitQty;
  }

  public BigDecimal getUnitPrice() {
    return unitPrice;
  }

}
