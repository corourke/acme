package acme.objects;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.json.JSONObject;

public class InventoryCount {
  private final int itemId;
  private final int storeId;
  private int qtyInStock;
  private int qtyOnOrder;
  private LocalDateTime qtyLastUpdated;

  // Constructor
  public InventoryCount(int itemId, int storeId, int qty_in_stock, int qty_on_order) {
    this.itemId = itemId;
    this.storeId = storeId;
    this.qtyInStock = qty_in_stock;
    this.qtyOnOrder = qty_on_order;
    this.qtyLastUpdated = LocalDateTime.now();
  }

  // Method to convert InventoryCount to JSON String
  public String toJson(Boolean isDeleted) {
    JSONObject json = new JSONObject();
    json.put("item_id", this.getItemId());
    json.put("store_id", this.getStoreId());
    json.put("qty_in_stock", this.getQtyInStock());
    json.put("qty_on_order", this.getQtyOnOrder());
    json.put("qty_last_updated", this.getQtyLastUpdated().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    if (isDeleted != null) {
      json.put("_hoodie_is_deleted", isDeleted);
    }
    return json.toString();
  }

  public int getItemId() {
    return itemId;
  }

  public int getStoreId() {
    return storeId;
  }

  public int getQtyInStock() {
    return qtyInStock;
  }

  public void setQtyInStock(int qty_in_stock) {
    this.qtyInStock = qty_in_stock;
    this.qtyLastUpdated = LocalDateTime.now();
  }

  public int getQtyOnOrder() {
    return qtyOnOrder;
  }

  public void setQtyOnOrder(int qty_on_order) {
    this.qtyOnOrder = qty_on_order;
    this.qtyLastUpdated = LocalDateTime.now();
  }

  public LocalDateTime getQtyLastUpdated() {
    return qtyLastUpdated;
  }
}
