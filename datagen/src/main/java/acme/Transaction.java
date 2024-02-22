package acme;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

public class Transaction {
  private final UUID scanId;
  private final int storeId;
  private final LocalDateTime scanDatetime;
  private final String itemUPC;
  private final int unitQty;
  private final BigDecimal unitPrice;

  // Constructor for Transaction
  public Transaction(UUID scanId, int storeId, LocalDateTime scanDatetime, String itemUPC, int unitQty,
      BigDecimal unitPrice) {
    this.scanId = scanId;
    this.storeId = storeId;
    this.scanDatetime = scanDatetime;
    this.itemUPC = itemUPC;
    this.unitQty = unitQty;
    this.unitPrice = unitPrice;
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
