package acme.objects;

import java.math.BigDecimal;

public class Product {
  private final int itemId;
  private final BigDecimal itemPrice;
  private final String itemUPC;
  private final int repl_qty;

  // Constructor
  public Product(int itemId, BigDecimal itemPrice, String itemUPC, int repl_qty) {
    this.itemId = itemId;
    this.itemPrice = itemPrice;
    this.itemUPC = itemUPC;
    this.repl_qty = repl_qty;
  }

  public int getItemId() {
    return itemId;
  }

  public BigDecimal getItemPrice() {
    return itemPrice;
  }

  public String getItemUPC() {
    return itemUPC;
  }

  public int getRepl_qty() {
    return repl_qty;
  }
}
