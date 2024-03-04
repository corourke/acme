## Retail Point of Sale Transaction Generator

A data generator written in Java that simulates point-of-sale transactions. It outputs batches of transactions as a JSON payload suitable for sending to a Kafka cluster.

The purpose of this program is to simulate point-of-sale (checkout) transactions in large quantities for analytical processing. It is based on a real-world scenario with a large retailer that has hundreds of stores throughout the US. The data team needed to bring near real-time sales data together for immediate analysis around the holidays in a pricing 'war room' situation.

To start:

```bash
mvn package
mvn exec:java
```
