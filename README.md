# acme demo

This is a data set and various data generators, samples, queries and configurations for the 'Acme' company demo. It is meant to emulate a data lakehouse pipeline. It has a Postgres database, CDC through Kafka, data generators, confluent schemas, dbt transforms, Athena queries, a Databricks notebook, and other moving parts.

This is based on a real-world scenario I encountered at a major retailer. They wanted to gather all retail point-of-sale (POS) data across thousands of stores in near-real time for instant analysis, particularly around the competitive holiday period.

#### Block diagram

<picture>
<img alt="Architecture diagram" src="https://github.com/corourke/acme/blob/main/doc/images/diagrams-architecture.png?raw=true">
</picture>

#### Tables

There are four main tables used by the demo.

1. Products are split into 32 different product categories that are contained in the `item_categories` table. This table is static.
2. The `stores` table represents 1000 stores across different timezones (that are used to generate activity based on the time of day) and for now remains static.
3. The `item_master` table starts off with about 32000 unique products. There is a continuously running data generator that creates new items, deletes old items, and updates prices. This allows us to show inserts, updates and deletes but at a relatively low volume. The data is updated in a Postgres database and ingested via CDC.
4. The `scans` table represents the point-of-sale register scans, and is fed by a continuously running high-velocity data generator that simulates activity at the 1000 stores and feeds the Kafka stream that is ingested into the data lake.

<picture>
<img alt="Table ER diagram" src="https://github.com/corourke/acme/blob/main/doc/images/diagrams-tables.png?raw=true">
</picture>

I'm working on a comprehensive and standalone setup guide. In the meantime, the <a href="https://github.com/corourke/acme/blob/1d030873fe3b34fdf62fbfecf17a6db049c0b7f1/doc/Onehouse_Postgres_CDC_Guide_2401.pdf">Onehouse PostgreSQL CDC Getting Started Guide will have to do.</a>

### Notes

1. The item master is random and simple. I'd like to improve it. A much better item master is available, for a price, at: https://crawlfeeds.com/datasets/target-products-dataset
