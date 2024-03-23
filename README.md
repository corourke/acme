# acme demo

This is a data set and various data generators, samples, queries and configurations for the 'Acme' company demo. It is meant to emulate a data lakehouse pipeline. It has a Postgres database, CDC through Kafka, data generators, confluent schemas, dbt transforms, Athena queries, a Databricks notebook, and other moving parts.

This is based on a real-world scenario I encountered at a major retailer. They wanted gather all retail point-of-sale (POS) data across thousands of stores in near-real time for instant analysis, particularly around the competitive holiday period.

Block diagram:
<picture>
<img alt="Architecture diagram" src="https://github.com/corourke/acme/blob/main/doc/images/diagrams-architecture.png?raw=true">
</picture>

Tables:
<picture>
<img alt="Table ER diagram" src="https://github.com/corourke/acme/blob/main/doc/images/diagrams-tables.png?raw=true">
</picture>

Note that the `batched_scans` table is to be generated with a data generator, and the `sales_summary` table is to be created with stream processing.

I'm working on a comprehensive and standalone setup guide. In the meantime, the <a href="https://github.com/corourke/acme/blob/1d030873fe3b34fdf62fbfecf17a6db049c0b7f1/doc/Onehouse_Postgres_CDC_Guide_2401.pdf">Onehouse PostgreSQL CDC Getting Started Guide will have to do.</a>

### Notes

1. The item master is random and simple. I'd like to improve it. A much better item master is available, for a price, at: https://crawlfeeds.com/datasets/target-products-dataset
