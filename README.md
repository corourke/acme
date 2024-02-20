# acme demo

For various data management and analytics demonstrations, I've been building up this ACME company dataset and various data generators and utilities. I'm going to start cleaning it up and placing it here.

Block diagram:
<picture>

  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/corourke/acme/blob/main/doc/images/diagrams-architecture-dark.png?raw=true">
  <source media="(prefers-color-scheme: light)" srcset="https://github.com/corourke/acme/blob/main/doc/images/diagrams-architecture.png?raw=true">
  <img alt="Architecture diagram" src="https://github.com/corourke/acme/blob/main/doc/images/diagrams-architecture.png?raw=true">
</picture>

Tables:
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/corourke/acme/blob/main/doc/images/diagrams-tables-dark.png?raw=true">
  <source media="(prefers-color-scheme: light)" srcset="https://github.com/corourke/acme/blob/main/doc/images/diagrams-tables.png?raw=true">
  <img alt="Architecture diagram" src="https://github.com/corourke/acme/blob/main/doc/images/diagrams-tables.png?raw=true">
</picture>

Note that the `batched_scans` table is to be generated with a data generator.

### Notes

1. The item master is random and simple. I'd like to improve it. A much better item master is available, for a price, at: https://crawlfeeds.com/datasets/target-products-dataset
