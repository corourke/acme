# Kafka CSV Uploader

This is a start on a program to upload CSV files to a Kafka topic,
one CSV row per message.

It will take a CSV file, an AVRO file containing a schema for the CSV file,
and a topic name as arguments. Kafka configuration is in `config.properties`.
The AVRO field names must match the CSV header row names 1:1.

This program is not finished.
