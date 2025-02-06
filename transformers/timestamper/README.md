# Timestamper transformer

### Install

Upload the `timestamper-0.2.jar` file to Onehouse, Settings -> Integrations -> Manage JARs.

### Usage

Add the transformer to a stream and add the following two properties:

timestamp.column The new column name to contain the timestamp
timestamp.granularity One of: "millisecond", "second", "minute", "hour", "day", "week", "month", "quarter"or "year"
