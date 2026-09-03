# CDC-Data-Pipeline
End-to-end data engineering pipeline capturing SQL Server changes with Debezium and Kafka, processing CDC events in Databricks, and maintaining historical data with SCD Type 2



# SQL Server CDC Pipeline with Debezium, Kafka & Databricks

## Overview

This project demonstrates an end-to-end Change Data Capture (CDC) pipeline that captures changes from SQL Server, streams those changes through Apache Kafka using Debezium, and processes the CDC events in Databricks.

The pipeline maintains historical customer records using Slowly Changing Dimension Type 2 (SCD Type 2) in Delta Lake.

## Architecture

<img width="346" height="347" alt="image" src="https://github.com/user-attachments/assets/893925f7-d659-472e-9608-d401ba9f364b" />


## Technologies

- SQL Server
- SQL Server CDC
- Debezium
- Apache Kafka
- Python
- Databricks
- Apache Spark
- Delta Lake
- SQL
- Docker

## Pipeline Components

### 1. SQL Server

The source system contains the `MichaelN_Retail` database and
`RetailCustomer` table.

SQL Server Change Data Capture tracks inserts and updates to the
source table.

### 2. Debezium

Debezium captures changes from SQL Server CDC and publishes them
as CDC events to Kafka.

Example Kafka topic:

`retail.MichaelN_Retail.dbo.RetailCustomer`

### 3. Kafka

Kafka provides the event streaming layer between SQL Server and
the downstream processing environment.

The Python CDC bridge consumes messages using the consumer group:

`retail-databricks-etl`

Kafka offsets are manually committed only after successful
delivery to Databricks.

### 4. Python CDC Bridge

The Python bridge consumes Debezium events from Kafka, batches
the events, and uploads them to Databricks Bronze as JSON.

The bridge uses a batch size of 10 records or a 10-second timeout.

The bridge follows an at-least-once processing approach by
committing Kafka offsets only after the Bronze upload succeeds.

### 5. Databricks Bronze

The raw Debezium events are landed in Databricks without
performing business transformations.

The Bronze layer preserves CDC information such as:

- `before`
- `after`
- `op`
- `snapshot`
- `change_lsn`
- `commit_lsn`
- CDC timestamp

### 6. Databricks Silver

The Bronze events are transformed into structured customer
changes.

The pipeline:

- Filters relevant CDC operations
- Extracts customer attributes
- Converts CDC timestamps
- Identifies the latest change for each customer within a batch

### 7. SCD Type 2

The Silver layer maintains historical versions of customer records.

When a customer changes, the previous record is closed and a new
current version is inserted.

Example:

| CustomerID | Location | ValidFrom | ValidTo | IsCurrent |
|------------|----------|-----------|---------|-----------|
| 8 | Rhode Island | ... | ... | false |
| 8 | Wyoming | ... | ... | false |
| 8 | Florida | ... | NULL | true |

This allows historical customer changes to be preserved rather
than overwritten.

## Example CDC Event

A customer location update produces a Debezium event containing
the previous and new values:

```json
{
  "before": {
    "CustomerID": 8,
    "Location": "Wyoming"
  },
  "after": {
    "CustomerID": 8,
    "Location": "Florida"
  },
  "op": "u"
}
