# Market Analytics Platform

## Setup

## Software Solutions
- Docker
- Apache Kafka

## Custom Software
- Ingestion: Contains the source files for a consumer that:
    - Automatically connects to *Yahoo finance* and subscribes to latest pricing data
    - Automatically scales based off of tickers placed in `subscription.yml`
    - Writes market data to Kafka for retention

- Storage:
    - Scrapes daily data from Kafka
    - Turns it into a dataframe
    - Stores it as parquet for fast DuckDB OLAP workloads

- Analytics: 