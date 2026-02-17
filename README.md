# Smart Manufacturing Command Center

## Overview

An end-to-end data pipeline and analytics platform that integrates e-commerce transactional data with IoT sensor telemetry for manufacturing intelligence. The system ingests data from multiple sources into PostgreSQL, orchestrates event streaming through Apache Kafka, and runs inside a fully containerized Docker environment.

## Architecture

```
Data Sources                      Infrastructure                    Analytics Layer
+------------------------+       +-------------------------+       +------------------+
| Olist E-Commerce Data  |       |                         |       |                  |
| - olist_order_items    |--ETL->| PostgreSQL 13           |------>| Analysis.ipynb   |
| - olist_orders         |       | (sap_sales_flat table)  |       | (Pandas + SQL)   |
| - olist_products       |       | (iot_sensors table)     |       |                  |
+------------------------+       +-------------------------+       +------------------+
                                          |
+------------------------+       +-------------------------+
| UCI AI4I 2020          |       |                         |
| Predictive Maintenance |--ETL->| Apache Kafka            |
| IoT Sensor Dataset     |       | (Confluent cp-kafka)    |
+------------------------+       | + Zookeeper             |
                                 +-------------------------+
```

## Data Sources

### E-Commerce Sales Data (Olist)

The pipeline merges three Olist open datasets into a unified sales table:

| Source File | Contents |
|-------------|----------|
| `olist_order_items` | Line items per order |
| `olist_orders` | Order metadata and timestamps |
| `olist_products` | Product catalog and categories |

**Result:** 112,650 records loaded into `sap_sales_flat` table with SAP-style column naming:
- `VBELN` (Order ID), `AUDAT` (Order Date), `MATNR` (Product ID), `NETWR` (Revenue), `CATEGORY` (Product Category), `order_status`

### IoT Sensor Data (UCI AI4I 2020 Predictive Maintenance)

Real-time manufacturing sensor data loaded into `iot_sensors` table:

| Feature | Description |
|---------|-------------|
| Machine_Type | Machine classification |
| Air_Temp | Ambient air temperature |
| Process_Temp | Active process temperature |
| Rotational_Speed | RPM measurement |
| Torque | Torque reading (Nm) |
| Tool_Wear | Cumulative tool wear (minutes) |
| Failure_Label | Binary failure indicator |

Synthetic timestamps are generated at 5-minute intervals to simulate real-time ingestion.

## Infrastructure

The system runs on a Docker Compose stack with three services:

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| PostgreSQL 13 | `postgres:13` | 5432 | Primary data warehouse |
| Apache Kafka | `confluentinc/cp-kafka:7.3.0` | 9092 | Event streaming |
| Zookeeper | `confluentinc/cp-zookeeper:7.3.0` | 2181 | Kafka coordination |

## ETL Pipeline (ingest_data.py)

The ingestion script performs the following steps:

1. Downloads Olist CSV files from remote URLs
2. Merges order items, orders, and products on shared keys
3. Renames columns to SAP-style naming conventions
4. Loads the merged dataframe into PostgreSQL via SQLAlchemy
5. Downloads the UCI AI4I 2020 dataset (ZIP archive)
6. Extracts sensor data, renames columns, adds synthetic timestamps
7. Loads sensor data into a separate PostgreSQL table
8. Validates record counts for both tables

## Tech Stack

- **Languages:** Python
- **Database:** PostgreSQL 13
- **Streaming:** Apache Kafka (Confluent), Zookeeper
- **Containerization:** Docker, Docker Compose
- **Libraries:** Pandas, NumPy, SQLAlchemy, python-dotenv, Requests
- **Data Sources:** Olist E-Commerce Dataset, UCI AI4I 2020 Predictive Maintenance Dataset

## Project Structure

```
AI_Manufacturing/
├── Analysis.ipynb                          # Data analysis and visualization notebook
├── Data/
│   ├── ingest_data.py                      # ETL pipeline script
│   └── requirements.txt                    # Python dependencies
├── infrastructure/
│   └── docker/
│       └── docker-compose.yaml             # PostgreSQL + Kafka + Zookeeper
├── .env                                    # Database credentials (not committed)
└── README.md
```

## Getting Started

```bash
# Start infrastructure
cd infrastructure/docker
docker-compose up -d

# Install Python dependencies
cd ../../Data
pip install -r requirements.txt

# Run the data ingestion pipeline
python ingest_data.py

# Open analysis notebook
jupyter notebook Analysis.ipynb
```

## License

MIT
