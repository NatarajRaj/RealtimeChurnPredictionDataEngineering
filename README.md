# Real-Time Customer Churn Prediction Pipeline

This project simulates and processes real-time customer churn data using Kafka, Spark Structured Streaming, Delta Lake, and visualizes insights using Matplotlib.

---

## 🚀 Pipeline Overview

1. **Data Generation (Python)**
   - Random generation of customer profiles, app usage, transactions, and support tickets.
   - Publishes JSON data to Kafka Bronze topics.

2. **Streaming ETL (Spark Structured Streaming)**
   - Reads from Kafka Bronze → applies transformations → writes to Kafka Silver & Delta Lake (local).
   - Silver data includes cleaned and structured DataFrames with schemas.

3. **Business Aggregations (Gold Layer)**
   - Calculates KPIs:
     - ARPU (Average Revenue Per User)
     - App behavior (login gap, session count)
     - Support sentiment score & resolution time
   - Stored in Delta format and published to Kafka.

4. **Visualization**
   - Uses Matplotlib to visualize trends like churn rate, resolution time, etc.

---

## 📁 Tech Stack

- **Apache Kafka**: Event streaming
- **Apache Spark (Structured Streaming)**: Real-time processing
- **Delta Lake**: Storage and time travel
- **Matplotlib**: Visualization
- **Python**: Data generation and orchestration
- **Local Disk**: Silver/Gold storage
- **Kafka Topics**: `bronze_topic`, `silver_topic`, `gold_topic`

---

## 📸 Architecture Diagram

![Pipeline Architecture](docs/pipeline_diagram.png)

> Note: Add `docs/pipeline_diagram.png` to your repo.

---

## 🛠️ Run Locally

1. Start Kafka (using Docker or local)
2. Run `data_generator.py` to simulate and publish data
3. Run `silver_streaming_job.py` to process Bronze → Silver
4. Run `gold_aggregator.py` to produce Gold metrics
5. View Delta tables or visualize using `visualize.py`

---

## 📬 Output Example (Gold Layer)

| customer_id | name | arpu | avg_resolution_time | avg_ticket_sentiment | 
| ------------ | ---- | ---- | --------------------- | ------------------| 
| C123         | John | 35.2 | 2.3                   | 0.78               
