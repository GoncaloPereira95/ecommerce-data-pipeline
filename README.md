# Ecommerce Data Pipeline — Real-time Kappa Architecture

> Group project built for a Big Data Architecture course. Real-time streaming pipeline for a fictional e-commerce platform using Kafka, Spark Structured Streaming, PyDeequ, InfluxDB and Grafana.

---

## What Was Built

A real-time data pipeline handling order events from ingestion to observability:

- **Kafka** ingests order, click and inventory events
- **Spark Structured Streaming** processes the stream — enrichment, data quality gate, fraud scoring
- **PyDeequ** runs 12 data quality checks on every batch, producing a scored report
- **PostgreSQL** stores orders, daily sales and top products
- **Redis** serves as hot cache and fraud blocklist
- **InfluxDB** collects pipeline metrics (throughput, latency, error rate, quality score)
- **Grafana** visualises the pipeline in real-time

---

## Tech Stack

| Layer | Tool |
|---|---|
| Event Streaming | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| Data Quality | PySpark + PyDeequ |
| Storage | PostgreSQL + Redis |
| Metrics | InfluxDB |
| Observability | Grafana |

---

## Data Quality

12 constraints across 5 dimensions implemented with PyDeequ:

| Dimension | Checks |
|---|---|
| Completeness | order_id, customer_id, product_id not null |
| Uniqueness | order_id unique |
| Validity | quantity in [1,50], unit_price in (0,10000], total ≥ 0 |
| Consistency | total_amount = quantity × unit_price, status and payment_method in allow-lists |
| Timeliness | order_date not in future, within last 5 years |

**Latest run: 100% score — GREEN gate (12/12 passed)**

---

## Observability Dashboard

![Grafana Dashboard](images/grafana_dashboard.png)

Live Grafana dashboard monitoring:
- Throughput by component (records/s)
- Latency by component (ms)
- Data quality score (gauge)
- Component summary table (last 1h)

---

## Repository Structure

```
ecommerce-data-pipeline/
|
|-- data_quality.ipynb          # PySpark + PyDeequ data quality notebook
|-- seed_metrics.py             # Seeds pipeline metrics into InfluxDB
|-- grafana_dashboard.json      # Grafana dashboard (importable)
|
|-- quality_reports/
|   |-- dq_results.json         # Latest DQ run output
|
|-- images/
|   |-- grafana_dashboard.png   # Live dashboard screenshot
```

---

## How to Run

**Prerequisites:** Docker, Python 3.8+, PySpark, PyDeequ

**1. Start the stack**
```bash
docker compose up -d kafka influxdb grafana
```

**2. Seed pipeline metrics into InfluxDB**
```bash
python seed_metrics.py --hours 24 --url http://localhost:8086
```

**3. Import the Grafana dashboard**
- Open Grafana at `http://localhost:3000`
- Dashboards → Import → upload `grafana_dashboard.json`

**4. Run the data quality notebook**
```bash
jupyter notebook data_quality.ipynb
```
