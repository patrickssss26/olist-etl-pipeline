# Olist E-commerce ETL Pipeline

ETL pipeline processing 100K Brazilian e-commerce orders on GCP with Airflow orchestration and Power BI analytics.



## Tech Stack

GCP (BigQuery, Cloud Storage) • Apache Airflow • Python • Power BI • Docker

## What It Does

- **Extract**: Load CSV files from GCS
- **Transform**: Clean data, sentiment analysis on reviews
- **Load**: Push to BigQuery, visualize in Power BI
- **Orchestrate**: Airflow DAG runs daily

## Results

- 99K orders, 33K products processed
- R$13.59M revenue analyzed
- 97% delivery success rate
- Automated sentiment classification

## Architecture
```
Kaggle CSV → GCS → Python/Pandas → BigQuery → Power BI
                      ↓
                  Airflow (Daily)
```

## Quick Start
```bash
# Setup
pip install -r requirements.txt
gcloud auth application-default login

# Run ETL
python scripts/transform_olist_data.py

# Start Airflow
docker-compose up -d
```

## Skills

Cloud ETL • Data Warehousing • Orchestration • BI Dashboards • Python • SQL

---
