# Sahakar Nagar Weather ETL & ARIMA Time-Series Forecasting

## Project Overview
An automated, serverless data pipeline and machine learning architecture. The system ingests real-time hyperlocal weather telemetry, stores it in Google BigQuery, and trains a native ARIMA_PLUS time-series model to forecast future climate conditions with statistical confidence intervals.

**Live Dashboard:** [View on Looker Studio](https://lookerstudio.google.com/reporting/80ca4b18-32a2-4e1f-b20d-53c845f13863)

## Architecture
**API Telemetry** -> **Python ETL (GitHub Actions)** -> **BigQuery Data Warehouse** -> **BigQuery ML (ARIMA_PLUS)** -> **Looker Studio**

## Machine Learning & Predictive Analytics
* **Model Selection:** Deployed BigQuery ML `ARIMA_PLUS` for automated time-series forecasting, executing computation directly within the data warehouse to eliminate data egress.
* **Data Transformation:** Aggregated raw, irregular API timestamps into uniform hourly intervals using `TIMESTAMP_TRUNC(timestamp, HOUR)` to satisfy ARIMA auto-frequency constraints and eliminate duplicate epoch collisions.
* **Forecasting Horizon:** Generated rolling 48-hour forward projections (`ML.FORECAST`), computing projected temperature values alongside 90% statistical confidence envelopes (`prediction_interval_lower_bound`, `prediction_interval_upper_bound`).
* **Resource Optimization:** Leveraged BigQuery ML processing quotas by isolating model retraining cycles from high-frequency real-time data ingestion.

## Core Data Engineering Features
* **Serverless Automation:** Executed via GitHub Actions on a 30-minute CRON schedule, maintaining continuous integration with zero dedicated compute infrastructure.
* **Hybrid Data Unification:** Merged 6 months of historical static backfill data (250,000+ records) with live API streams via strict schema enforcement (Float64 type casting).
* **Defensive Ingestion:** Implemented UTC timezone normalization and programmatic null-value handling to prevent execution failures during API latency or empty JSON payloads.

## Tech Stack
* **Cloud Data Warehouse & ML:** Google BigQuery, BigQuery ML
* **Language:** Python 3.10 (Pandas, Google-Cloud-BigQuery, Requests)
* **Orchestration:** GitHub Actions (CI/CD YAML)
* **Visualization:** Google Looker Studio
* **Data Source:** Weather Union API (Zomato)

## Project Structure
├── .github/workflows/    # CI/CD Configuration (CRON Schedule)
├
├── ETL.py                # Main Extraction & Load Script
├
├── Back_fill.py          # Historical Data Upload Script
├
├── requirements.txt      # Dependency Management
├
└── README.md             # Documentation

## Execution Flow
1. **Trigger:** GitHub Actions initializes an Ubuntu runner every 30 minutes.
2. **Extract & Transform:** `ETL.py` fetches JSON data via injected GitHub Secrets, normalizes timestamps to UTC, and casts metrics to target schema types.
3. **Load:** Appends validated telemetry to `market_data_staging.weather_logs` in BigQuery.
4. **Aggregate & Train:** BigQuery ML processes raw telemetry into hourly averages, functioning as the input vector for ARIMA model training.
5. **Predict & Visualize:** `ML.FORECAST` materializes the 48-hour predictive intervals into a discrete table. Looker Studio renders this projection alongside real-time heat risk classifications (`CASE WHEN temp >= 32`).
