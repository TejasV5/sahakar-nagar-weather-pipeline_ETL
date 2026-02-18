# Sahakar Nagar Hyperlocal Weather Pipeline 🌤️

## 📌 Project Overview
An automated, serverless End-to-End (E2E) data pipeline that monitors hyperlocal weather conditions in Sahakar Nagar, Bengaluru. The system ingests real-time telemetry from the Weather Union API, processes it using Python, and loads it into a Google BigQuery data warehouse for historical analysis and visualization.

**Live Dashboard:**  *(https://lookerstudio.google.com/reporting/80ca4b18-32a2-4e1f-b20d-53c845f13863)*

## 🏗️ Architecture
**Source (API)** ➡️ **Extraction (Python)** ➡️ **Automation (GitHub Actions)** ➡️ **Warehouse (BigQuery)** ➡️ **Viz (Looker Studio)**

## 🚀 Key Features
* **Serverless Automation:** Utilizes **GitHub Actions** to execute the ETL script on a CRON schedule (every 30 minutes), requiring zero local infrastructure.
* **Hybrid Data Strategy:** Merged **6 months of historical backfill data** (Excel/Static) with **live real-time streams** into a single unified schema.
* **Robust Error Handling:** Implemented schema enforcement (strict Float64 typing), timezone normalization (UTC conversion), and "Real-time" status tagging.
* **Dynamic Visualization:** Interactive Looker Studio dashboard featuring **Heat Risk Logic** (`CASE WHEN temp > 35 THEN 'Critical'`) and temporal trend analysis.

## 🛠️ Tech Stack
* **Language:** Python 3.9 (Pandas, Google-Cloud-BigQuery, Requests)
* **Cloud Data Warehouse:** Google BigQuery
* **Orchestration:** GitHub Actions (CI/CD YAML)
* **Visualization:** Google Looker Studio
* **Data Source:** Weather Union API (Zomato)

## 📂 Project Structure
├── .github/workflows/    # CI/CD Configuration (CRON Schedule)
├── ETL.py                # Main Extraction & Load Script
├── Back_fill.py          # One-time Historical Data Upload Script
├── requirements.txt      # Dependency Management
└── README.md             # Documentation

## ⚙️ How It Works
1.  **Trigger:** GitHub Actions wakes up a runner every 30 mins.
2.  **Extract:** `ETL.py` fetches JSON data from the API.
3.  **Transform:**
    * Normalizes timestamps to UTC.
    * Enforces Float types for `rain_intensity` and `wind_speed`.
    * Tags data as `"Real-time Ingestion"`.
4.  **Load:** Appends clean data to `market_data_staging.weather_logs` in BigQuery.
