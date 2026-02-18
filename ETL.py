import os
import pandas as pd
import requests
from google.cloud import bigquery
import json

# --- DEBUGGING START ---
print("🚀 Starting ETL Pipeline...")

# 1. SETUP AUTHENTICATION
if "GCP_KEYS_JSON" in os.environ:
    print("✅ Found GCP_KEYS_JSON secret.")
    try:
        # Create the key file
        with open("google_key.json", "w") as f:
            f.write(os.environ["GCP_KEYS_JSON"])
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "google_key.json"
        print("✅ Google Key File created successfully.")
    except Exception as e:
        print(f"❌ Error creating key file: {e}")
        exit(1)
else:
    print("❌ CRITICAL ERROR: GCP_KEYS_JSON secret is missing!")
    exit(1)

# 2. SETUP API KEY
WEATHER_API_KEY = os.environ.get("WEATHER_API_KEY")
if not WEATHER_API_KEY:
    print("❌ CRITICAL ERROR: WEATHER_API_KEY secret is missing!")
    exit(1)
else:
    # Print first 4 chars only to verify it's loaded without leaking it
    print(f"✅ Weather API Key loaded (Starts with: {WEATHER_API_KEY[:4]}...)")

# 3. SETUP PROJECT ID (If you made it a secret)
PROJECT_ID = os.environ.get("PROJECT_ID", "personal-data-pipeline-487807")
print(f"✅ Using Project ID: {PROJECT_ID}")

TABLE_ID = f"{PROJECT_ID}.market_data_staging.weather_logs"
LAT = 13.0651
LON = 77.5842

# --- MAIN LOGIC ---
def run_weather_etl():
    print("📡 Fetching data from Weather Union...")
    url = "https://www.weatherunion.com/gw/weather/external/v0/get_weather_data"
    headers = {"x-zomato-api-key": WEATHER_API_KEY}
    params = {"latitude": LAT, "longitude": LON}

    try:
        response = requests.get(url, headers=headers, params=params)
        print(f"   Response Status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"❌ API Error: {response.text}")
            exit(1)
            
        raw_data = response.json()
        print("✅ Data received from API.")

    except Exception as e:
        print(f"❌ Network Error: {e}")
        exit(1)

    # Transform
    try:
        weather_metrics = raw_data.get("locality_weather_data", {})
        if not weather_metrics:
            print("❌ Error: 'locality_weather_data' is empty.")
            exit(1)
            
        weather_metrics['timestamp'] = pd.Timestamp.now(tz='UTC')
        weather_metrics['status_message'] = "Real-time Ingestion"
        
        df = pd.DataFrame([weather_metrics])
        
        # Schema Alignment
        expected_cols = ['timestamp', 'temperature', 'humidity', 'wind_speed', 
                         'wind_direction', 'rain_intensity', 'rain_accumulation', 'status_message']
        
        for col in expected_cols:
            if col not in df.columns:
                df[col] = 0.0 if col != 'status_message' else "OK"
                
        df = df[expected_cols]
        print("✅ Data transformation complete.")
        
    except Exception as e:
        print(f"❌ Transformation Error: {e}")
        exit(1)

    # Load to BigQuery
    print(f"📤 Uploading to BigQuery table: {TABLE_ID}...")
    try:
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema_update_options=["ALLOW_FIELD_ADDITION"],
            schema=[
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                bigquery.SchemaField("temperature", "FLOAT"),
                bigquery.SchemaField("humidity", "FLOAT"),
                bigquery.SchemaField("wind_speed", "FLOAT"),
                bigquery.SchemaField("wind_direction", "FLOAT"),
                bigquery.SchemaField("rain_intensity", "FLOAT"),
                bigquery.SchemaField("rain_accumulation", "FLOAT"),
                bigquery.SchemaField("status_message", "STRING"),
            ],
        )
        job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
        job.result()
        print("✅ SUCCESS! Data uploaded to BigQuery.")
        
    except Exception as e:
        print(f"❌ BigQuery Upload Error: {e}")
        exit(1)

if __name__ == "__main__":
    run_weather_etl()
