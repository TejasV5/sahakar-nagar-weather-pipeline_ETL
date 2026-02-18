import os
import pandas as pd
import requests
from google.cloud import bigquery


if "GCP_KEYS_JSON" in os.environ:
    with open("google_key.json", "w") as f:
        f.write(os.environ["GCP_KEYS_JSON"])
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "google_key.json"
else:
    raise ValueError("❌ Error: GCP_KEYS_JSON secret is missing. This script only runs on GitHub.")


WEATHER_API_KEY = os.environ.get("WEATHER_API_KEY")

if not WEATHER_API_KEY:
    raise ValueError("❌ Error: WEATHER_API_KEY secret is missing.")


LAT = 13.0651  # Sahakar Nagar
LON = 77.5842
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
TABLE_ID = f"{PROJECT_ID}.market_data_staging.weather_logs"

def run_weather_etl():
    print("--- Starting ETL Pipeline ---")
    
   
    url = "https://www.weatherunion.com/gw/weather/external/v0/get_weather_data"
    headers = {"x-zomato-api-key": WEATHER_API_KEY}
    params = {"latitude": LAT, "longitude": LON}
    
    try:
        response = requests.get(url, headers=headers, params=params)
    except Exception as e:
        print(f"Network Error: {e}")
        return
    
    if response.status_code != 200:
        print(f"API Error {response.status_code}: {response.text}")
        return

    raw_data = response.json()
    

    weather_metrics = raw_data.get("locality_weather_data", {})
    
    
    if not weather_metrics or weather_metrics.get("temperature") is None:
        print(f"Skipping Load: Data is null. API Message: {raw_data.get('message')}")
        return

   
    weather_metrics['timestamp'] = pd.Timestamp.now(tz='UTC')   
    weather_metrics['status_message'] = "Real-time Ingestion"

   
    df = pd.DataFrame([weather_metrics])

   
    expected_cols = [
        'timestamp', 
        'temperature', 
        'humidity', 
        'wind_speed', 
        'wind_direction', 
        'rain_intensity', 
        'rain_accumulation', 
        'status_message'
    ]
    
   
    for col in expected_cols:
        if col not in df.columns:
            if col == 'status_message':
                df[col] = "OK"
            elif col == 'timestamp':
                df[col] = pd.Timestamp.now(tz='UTC')
            else:
                df[col] = 0.0 

    
    float_cols = ['temperature', 'humidity', 'wind_speed', 'wind_direction', 'rain_intensity', 'rain_accumulation']
    for col in float_cols:
        df[col] = df[col].astype(float)

   
    df = df[expected_cols]

    print(f"Data Prepared: {df['temperature'][0]}°C at {df['timestamp'][0]}")

   
    client = bigquery.Client()
    
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
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

    try:
        job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
        job.result()  
        print(f"✅ Successfully loaded data to {TABLE_ID}")
    except Exception as e:
        print(f"❌ BigQuery Load Error: {e}")
        if hasattr(e, 'errors'):
            print("Details:", e.errors)

if __name__ == "__main__":
    run_weather_etl()