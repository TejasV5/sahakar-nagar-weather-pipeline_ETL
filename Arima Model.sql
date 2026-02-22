CREATE OR REPLACE MODEL `personal-data-pipeline-487807.market_data_staging.temperature_forecast_model`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='hour_timestamp',
  time_series_data_col='avg_temperature',
  data_frequency='AUTO_FREQUENCY'
) AS
SELECT
  TIMESTAMP_TRUNC(timestamp, HOUR) AS hour_timestamp,
  AVG(temperature) AS avg_temperature
FROM `personal-data-pipeline-487807.market_data_staging.weather_logs`
WHERE temperature IS NOT NULL
GROUP BY hour_timestamp;