CREATE TABLE IF NOT EXISTS energy_consumption (
  timestamp TIMESTAMP,
  region TEXT,
  demand_mw NUMERIC,
  source TEXT,
  ingested_at TIMESTAMP,
  UNIQUE (timestamp, region)
);

CREATE TABLE IF NOT EXISTS weather_daily (
  date DATE,
  temperature_2m_max NUMERIC,
  temperature_2m_min NUMERIC,
  rain_sum NUMERIC,
  showers_sum NUMERIC,
  snowfall_sum NUMERIC,
  UNIQUE (date)
);

CREATE TABLE IF NOT EXISTS fact_energy_weather (
  timestamp TIMESTAMP,
  region TEXT,
  demand_mw NUMERIC,
  temperature_2m_max NUMERIC,
  temperature_2m_min NUMERIC,
  rain_sum NUMERIC,
  showers_sum NUMERIC,
  snowfall_sum NUMERIC,
  ingested_at TIMESTAMP,
  UNIQUE (timestamp, region)
);

CREATE TABLE IF NOT EXISTs pipeline_runs (
  run_id SERIAL PRIMARY KEY,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  status TEXT,
  error_message TEXT
);