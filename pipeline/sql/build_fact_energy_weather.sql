
INSERT INTO fact_energy_weather (
  timestamp,
  region,
  demand_mw,
  temperature_2m_max,
  temperature_2m_min,
  rain_sum,
  showers_sum,
  snowfall_sum,
  ingested_at
)

SELECT
  e.timestamp,
  e.region,
  e.demand_mw,
  w.temperature_2m_max,
  w.temperature_2m_min,
  w.rain_sum,
  w.showers_sum,
  w.snowfall_sum,
  NOW()
FROM energy_consumption e
LEFT JOIN weather_daily w
ON DATE(e.timestamp) = w.date
ON CONFLICT (timestamp,region) DO NOTHING;