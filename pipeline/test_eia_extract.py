from pipeline.extract.weather_extractor import fetch_weather_data

payload = fetch_weather_data(
    latitude=38.6275, longitude=92.5666, start_date="2026-02-04", end_date="2026-02-18"
)


print(payload.keys())
print(payload["daily"].keys())
print(payload["daily"]["time"][:3])
