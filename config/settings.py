import os

from dotenv import load_dotenv

load_dotenv()

EIA_API_KEY = os.getenv("EIA_API_KEY")
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")

REQUEST_TIMEOUT = 30

EIA_BASE_URL = "https://api.eia.gov/v2/electricity/rto/daily-region-sub-ba-data/data/"
WEATHER_BASE_URL = "https://api.open-meteo.com/v1/forecast"

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}
