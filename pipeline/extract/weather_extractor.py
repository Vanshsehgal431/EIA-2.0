from config.settings import REQUEST_TIMEOUT, WEATHER_BASE_URL
from pipeline.utils.api import call_api
from pipeline.utils.logger import setup_logger

logger = setup_logger(__name__)


def fetch_weather_data(latitude: float, longitude: float, start_date, end_date):
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": (
            "temperature_2m_max,"
            "temperature_2m_min,"
            "rain_sum,"
            "showers_sum,"
            "snowfall_sum"
        ),
        "timezone": "auto",
        "start_date": start_date,
        "end_date": end_date,
    }

    logger.info(f"Fetching weather data for lat = {latitude}, lon = {longitude}")

    return call_api(WEATHER_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
