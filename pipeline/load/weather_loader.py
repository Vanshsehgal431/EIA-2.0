import psycopg2
from psycopg2.extras import execute_batch

from config.settings import POSTGRES_CONFIG
from pipeline.utils.logger import setup_logger

logger = setup_logger(__name__)


def load_weather_dataframe(df):
    logger.info("Loading weather data into weather_daily")

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    query = """
        INSERT INTO weather_daily (
            date,
            temperature_2m_max,
            temperature_2m_min,
            rain_sum,
            showers_sum,
            snowfall_sum
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING;
    """

    records = [
        (
            row.date,
            row.temperature_2m_max,
            row.temperature_2m_min,
            row.rain_sum,
            row.showers_sum,
            row.snowfall_sum,
        )
        for row in df.itertuples()
    ]

    execute_batch(cursor, query, records)

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Inserted {len(records)} rows")
