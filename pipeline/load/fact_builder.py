import psycopg2

from config.settings import POSTGRES_CONFIG
from pipeline.utils.logger import setup_logger

logger = setup_logger(__name__)


def build_fact_table():
    logger.info("Building fact_energy_weather table")

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    with open("pipeline/sql/build_fact_energy_weather.sql") as f:
        query = f.read()

    cursor.execute(query)

    conn.commit()
    cursor.close()
    conn.close()

    logger.info("Fact table build completed")
    logger.info("Fact table build completed")
