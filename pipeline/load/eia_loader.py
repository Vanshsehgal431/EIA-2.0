import psycopg2
from psycopg2.extras import execute_batch

from config.settings import POSTGRES_CONFIG
from pipeline.utils.logger import setup_logger

logger = setup_logger(__name__)


def load_eia_dataframe(df):
    logger.info("Loading EIA data into energy_consumption ")

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    query = """
    INSERT INTO energy_consumption (
      timestamp,
      region,
      demand_mw,
      source,
      ingested_at
    )
    VALUES(%s,%s,%s,%s,%s)
    ON CONFLICT (timestamp, region) DO NOTHING;
  """

    records = [
        (row.timestamp, row.region, row.demand_mw, row.source, row.ingested_at)
        for row in df.itertuples()
    ]

    execute_batch(cursor, query, records)
    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Inserted {len(records)} rows")
