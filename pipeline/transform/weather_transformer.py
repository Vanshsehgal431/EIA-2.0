from datetime import datetime

import pandas as pd

from pipeline.utils.logger import setup_logger

logger = setup_logger(__name__)


def transform_weather_payload(payload: dict) -> pd.DataFrame:
    """
    Transform raw weather payload into structured DataFrame.
    """
    logger.info("Starting Weather Transformation")

    response = payload.get("daily", {})

    if not response:
        return pd.DataFrame()

    df = pd.DataFrame(response)

    df = df.rename(columns={"time": "date"})

    df["date"] = pd.to_datetime(df["date"])
    df["source"] = "open-meteo"
    df["ingest_at"] = datetime.utcnow()

    logger.info("Weather transformation completed successfully")

    return df
