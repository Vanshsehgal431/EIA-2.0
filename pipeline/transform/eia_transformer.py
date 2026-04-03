from datetime import datetime

import pandas as pd

from pipeline.utils.logger import setup_logger

logger = setup_logger(__name__)


def transform_eia_payload(payload: dict) -> pd.DataFrame:
    """
    Transform raw EIA payload into structured DataFrame.
    """
    logger.info("Starting EIA transformation")

    response = payload.get("response", {})
    records = response.get("data", [])

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    logger.info(f"Loaded {len(df)} records into DataFrame")

    df = df.rename(
        columns={"period": "timestamp", "value": "demand_mw", "subba": "region"}
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["demand_mw"] = pd.to_numeric(df["demand_mw"], errors="coerce")

    df["source"] = "eia"
    df["ingested_at"] = datetime.utcnow()

    logger.info("EIA transformation completed successfully")

    return df.sort_values("timestamp")
