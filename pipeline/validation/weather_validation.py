import pandas as pd

from pipeline.utils.logger import setup_logger

logger = setup_logger(__name__)


def validate_weather_dataframe(df: pd.DataFrame):
    """
    Validate transformed Weather dataframe
    Raises ValueError if validation fails
    """

    logger.info("Starting Weather validation")

    if df.empty:
        logger.error("Validation failed: DataFrame is empty")
        raise ValueError("Weather dataframe is empty")

    required_columns = [
        "temperature_2m_max",
        "temperature_2m_min",
        "snowfall_sum",
        "rain_sum",
        "showers_sum",
    ]

    for col in required_columns:
        if col not in df.columns:
            logger.error(f"Missing required column: {col}")
            raise ValueError(f"Missing required column: {col}")

    logger.info("Schema validation passed")

    for col in required_columns:
        if df[col].isnull().any():
            logger.error(f"Null values detected in {col}")
            raise ValueError(f"Null values detected in {col}")

    logger.info("Null checks passed")

    if (df["temperature_2m_min"] > df["temperature_2m_max"]).any():
        logger.error("Min temperature greater than max detected")
        raise ValueError("Invalid temperature range")

    if (df[["snowfall_sum", "rain_sum", "showers_sum"]] < 0).any().any():
        logger.error("Negative precipitation detected")
        raise ValueError("Negative precipitation detected")

    logger.info("Weather validation completed successfully")

    return True
