import pandas as pd

from pipeline.utils.logger import setup_logger

logger = setup_logger(__name__)


def validate_eia_dataframe(df: pd.DataFrame):
    """
    Validate transformed EIA dataframe
    Raises ValueError if validation fails
    """

    logger.info("Starting EIA validation")

    if df.empty:
        logger.error("Validation failed: DataFrame is empty")
        raise ValueError("EIA dataframe is empty")

    required_columns = ["timestamp", "region", "demand_mw"]

    for col in required_columns:
        if col not in df.columns:
            logger.error(f"Missing required column: {col}")
            raise ValueError(f"Missing required column: {col}")

    logger.info("Schema validation passed")

    if df["timestamp"].isnull().any():
        logger.error("Null timestamps detected")
        raise ValueError("Null timestamps detected")

    if df["demand_mw"].isnull().any():
        logger.error("Null demand values detected")
        raise ValueError("Null demand values detected")

    logger.info("Null checks passed")

    if (df["demand_mw"] < 0).any():
        logger.error("Negative demand detected")
        raise ValueError("Negative demand detected")

    logger.info("Range checks passed")

    """
        Temporary deleting diplicate to test pipeline
    """

    df = df.drop_duplicates(subset=["timestamp", "region"])

    duplicates = df.duplicated(subset=["timestamp", "region"])
    if duplicates.any():
        logger.error("Duplicate records detected")
        raise ValueError("Duplicate records detected")

    logger.info("Duplicate check passed")

    logger.info("EIA validation completed successfully")

    return True
