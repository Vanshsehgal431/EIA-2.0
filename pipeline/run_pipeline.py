from datetime import UTC, datetime, timedelta

from pipeline.extract.eia_extractor import fetch_all_power_data
from pipeline.extract.weather_extractor import fetch_weather_data
from pipeline.load.eia_loader import load_eia_dataframe
from pipeline.load.fact_builder import build_fact_table
from pipeline.load.watermark import (
    get_last_energy_timestamp,
    get_last_weather_timestamp,
)
from pipeline.load.weather_loader import load_weather_dataframe
from pipeline.storage.bronze_writer import save_raw_payload
from pipeline.transform.eia_transformer import transform_eia_payload
from pipeline.transform.weather_transformer import transform_weather_payload
from pipeline.utils.logger import setup_logger
from pipeline.validation.eia_validation import validate_eia_dataframe
from pipeline.validation.weather_validation import validate_weather_dataframe

logger = setup_logger(__name__)


def run():

    start_time = datetime.now(UTC)

    rows_eia = 0
    rows_weather = 0

    print("PIPELINE STARTING...")
    logger.info("===== PIPELINE START =====")

    safe_now = datetime.utcnow() - timedelta(days=2)

    # EIA PROCESSING
    logger.info("Processing EIA data")

    energy_watermark = get_last_energy_timestamp()

    if energy_watermark:
        start_ts = min(energy_watermark, safe_now)
    else:
        start_ts = safe_now

    start_str = start_ts.strftime("%Y-%m-%d")
    logger.info(f"EIA fetch start date: {start_str}")

    eia_payloads = fetch_all_power_data(start=start_str)

    for payload in eia_payloads:
        save_raw_payload(payload, source="eia")

        df = transform_eia_payload(payload)

        rows_eia += len(df)

        validate_eia_dataframe(df)

        load_eia_dataframe(df)

    logger.info("EIA processing done")

    # WEATHER PROCESSING
    logger.info("Processing weather data")

    weather_watermark = get_last_weather_timestamp()
    safe_now_date = safe_now.date()

    if weather_watermark:
        start_weather_date = min(weather_watermark, safe_now_date)
    else:
        start_weather_date = safe_now_date

    start_date = start_weather_date.strftime("%Y-%m-%d")
    end_date = datetime.utcnow().strftime("%Y-%m-%d")

    logger.info(f"Weather fetch window: {start_date} -> {end_date}")

    weather_payload = fetch_weather_data(
        latitude=38.6275,
        longitude=-92.5666,
        start_date=start_date,
        end_date=end_date,
    )

    save_raw_payload(weather_payload, source="weather")

    weather_df = transform_weather_payload(weather_payload)

    rows_weather += len(weather_df)

    validate_weather_dataframe(weather_df)

    load_weather_dataframe(weather_df)

    logger.info("Weather processing done")

    # FACT TABLE
    logger.info("Building fact table")

    build_fact_table()

    duration = (datetime.now(UTC) - start_time).total_seconds()

    logger.info(f"Rows EIA: {rows_eia}")
    logger.info(f"Rows Weather: {rows_weather}")
    logger.info(f"Pipeline duration: {duration:.2f} sec")

    logger.info("===== PIPELINE SUCCESS =====")


if __name__ == "__main__":
    run()
