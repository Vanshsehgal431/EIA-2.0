import time

import requests

from pipeline.utils.logger import setup_logger

logger = setup_logger(__name__)


class ApiError(RuntimeError):
    pass


def call_api(url, params=None, timeout=50, retries=3, backoff=2):

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()

            logger.info(f"API call successful (attempt {attempt})")
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.warning(f"API attempt {attempt} failed: {e}")

            if attempt == retries:
                logger.error("API failed after max retries")
                raise ApiError("External API failure") from e

            sleep_time = backoff**attempt
            logger.info(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
