from config.settings import EIA_API_KEY, EIA_BASE_URL, REQUEST_TIMEOUT
from pipeline.utils.api import call_api
from pipeline.utils.logger import setup_logger

logger = setup_logger(__name__)


def fetch_daily_data(limit=5000, offset=0, start=None):
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "daily",
        "data[0]": "value",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": offset,
        "length": limit,
        "facets[subba][]": "SCE",
    }
    if start:
        params["start"] = start

    return call_api(EIA_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)


def fetch_all_power_data(page_size=5000, start=None):
    offset = 0
    all_payloads = []

    while True:
        logger.info(f"Requesting offset {offset}")

        payload = fetch_daily_data(limit=page_size, offset=offset, start=start)

        total = int(payload.get("response", {}).get("total", 0))
        records = payload.get("response", {}).get("data", [])

        if not records:
            logger.info("No more records found.")
            break

        if not payload:
            logger.error("Empty payload received from API.")
            break

        all_payloads.append(payload)

        offset += page_size

        if offset >= total:
            logger.info("Reached total record count.")
            break

    return all_payloads
