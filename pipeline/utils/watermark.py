from pathlib import Path

import pandas as pd

WATERMARK_FILE = Path("data/watermark.txt")
DEFAULT_WATERMARK = "2026-01-01"


def read_watermark():
    if not WATERMARK_FILE.exists():
        return pd.Timestamp(DEFAULT_WATERMARK, tz="UTC")

    value = WATERMARK_FILE.read_text().strip()
    return pd.Timestamp(value, tz="UTC")


def write_watermark(ts: pd.Timestamp):
    WATERMARK_FILE.parent.mkdir(parents=True, exist_ok=True)
    WATERMARK_FILE.write_text(ts.strftime("%Y-%m-%d"))
