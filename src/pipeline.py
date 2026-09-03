
from __future__ import annotations

import csv
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_FILE = PROJECT_ROOT / "data" / "cityInputs.txt"
LOG_FILE = PROJECT_ROOT / "reports" / "pipeline.log"

# Canonical display names for the compacted normalized keys.
CITY_ALIASES = {
    "boston": "Boston",
    "geneva": "Geneva",
    "london": "London",
    "losangeles": "Los Angeles",
    "madrid": "Madrid",
    "mexicocity": "Mexico City",
    "montreal": "Montreal",
    "moscow": "Moscow",
    "newyork": "New York",
    "paris": "Paris",
    "rome": "Rome",
    "saopaulo": "São Paulo",
    "sydney": "Sydney",
    "toronto": "Toronto",
    "berlin": "Berlin",
}


@dataclass(frozen=True)
class CityRecord:
    city: str
    latitude: float
    longitude: float


def configure_logger() -> logging.Logger:
    """Configure a file logger using LOG_LEVEL from .env."""
    load_dotenv(PROJECT_ROOT / ".env")

    log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("weather_pipeline")
    logger.setLevel(log_level)
    logger.propagate = False

    # Prevent duplicate entries if this module is run multiple times.
    if not logger.handlers:
        handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
            )
        )
        logger.addHandler(handler)

    return logger


def normalize_city_name(raw_city: str) -> str:
    """Remove formatting noise and return a canonical city name."""
    cleaned = raw_city.strip()
    cleaned = re.sub(r"[^\w\s]", "", cleaned)  # Remove '.', '-', etc.
    cleaned = re.sub(r"\s+", " ", cleaned)     # Collapse erratic whitespace.

    # The compact key handles values like "Bos ton" and "MexicoCity".
    compact_key = re.sub(r"[\s_]", "", cleaned).casefold()

    if compact_key in CITY_ALIASES:
        return CITY_ALIASES[compact_key]

    # Sensible fallback for a city not yet in the alias map.
    return cleaned.title()


def parse_city_data(input_file: Path = INPUT_FILE) -> list[CityRecord]:
    """Parse and normalize city-coordinate records from the source CSV."""
    logger = configure_logger()
    records: list[CityRecord] = []

    logger.info("Starting parse of %s", input_file)

    try:
        with input_file.open("r", newline="", encoding="utf-8") as source:
            reader = csv.reader(source)

            for line_number, row in enumerate(reader, start=1):
                try:
                    if len(row) != 3:
                        raise ValueError(
                            f"Expected 3 CSV fields, received {len(row)}"
                        )

                    city = normalize_city_name(row[0])
                    latitude = float(row[1].strip())
                    longitude = float(row[2].strip())

                    if not -90 <= latitude <= 90:
                        raise ValueError(f"Invalid latitude: {latitude}")
                    if not -180 <= longitude <= 180:
                        raise ValueError(f"Invalid longitude: {longitude}")

                    records.append(CityRecord(city, latitude, longitude))
                    logger.info(
                        "Parsed line %d: %s (%.4f, %.4f)",
                        line_number,
                        city,
                        latitude,
                        longitude,
                    )

                except (TypeError, ValueError) as error:
                    logger.error(
                        "Skipping invalid record on line %d: %r (%s)",
                        line_number,
                        row,
                        error,
                    )

    except OSError as error:
        logger.error("Unable to read %s: %s", input_file, error)
        raise

    logger.info("Parsing complete: %d valid city records", len(records))
    return records


if __name__ == "__main__":
    for record in parse_city_data():
        print(record)
        