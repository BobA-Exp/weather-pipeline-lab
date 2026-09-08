from __future__ import annotations

import csv
import logging
import os
import re
import ssl
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_FILE = PROJECT_ROOT / "data" / "cityInputs.txt"
LOG_FILE = PROJECT_ROOT / "reports" / "pipeline.log"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

CITY_ALIASES = {
    "berlin": "Berlin",
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
}


@dataclass(frozen=True)
class CityRecord:
    city: str
    latitude: float
    longitude: float


@dataclass(frozen=True)
class WeatherRecord:
    city: str
    latitude: float
    longitude: float
    weather_data: dict[str, Any]


def configure_logger() -> logging.Logger:
    """Configure file logging and load variables from .env."""
    load_dotenv(PROJECT_ROOT / ".env")

    configured_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, configured_level, logging.INFO)

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("weather_pipeline")
    logger.setLevel(log_level)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
        logger.addHandler(handler)

    return logger


def ssl_verify_setting(logger: logging.Logger) -> bool | ssl.SSLContext:
    """
    Return an httpx TLS verification setting.

    A custom CA keeps certificate verification enabled. The insecure setting
    is a temporary local-development workaround only.
    """
    ca_bundle_value = (
        os.getenv("WEATHER_CA_BUNDLE")
        or os.getenv("REQUESTS_CA_BUNDLE")
        or os.getenv("SSL_CERT_FILE")
    )

    if ca_bundle_value:
        ca_bundle = Path(os.path.expandvars(os.path.expanduser(ca_bundle_value)))

        if ca_bundle.is_file():
            try:
                logger.info(
                    "Using custom CA bundle for TLS verification: %s",
                    ca_bundle,
                )
                return ssl.create_default_context(cafile=str(ca_bundle))
            except (OSError, ssl.SSLError) as error:
                logger.error(
                    "Unable to load TLS CA bundle %s: %s",
                    ca_bundle,
                    error,
                )
                raise RuntimeError("Invalid TLS CA bundle configuration") from error

        logger.warning(
            "Configured TLS CA bundle does not exist: %s",
            ca_bundle,
        )

    if os.getenv("WEATHER_INSECURE_SSL", "0") == "1":
        logger.warning(
            "TLS certificate verification is DISABLED. "
            "Use only for temporary local development."
        )
        return False

    logger.info("Using the default TLS certificate store")
    return True


def normalize_city_name(raw_city: str) -> str:
    """Remove formatting noise and produce a canonical city name."""
    cleaned = raw_city.strip()
    cleaned = re.sub(r"[\W\d_]+", " ", cleaned, flags=re.UNICODE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    compact_key = re.sub(r"\s+", "", cleaned).casefold()

    if compact_key in CITY_ALIASES:
        return CITY_ALIASES[compact_key]

    return cleaned.title()


def parse_city_data(
    input_file: Path,
    logger: logging.Logger,
) -> list[CityRecord]:
    """Parse valid city-coordinate rows from the source CSV."""
    records: list[CityRecord] = []
    logger.info("Starting parse of %s", input_file)

    try:
        with input_file.open("r", newline="", encoding="utf-8") as source:
            reader = csv.reader(source)

            for line_number, row in enumerate(reader, start=1):
                try:
                    if len(row) != 3:
                        raise ValueError(f"Expected 3 CSV fields, received {len(row)}")

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


def fetch_hourly_weather(
    city: CityRecord,
    client: httpx.Client,
    logger: logging.Logger,
) -> WeatherRecord | None:
    """Request hourly temperature and precipitation for one city."""
    params = {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "hourly": "temperature_2m,precipitation",
        "timezone": "auto",
    }

    try:
        logger.info("Requesting weather data for %s", city.city)

        response = client.get(OPEN_METEO_URL, params=params)
        response.raise_for_status()

        weather_data = response.json()
        if not isinstance(weather_data, dict):
            raise TypeError("API returned an unexpected response format")

        logger.info("Retrieved weather data for %s", city.city)
        return WeatherRecord(
            city=city.city,
            latitude=city.latitude,
            longitude=city.longitude,
            weather_data=weather_data,
        )

    except (httpx.HTTPError, TypeError, ValueError) as error:
        logger.error("Weather request failed for %s: %s", city.city, error)
        return None


def extract_weather(
    cities: list[CityRecord],
    logger: logging.Logger,
) -> list[WeatherRecord]:
    """Sequentially retrieve weather data for each parsed city."""
    results: list[WeatherRecord] = []
    start_time = time.perf_counter()
    verify = ssl_verify_setting(logger)

    logger.info("Starting sequential API extraction for %d cities", len(cities))

    try:
        with httpx.Client(
            timeout=15.0,
            verify=verify,
            trust_env=True,
        ) as client:
            for city in cities:
                result = fetch_hourly_weather(city, client, logger)

                if result is not None:
                    results.append(result)

    finally:
        elapsed_seconds = time.perf_counter() - start_time
        logger.info(
            "API extraction complete: %d of %d cities succeeded in %.2f seconds",
            len(results),
            len(cities),
            elapsed_seconds,
        )

    return results


def weather_records_to_dataframe(
    weather_records: list[WeatherRecord],
    logger: logging.Logger,
) -> pd.DataFrame:
    """Load hourly weather JSON payloads into one normalized DataFrame."""
    frames: list[pd.DataFrame] = []
    required_columns = {"time", "temperature_2m", "precipitation"}

    for record in weather_records:
        hourly_data = record.weather_data.get("hourly")

        if not isinstance(hourly_data, dict):
            logger.error("Missing hourly data in API response for %s", record.city)
            continue

        missing_columns = required_columns.difference(hourly_data)
        if missing_columns:
            logger.error(
                "Hourly API response for %s is missing columns: %s",
                record.city,
                ", ".join(sorted(missing_columns)),
            )
            continue

        hourly_frame = pd.DataFrame(hourly_data)
        hourly_frame["city"] = record.city
        hourly_frame["latitude"] = record.latitude
        hourly_frame["longitude"] = record.longitude

        hourly_frame["timestamp"] = pd.to_datetime(
            hourly_frame.pop("time"),
            errors="coerce",
        )
        hourly_frame["temperature_2m"] = pd.to_numeric(
            hourly_frame["temperature_2m"],
            errors="coerce",
        )
        hourly_frame["precipitation"] = pd.to_numeric(
            hourly_frame["precipitation"],
            errors="coerce",
        )

        invalid_timestamp_count = hourly_frame["timestamp"].isna().sum()
        missing_temperature_count = hourly_frame["temperature_2m"].isna().sum()
        missing_precipitation_count = hourly_frame["precipitation"].isna().sum()

        if invalid_timestamp_count:
            logger.warning(
                "Dropping %d rows with invalid timestamps for %s",
                invalid_timestamp_count,
                record.city,
            )
            hourly_frame = hourly_frame.dropna(subset=["timestamp"])

        if missing_temperature_count or missing_precipitation_count:
            logger.warning(
                "Missing values for %s: temperature=%d, precipitation=%d",
                record.city,
                missing_temperature_count,
                missing_precipitation_count,
            )

        frames.append(hourly_frame)

    if not frames:
        return pd.DataFrame(
            columns=[
                "city",
                "latitude",
                "longitude",
                "timestamp",
                "temperature_2m",
                "precipitation",
            ]
        )

    hourly_forecasts = pd.concat(frames, ignore_index=True)
    logger.info("Loaded %d hourly forecast rows into pandas", len(hourly_forecasts))
    return hourly_forecasts


def aggregate_daily_weather(hourly_forecasts: pd.DataFrame) -> pd.DataFrame:
    """Calculate daily maximum temperature and precipitation totals per city."""
    if hourly_forecasts.empty:
        return pd.DataFrame(
            columns=[
                "city",
                "date",
                "max_temperature_c",
                "total_precipitation_mm",
            ]
        )

    forecasts = hourly_forecasts.copy()
    forecasts["date"] = forecasts["timestamp"].dt.normalize()

    return (
        forecasts.groupby(["city", "date"], as_index=False)
        .agg(
            max_temperature_c=("temperature_2m", "max"),
            total_precipitation_mm=(
                "precipitation",
                lambda values: values.sum(min_count=1),
            ),
        )
        .sort_values(["city", "date"], ignore_index=True)
    )


def merge_city_weather_statistics(
    cities: list[CityRecord],
    daily_weather: pd.DataFrame,
) -> pd.DataFrame:
    """Join normalized CSV city details to daily aggregated weather statistics."""
    cities_frame = pd.DataFrame(
        [
            {
                "city": city.city,
                "latitude": city.latitude,
                "longitude": city.longitude,
            }
            for city in cities
        ]
    )

    return cities_frame.merge(
        daily_weather,
        on="city",
        how="left",
        validate="one_to_many",
    ).sort_values(["city", "date"], ignore_index=True)


def transform_weather_data(
    cities: list[CityRecord],
    weather_records: list[WeatherRecord],
    logger: logging.Logger,
) -> pd.DataFrame:
    """Transform API responses, aggregate daily values, and join city metadata."""
    hourly_forecasts = weather_records_to_dataframe(weather_records, logger)
    daily_weather = aggregate_daily_weather(hourly_forecasts)
    merged_statistics = merge_city_weather_statistics(cities, daily_weather)

    logger.info(
        "Created %d daily city weather-statistic rows",
        len(merged_statistics),
    )
    return merged_statistics


def main() -> None:
    """Run the parsing, API extraction, and daily aggregation pipeline."""
    logger = configure_logger()
    cities = parse_city_data(INPUT_FILE, logger)
    weather_records = extract_weather(cities, logger)
    daily_statistics = transform_weather_data(cities, weather_records, logger)

    if daily_statistics.empty:
        print("No weather statistics were created.")
    else:
        print(daily_statistics.to_string(index=False))


if __name__ == "__main__":
    main()
