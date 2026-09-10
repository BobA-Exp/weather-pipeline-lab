import logging
from unittest.mock import Mock

import httpx
import pandas as pd
import pytest

import pipeline

LOGGER = logging.getLogger("weather_pipeline_tests")


@pytest.mark.parametrize(
    ("raw_city", "expected_city"),
    [
        (" madrid ", "Madrid"),
        ("Bos ton ", "Boston"),
        ("Losangeles..", "Los Angeles"),
        (" saopaulo---", "São Paulo"),
        (" -- berlin", "Berlin"),
    ],
)
def test_normalize_city_name_removes_formatting_noise(
    raw_city: str,
    expected_city: str,
) -> None:
    assert pipeline.normalize_city_name(raw_city) == expected_city


def test_fetch_hourly_weather_uses_open_meteo_parameters() -> None:
    city = pipeline.CityRecord("Madrid", 40.4168, -3.7038)
    response = httpx.Response(
        200,
        json={
            "hourly": {
                "time": ["2026-09-08T00:00"],
                "temperature_2m": [20.0],
                "precipitation": [0.0],
            }
        },
        request=httpx.Request("GET", pipeline.OPEN_METEO_URL),
    )
    client = Mock(spec=httpx.Client)
    client.get.return_value = response

    result = pipeline.fetch_hourly_weather(city, client, LOGGER)

    assert result is not None
    assert result.city == "Madrid"
    client.get.assert_called_once_with(
        pipeline.OPEN_METEO_URL,
        params={
            "latitude": 40.4168,
            "longitude": -3.7038,
            "hourly": "temperature_2m,precipitation",
            "timezone": "auto",
        },
    )


def test_weather_records_to_dataframe_handles_missing_values_and_timestamps() -> None:
    record = pipeline.WeatherRecord(
        city="Madrid",
        latitude=40.4168,
        longitude=-3.7038,
        weather_data={
            "hourly": {
                "time": ["2026-09-08T00:00", "not-a-date", "2026-09-08T02:00"],
                "temperature_2m": ["20.5", None, "not-a-number"],
                "precipitation": ["0.2", None, "1.1"],
            }
        },
    )

    forecasts = pipeline.weather_records_to_dataframe([record], LOGGER)

    assert len(forecasts) == 2
    assert pd.api.types.is_datetime64_any_dtype(forecasts["timestamp"])
    assert forecasts["temperature_2m"].isna().sum() == 1
    assert forecasts["precipitation"].isna().sum() == 0


def test_transform_weather_data_aggregates_and_merges_city_metadata() -> None:
    city = pipeline.CityRecord("Madrid", 40.4168, -3.7038)
    record = pipeline.WeatherRecord(
        city="Madrid",
        latitude=40.4168,
        longitude=-3.7038,
        weather_data={
            "hourly": {
                "time": [
                    "2026-09-08T00:00",
                    "2026-09-08T12:00",
                    "2026-09-09T00:00",
                ],
                "temperature_2m": [20.0, 25.5, 18.0],
                "precipitation": [0.2, 1.3, 0.0],
            }
        },
    )

    statistics = pipeline.transform_weather_data([city], [record], LOGGER)

    assert statistics["city"].tolist() == ["Madrid", "Madrid"]
    assert statistics["latitude"].tolist() == [40.4168, 40.4168]
    assert statistics["max_temperature_c"].tolist() == [25.5, 18.0]
    assert statistics["total_precipitation_mm"].tolist() == [1.5, 0.0]
