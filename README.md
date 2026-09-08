# Weather Pipeline Lab

Weather Pipeline Laboratory for Cenfotec University course SOFT-753, section
SEV1, period M5-2026.

The pipeline reads city coordinates from a CSV file, retrieves hourly weather
forecasts from Open-Meteo, transforms the responses with pandas, and generates
daily weather reports.

## Prerequisites

- Python 3.13
- [uv](https://docs.astral.sh/uv/)

Install the project dependencies:

```powershell
uv sync
```

## Configuration

Create a local `.env` file in the repository root:

```dotenv
LOG_LEVEL=INFO
REPORT_THRESHOLD_C=30
```

`REPORT_THRESHOLD_C` controls the heat-alert report. A city is included when
its daily maximum temperature is strictly greater than this Celsius value.

### Corporate TLS and proxy configuration

On a network that requires an HTTP proxy or inspects TLS traffic, configure the
proxy and, preferably, the corporate certificate bundle:

```dotenv
HTTPS_PROXY=http://proxy.example.com:8080
HTTP_PROXY=http://proxy.example.com:8080
WEATHER_CA_BUNDLE=C:\path\to\corporate-ca-bundle.pem
```

`WEATHER_CA_BUNDLE` keeps certificate verification enabled. If a corporate CA
bundle is unavailable, `WEATHER_INSECURE_SSL=1` can be used only as a temporary
local-development workaround. It disables TLS certificate verification and must
not be used in production.

## Run the pipeline

From the repository root, run:

```powershell
uv run python .\src\pipeline.py
```

The pipeline writes execution details to `reports/pipeline.log` and creates:

- `reports/daily_weather_report.xlsx` — formatted daily maximum-temperature and
  precipitation statistics.
- `reports/heat_alerts.json` — an alert payload listing unique cities whose
  daily maximum temperature exceeds `REPORT_THRESHOLD_C`.

## Quality checks and tests

Run the same checks used by pull-request CI:

```powershell
uv run ruff format --check .
uv run ruff check .
uv run pytest
```

The test suite uses mocked Open-Meteo responses, so tests do not require
internet access.
