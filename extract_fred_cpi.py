"""
extract_fred_cpi.py
=====================
Pulls the US CPI-All-Urban-Consumers series (FRED series id: CPIAUCSL),
which is the macroeconomic series referenced on the resume for the
CPI vs. box-office correlation analysis.

Primary path: real FRED API call (needs a free API key from
https://fred.stlouisfed.org/docs/api/api_key.html, set as FRED_API_KEY).

Fallback path: synthetic-but-realistic monthly CPI series (matches the
real long-run trend shape: ~slow steady climb with a sharper post-2021
inflation jump) so the rest of the pipeline runs without an API key.

Output: data/raw/cpi.csv  (columns: year_month, cpi_value)
"""
import os
import csv
import json
import logging
import urllib.request
import urllib.error
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("extract_fred_cpi")

RAW_DIR = Path(os.environ.get("MOVIANALYTICS_DATA_DIR", "/opt/airflow/data/raw"))
FRED_API_KEY = os.environ.get("FRED_API_KEY")
SERIES_ID = "CPIAUCSL"
START_DATE = "2000-01-01"
END_DATE = "2023-12-31"


def try_fred_download() -> bool:
    if not FRED_API_KEY:
        logger.warning("FRED_API_KEY not set. Falling back to synthetic CPI series.")
        return False

    url = (
        "https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={SERIES_ID}&api_key={FRED_API_KEY}&file_type=json"
        f"&observation_start={START_DATE}&observation_end={END_DATE}"
    )
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            payload = json.loads(resp.read().decode())
    except (urllib.error.URLError, TimeoutError) as e:
        logger.warning("FRED API request failed (%s). Falling back to synthetic data.", e)
        return False

    observations = payload.get("observations", [])
    if not observations:
        logger.warning("FRED API returned no observations. Falling back to synthetic data.")
        return False

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DIR / "cpi.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["year_month", "cpi_value"])
        for obs in observations:
            if obs["value"] == ".":
                continue  # FRED uses "." for missing values
            year_month = obs["date"][:7]
            writer.writerow([year_month, obs["value"]])

    logger.info("Real FRED CPI data written to %s", out_path)
    return True


def generate_synthetic_cpi():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DIR / "cpi.csv"
    logger.info("Generating synthetic CPI series (2000-2023)")

    rows = []
    cpi = 169.0  # approx actual CPI-U value Jan 2000
    for year in range(2000, 2024):
        for month in range(1, 13):
            if year == 2023 and month > 12:
                break
            # Base trend ~0.18%/month, with a sharp inflation regime 2021-2022
            if 2021 <= year <= 2022:
                monthly_growth = 0.006
            elif year == 2020:
                monthly_growth = 0.0005  # pandemic dip
            else:
                monthly_growth = 0.0018
            cpi *= (1 + monthly_growth)
            rows.append((f"{year}-{month:02d}", round(cpi, 3)))

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["year_month", "cpi_value"])
        writer.writerows(rows)

    logger.info("Synthetic CPI data written to %s", out_path)


def main():
    if not try_fred_download():
        generate_synthetic_cpi()
    logger.info("Extract (FRED CPI) stage complete.")


if __name__ == "__main__":
    main()
