"""
validate.py
=============
Runs a set of data-quality checks against the warehouse after each load,
writes results into warehouse.etl_validation_log (so Power BI's
vw_latest_validation_report can surface refresh health to end users), and
also drops a human-readable HTML report into reports/ for quick viewing.

This is the "validation report generation each refresh" bullet.
"""
import os
import logging
from pathlib import Path
from datetime import datetime

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("validate")

PG_CONN = dict(
    host=os.environ.get("WAREHOUSE_DB_HOST", "warehouse-postgres"),
    port=os.environ.get("WAREHOUSE_DB_PORT", "5432"),
    dbname=os.environ.get("WAREHOUSE_DB_NAME", "movianalytics"),
    user=os.environ.get("WAREHOUSE_DB_USER", "movianalytics"),
    password=os.environ.get("WAREHOUSE_DB_PASSWORD", "movianalytics"),
)

REPORTS_DIR = Path(os.environ.get("MOVIANALYTICS_REPORTS_DIR", "/opt/airflow/reports"))
BATCH_ID = os.environ.get("AIRFLOW_RUN_ID", "manual")

CHECKS = [
    {
        "name": "row_count_min_threshold",
        "sql": "SELECT COUNT(*) FROM warehouse.dim_movie",
        "rule": lambda v: ("PASS" if v >= 1000 else "FAIL", f"{v} movies loaded"),
    },
    {
        "name": "no_null_movie_titles",
        "sql": "SELECT COUNT(*) FROM warehouse.dim_movie WHERE title IS NULL OR title = ''",
        "rule": lambda v: ("PASS" if v == 0 else "FAIL", f"{v} rows with null/blank title"),
    },
    {
        "name": "fact_referential_integrity",
        "sql": """
            SELECT COUNT(*) FROM warehouse.fact_movie_performance f
            LEFT JOIN warehouse.dim_movie m ON f.movie_key = m.movie_key
            WHERE m.movie_key IS NULL
        """,
        "rule": lambda v: ("PASS" if v == 0 else "FAIL", f"{v} orphaned fact rows"),
    },
    {
        "name": "revenue_non_negative",
        "sql": "SELECT COUNT(*) FROM warehouse.fact_movie_performance WHERE revenue_usd < 0",
        "rule": lambda v: ("PASS" if v == 0 else "FAIL", f"{v} negative revenue rows"),
    },
    {
        "name": "rating_within_bounds",
        "sql": "SELECT COUNT(*) FROM warehouse.fact_movie_performance WHERE avg_rating NOT BETWEEN 0 AND 5",
        "rule": lambda v: ("PASS" if v == 0 else "FAIL", f"{v} ratings outside [0,5]"),
    },
    {
        "name": "cpi_coverage",
        "sql": """
            SELECT COUNT(*) FROM warehouse.fact_movie_performance
            WHERE cpi_key IS NULL AND date_key IS NOT NULL
        """,
        "rule": lambda v: ("WARN" if v > 0 else "PASS", f"{v} fact rows missing a CPI match"),
    },
    {
        "name": "duplicate_movie_ids",
        "sql": """
            SELECT COUNT(*) FROM (
                SELECT movie_id, COUNT(*) c FROM warehouse.dim_movie GROUP BY movie_id HAVING COUNT(*) > 1
            ) d
        """,
        "rule": lambda v: ("PASS" if v == 0 else "FAIL", f"{v} duplicate movie_ids"),
    },
]


def run_checks(cur):
    results = []
    for check in CHECKS:
        cur.execute(check["sql"])
        value = cur.fetchone()[0]
        status, details = check["rule"](value)
        results.append((check["name"], status, details))
        logger.info("[%s] %s — %s", status, check["name"], details)
    return results


def persist_results(cur, results):
    for name, status, details in results:
        cur.execute(
            """
            INSERT INTO warehouse.etl_validation_log (batch_id, check_name, status, details)
            VALUES (%s, %s, %s, %s)
            """,
            (BATCH_ID, name, status, details),
        )


def write_html_report(results):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().isoformat()
    n_fail = sum(1 for _, s, _ in results if s == "FAIL")
    overall = "FAILED" if n_fail else "PASSED"

    rows_html = "\n".join(
        f"<tr><td>{name}</td><td class='{status.lower()}'>{status}</td><td>{details}</td></tr>"
        for name, status, details in results
    )
    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>MoviAnalytics Validation Report</title>
<style>
body {{ font-family: sans-serif; margin: 2rem; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border: 1px solid #ccc; padding: 8px 12px; text-align: left; }}
th {{ background: #222; color: #fff; }}
.pass {{ color: green; font-weight: bold; }}
.fail {{ color: red; font-weight: bold; }}
.warn {{ color: darkorange; font-weight: bold; }}
</style></head>
<body>
<h1>MoviAnalytics — ETL Validation Report</h1>
<p><b>Batch:</b> {BATCH_ID} &nbsp; <b>Run time (UTC):</b> {timestamp} &nbsp; <b>Overall:</b> {overall}</p>
<table>
<tr><th>Check</th><th>Status</th><th>Details</th></tr>
{rows_html}
</table>
</body></html>
"""
    out_path = REPORTS_DIR / f"validation_report_{BATCH_ID}.html"
    out_path.write_text(html, encoding="utf-8")
    # also keep a stable "latest" copy
    (REPORTS_DIR / "validation_report_latest.html").write_text(html, encoding="utf-8")
    logger.info("Validation report written to %s", out_path)
    return overall


def main():
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    try:
        results = run_checks(cur)
        persist_results(cur, results)
        conn.commit()
        overall = write_html_report(results)
        if overall == "FAILED":
            raise RuntimeError("Validation failed — see report for details")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
