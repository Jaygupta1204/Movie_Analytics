# MoviAnalytics — End-to-End Movie & Macroeconomic Data Engineering Pipeline

An ETL pipeline that ingests movie metadata (MovieLens/Kaggle "The Movies
Dataset") and US CPI macroeconomic data (FRED API), transforms it with
PySpark, loads it into a PostgreSQL star-schema data warehouse, and
orchestrates the whole thing with Apache Airflow on Docker. A Power BI
dashboard connects live to the warehouse for genre revenue trends,
top-rated movie clusters, and CPI vs. box-office correlation, with a
validation report regenerated on every refresh.

## Architecture

```
                 ┌─────────────────────┐     ┌─────────────────────┐
                 │ extract_movielens.py│     │ extract_fred_cpi.py │
                 │ (Kaggle API /       │     │ (FRED API /         │
                 │  synthetic fallback)│     │  synthetic fallback)│
                 └──────────┬───────────┘     └──────────┬───────────┘
                            │  CSV                        │  CSV
                            └───────────────┬──────────────┘
                                            ▼
                                ┌─────────────────────┐
                                │  transform_spark.py  │
                                │  (PySpark: clean,    │
                                │   join, KMeans        │
                                │   rating clusters)    │
                                └──────────┬────────────┘
                                            │ Parquet
                                            ▼
                                ┌─────────────────────┐
                                │  load_postgres.py    │
                                │  -> star schema       │
                                └──────────┬────────────┘
                                            ▼
                                ┌─────────────────────┐
                                │    validate.py        │
                                │  (data quality checks,│
                                │   HTML report)        │
                                └──────────┬────────────┘
                                            ▼
                                ┌─────────────────────┐
                                │  Power BI (DirectQuery)│
                                │  live dashboard        │
                                └─────────────────────┘

All five Python stages are wired together as an Airflow DAG
(`dags/movianalytics_etl_dag.py`), running daily on a Dockerized
Airflow + Postgres stack.
```

## Star schema

`dim_movie` · `dim_genre` · `dim_movie_genre` (bridge, many-to-many) ·
`dim_date` · `dim_cpi` → `fact_movie_performance` (grain: one row per
movie). Full DDL in `sql/schema.sql`; Power BI-facing views in
`sql/views_powerbi.sql`.

## Data sources

| Source | Real path | Fallback (no credentials needed) |
|---|---|---|
| Movies | Kaggle `rounakbanik/the-movies-dataset` via Kaggle API | Synthetic, schema-identical, 45,000 rows, 2000–2023 |
| CPI | FRED series `CPIAUCSL` via FRED API | Synthetic monthly series matching real CPI's long-run shape |

Both extract scripts try the real API first and transparently fall back to
a synthetic generator if no API key is configured — so the pipeline runs
out of the box, and produces the real thing the moment you add credentials.
**To use real data**, fill in `FRED_API_KEY` and `KAGGLE_USERNAME`/`KAGGLE_KEY`
in `.env` (see `.env.example`).

## Prerequisites

- Docker Desktop (you don't have this yet — install from
  https://www.docker.com/products/docker-desktop/, then `docker --version`
  to confirm)
- Power BI Desktop (you already have this)
- ~4GB RAM free for the containers

## Running it

```bash
cd movianalytics
cp .env.example .env          # fill in API keys here if you want real data

docker compose build
docker compose up -d

# Airflow UI: http://localhost:8080  (user: admin / pass: admin)
# Trigger the DAG manually the first time, or wait for the daily schedule.
```

Once the `movianalytics_etl` DAG finishes (watch it in the Airflow UI —
all 5 tasks should go green), the warehouse is populated and live at
`localhost:5433`. Open Power BI Desktop and follow `powerbi/SETUP.md` to
connect and build the dashboard pages.

Validation reports land in `reports/validation_report_latest.html` after
every run — open it directly in a browser, or view it inside Power BI via
the `vw_latest_validation_report` view.

## Project structure

```
movianalytics/
├── dags/movianalytics_etl_dag.py     # Airflow DAG (5 tasks, daily)
├── etl/
│   ├── extract_movielens.py
│   ├── extract_fred_cpi.py
│   ├── transform_spark.py             # PySpark cleaning + KMeans clustering
│   ├── load_postgres.py
│   └── validate.py
├── sql/
│   ├── schema.sql                     # star schema DDL
│   └── views_powerbi.sql              # Power BI-facing views
├── powerbi/SETUP.md                   # connection + DAX measures
├── docker/airflow/Dockerfile          # Airflow + Java + PySpark image
├── docker-compose.yml
├── requirements.txt
└── reports/                           # generated validation reports
```

## Design notes / known trade-offs

Worth knowing these going in, since they're the kind of thing that comes
up in an interview:

- **Genre attribution**: a movie's revenue is attributed in full to *each*
  of its genres in `vw_genre_revenue`, so genre totals don't sum to total
  box office. This is a common, explicitly-documented simplification in
  movie analytics (vs. fractional attribution) — flagged here rather than
  hidden.
- **Rating clusters** use a 3-means KMeans over `[avg_rating, rating_count,
  popularity]`, relabeled by mean rating so cluster IDs are stable across
  runs. It's intentionally simple — a good place to extend with more
  features (budget tier, genre, release decade) if asked to go deeper.
- **CPI join granularity** is monthly (`year_month`), matching FRED's
  native release cadence; box-office revenue isn't actually CPI-adjusted
  in this version, it's juxtaposed for trend comparison — a natural
  follow-up would be a real-dollar (CPI-deflated) revenue column.
- **Idempotency**: loads are scoped by `load_batch_id` so re-running a
  DAG run replaces only that run's fact rows.

## Testing

`tests/test_validate.py` has a couple of lightweight checks you can extend.
Run with `pytest` after `pip install -r requirements.txt -r requirements-dev.txt`.
