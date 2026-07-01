"""
load_postgres.py
==================
Load stage: reads the Parquet output of the transform stage and loads it
into the PostgreSQL star schema (warehouse.dim_movie, dim_genre,
dim_movie_genre, dim_date, dim_cpi, fact_movie_performance).

Uses plain pandas + psycopg2 execute_values for portability (no Spark-JDBC
dependency needed in the Airflow worker), since by this point the data
volumes are aggregate-level and comfortably fit in memory.
"""
import os
import uuid
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("load_postgres")

PROCESSED_DIR = Path(os.environ.get("MOVIANALYTICS_PROCESSED_DIR", "/opt/airflow/data/processed"))

PG_CONN = dict(
    host=os.environ.get("WAREHOUSE_DB_HOST", "warehouse-postgres"),
    port=os.environ.get("WAREHOUSE_DB_PORT", "5432"),
    dbname=os.environ.get("WAREHOUSE_DB_NAME", "movianalytics"),
    user=os.environ.get("WAREHOUSE_DB_USER", "movianalytics"),
    password=os.environ.get("WAREHOUSE_DB_PASSWORD", "movianalytics"),
)

BATCH_ID = os.environ.get("AIRFLOW_RUN_ID", str(uuid.uuid4()))


def get_conn():
    return psycopg2.connect(**PG_CONN)


def load_dim_date(cur, year_months):
    rows = []
    for ym in sorted(set(year_months)):
        if not ym or pd.isna(ym):
            continue
        year, month = int(ym[:4]), int(ym[5:7])
        full_date = f"{ym}-01"
        quarter = (month - 1) // 3 + 1
        rows.append((full_date, year, month, quarter, ym))

    execute_values(
        cur,
        """
        INSERT INTO warehouse.dim_date (full_date, year, month, quarter, year_month)
        VALUES %s
        ON CONFLICT (full_date) DO NOTHING
        """,
        rows,
    )


def load_dim_cpi(cur, cpi_df):
    cpi_df = cpi_df.sort_values("year_month").reset_index(drop=True)
    cpi_df["yoy_pct_change"] = cpi_df["cpi_value"].pct_change(periods=12) * 100
    rows = list(
        cpi_df[["year_month", "cpi_value", "yoy_pct_change"]]
        .itertuples(index=False, name=None)
    )
    execute_values(
        cur,
        """
        INSERT INTO warehouse.dim_cpi (year_month, cpi_value, yoy_pct_change)
        VALUES %s
        ON CONFLICT (year_month) DO UPDATE SET
            cpi_value = EXCLUDED.cpi_value,
            yoy_pct_change = EXCLUDED.yoy_pct_change
        """,
        rows,
    )


def load_dim_genre(cur, genre_names):
    rows = [(g,) for g in sorted(set(genre_names))]
    execute_values(
        cur,
        "INSERT INTO warehouse.dim_genre (genre_name) VALUES %s ON CONFLICT (genre_name) DO NOTHING",
        rows,
    )


def _nullable(df, cols):
    """pandas/Parquet represents missing numeric values as NaN, which
    psycopg2 will happily insert as the literal float NaN rather than SQL
    NULL. Convert to None first so optional columns (budget, revenue,
    rating_cluster, etc.) load as proper NULLs."""
    sub = df[cols].copy()
    return sub.astype(object).where(pd.notnull(sub), None)


def load_dim_movie(cur, movies_df):
    rows = list(
        _nullable(movies_df, [
            "movie_id", "title", "original_language", "runtime",
            "release_date", "budget", "revenue", "popularity",
        ]).itertuples(index=False, name=None)
    )
    execute_values(
        cur,
        """
        INSERT INTO warehouse.dim_movie
            (movie_id, title, original_language, runtime_minutes,
             release_date, budget_usd, revenue_usd, popularity_score)
        VALUES %s
        ON CONFLICT (movie_id) DO UPDATE SET
            title = EXCLUDED.title,
            original_language = EXCLUDED.original_language,
            runtime_minutes = EXCLUDED.runtime_minutes,
            release_date = EXCLUDED.release_date,
            budget_usd = EXCLUDED.budget_usd,
            revenue_usd = EXCLUDED.revenue_usd,
            popularity_score = EXCLUDED.popularity_score
        """,
        rows,
    )


def load_bridge_movie_genre(cur, genre_long_df):
    cur.execute("""
        INSERT INTO warehouse.dim_movie_genre (movie_key, genre_key)
        SELECT DISTINCT m.movie_key, g.genre_key
        FROM warehouse.dim_movie m
        JOIN (VALUES {}) AS src(movie_id, genre_name) ON src.movie_id = m.movie_id
        JOIN warehouse.dim_genre g ON g.genre_name = src.genre_name
        ON CONFLICT DO NOTHING
    """.format(
        ",".join(
            cur.mogrify("(%s,%s)", row).decode() for row in genre_long_df.itertuples(index=False, name=None)
        )
    )) if len(genre_long_df) else None


def load_fact(cur, fact_df):
    """Batched INSERT...SELECT that resolves movie_id/year_month into the
    surrogate keys of dim_movie / dim_date / dim_cpi in a single statement."""
    rows = list(
        _nullable(fact_df, [
            "movie_id", "year_month", "avg_rating", "rating_count",
            "revenue", "budget", "roi", "rating_cluster",
        ]).itertuples(index=False, name=None)
    )
    values_sql = ",".join(
        cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", row).decode() for row in rows
    )
    if not values_sql:
        return
    cur.execute(f"""
        INSERT INTO warehouse.fact_movie_performance
            (movie_key, date_key, cpi_key, avg_rating, rating_count,
             revenue_usd, budget_usd, roi, rating_cluster, load_batch_id)
        SELECT
            m.movie_key, d.date_key, c.cpi_key,
            src.avg_rating, src.rating_count, src.revenue, src.budget,
            src.roi, src.rating_cluster, '{BATCH_ID}'
        FROM (VALUES {values_sql})
            AS src(movie_id, year_month, avg_rating, rating_count, revenue, budget, roi, rating_cluster)
        JOIN warehouse.dim_movie m ON m.movie_id = src.movie_id
        LEFT JOIN warehouse.dim_date d ON d.year_month = src.year_month
        LEFT JOIN warehouse.dim_cpi c ON c.year_month = src.year_month
    """)


def main():
    movies_df = pd.read_parquet(PROCESSED_DIR / "movie_facts")
    genre_long_df = pd.read_parquet(PROCESSED_DIR / "movie_genres")
    cpi_df = pd.read_parquet(PROCESSED_DIR / "cpi")

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()
    try:
        # First clear this batch's previous attempt (idempotent re-runs)
        cur.execute("DELETE FROM warehouse.fact_movie_performance WHERE load_batch_id = %s", (BATCH_ID,))

        load_dim_date(cur, movies_df["year_month"].tolist())
        load_dim_cpi(cur, cpi_df)
        load_dim_genre(cur, genre_long_df["genre_name"].tolist())
        load_dim_movie(cur, movies_df)
        load_bridge_movie_genre(cur, genre_long_df)
        load_fact(cur, movies_df)

        conn.commit()
        logger.info("Load complete. batch_id=%s, movies=%d", BATCH_ID, len(movies_df))
    except Exception:
        conn.rollback()
        logger.exception("Load failed, transaction rolled back")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
