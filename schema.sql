-- =====================================================================
-- MoviAnalytics Data Warehouse — Star Schema
-- Target: PostgreSQL 15+
-- Grain of fact_movie_performance: one row per movie
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS warehouse;
SET search_path TO warehouse;

-- ---------------------------------------------------------------------
-- DIMENSION: dim_movie
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS fact_movie_performance CASCADE;
DROP TABLE IF EXISTS dim_movie_genre CASCADE;
DROP TABLE IF EXISTS dim_movie CASCADE;
DROP TABLE IF EXISTS dim_genre CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_cpi CASCADE;
DROP TABLE IF EXISTS etl_validation_log CASCADE;

CREATE TABLE dim_movie (
    movie_key           SERIAL PRIMARY KEY,
    movie_id            INTEGER NOT NULL UNIQUE,      -- source id (MovieLens/TMDB id)
    title                TEXT NOT NULL,
    original_language   VARCHAR(10),
    runtime_minutes      INTEGER,
    release_date         DATE,
    budget_usd           NUMERIC(14,2),
    revenue_usd          NUMERIC(14,2),
    popularity_score     NUMERIC(10,4),
    created_at           TIMESTAMP DEFAULT now()
);

-- ---------------------------------------------------------------------
-- DIMENSION: dim_genre  (movies have many genres -> bridge table)
-- ---------------------------------------------------------------------
CREATE TABLE dim_genre (
    genre_key   SERIAL PRIMARY KEY,
    genre_name  VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE dim_movie_genre (
    movie_key   INTEGER NOT NULL REFERENCES dim_movie(movie_key) ON DELETE CASCADE,
    genre_key   INTEGER NOT NULL REFERENCES dim_genre(genre_key) ON DELETE CASCADE,
    PRIMARY KEY (movie_key, genre_key)
);

-- ---------------------------------------------------------------------
-- DIMENSION: dim_date  (release-month grain, used to join CPI)
-- ---------------------------------------------------------------------
CREATE TABLE dim_date (
    date_key    SERIAL PRIMARY KEY,
    full_date   DATE NOT NULL UNIQUE,
    year        INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    quarter     INTEGER NOT NULL,
    year_month  VARCHAR(7) NOT NULL  -- 'YYYY-MM' join key with CPI
);

-- ---------------------------------------------------------------------
-- DIMENSION: dim_cpi  (FRED CPIAUCSL — monthly, US macro context)
-- ---------------------------------------------------------------------
CREATE TABLE dim_cpi (
    cpi_key      SERIAL PRIMARY KEY,
    year_month   VARCHAR(7) NOT NULL UNIQUE,
    cpi_value    NUMERIC(10,3) NOT NULL,
    yoy_pct_change NUMERIC(6,3)
);

-- ---------------------------------------------------------------------
-- FACT: fact_movie_performance
-- ---------------------------------------------------------------------
CREATE TABLE fact_movie_performance (
    fact_key            SERIAL PRIMARY KEY,
    movie_key           INTEGER NOT NULL REFERENCES dim_movie(movie_key),
    date_key            INTEGER REFERENCES dim_date(date_key),
    cpi_key             INTEGER REFERENCES dim_cpi(cpi_key),
    avg_rating          NUMERIC(4,3),
    rating_count         INTEGER,
    revenue_usd          NUMERIC(14,2),
    budget_usd            NUMERIC(14,2),
    roi                   NUMERIC(10,4),
    rating_cluster        SMALLINT,   -- KMeans cluster label (top-rated / mid / low)
    load_batch_id          VARCHAR(40),
    loaded_at               TIMESTAMP DEFAULT now()
);

CREATE INDEX idx_fact_movie_key ON fact_movie_performance(movie_key);
CREATE INDEX idx_fact_date_key  ON fact_movie_performance(date_key);
CREATE INDEX idx_fact_cpi_key   ON fact_movie_performance(cpi_key);
CREATE INDEX idx_movie_genre_genre ON dim_movie_genre(genre_key);

-- ---------------------------------------------------------------------
-- ETL validation log — one row per pipeline run, read by Power BI /
-- the validation report so refresh quality is visible in the dashboard
-- ---------------------------------------------------------------------
CREATE TABLE etl_validation_log (
    run_id            SERIAL PRIMARY KEY,
    batch_id           VARCHAR(40) NOT NULL,
    run_timestamp       TIMESTAMP DEFAULT now(),
    check_name           VARCHAR(100) NOT NULL,
    status                VARCHAR(10) NOT NULL,   -- PASS / FAIL / WARN
    details                TEXT
);
