"""
extract_movielens.py
=====================
Extract stage of the MoviAnalytics pipeline.

Primary path: download "The Movies Dataset" (MovieLens 20M + TMDB metadata)
from Kaggle via the official `kaggle` CLI/API, which is what the resume
bullet refers to (45,000+ movie records, budget/revenue/genre fields).

Fallback path: if no Kaggle API credentials are present (e.g. first run,
CI, or a reviewer spinning this up without a Kaggle account), generate a
synthetic dataset that is schema-identical and statistically realistic
(same columns, same row-count ballpark, same 20+ year date range), so the
rest of the pipeline (transform/load/validate/dashboard) runs unchanged.

Output: data/raw/movies_metadata.csv, data/raw/ratings.csv
"""
import os
import csv
import random
import logging
from datetime import date
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("extract_movielens")

RAW_DIR = Path(os.environ.get("MOVIANALYTICS_DATA_DIR", "/opt/airflow/data/raw"))
N_MOVIES = int(os.environ.get("N_MOVIES", 45000))

GENRES = [
    "Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary",
    "Drama", "Family", "Fantasy", "History", "Horror", "Music", "Mystery",
    "Romance", "Science Fiction", "Thriller", "War", "Western",
]

LANGUAGES = ["en", "fr", "es", "hi", "ja", "de", "ko", "it", "zh", "ru"]


def try_kaggle_download() -> bool:
    """
    Attempt the real download via the Kaggle API.
    Requires KAGGLE_USERNAME / KAGGLE_KEY env vars (or ~/.kaggle/kaggle.json)
    set on the machine running this DAG. Returns True on success.
    """
    try:
        import kaggle  # noqa: F401  -- import itself fails without credentials
    except Exception as e:
        logger.warning("Kaggle API not available/configured (%s). Falling back to synthetic data.", e)
        return False

    try:
        from kaggle.api.kaggle_api_extended import KaggleApi

        api = KaggleApi()
        api.authenticate()
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading 'rounakbanik/the-movies-dataset' from Kaggle ...")
        api.dataset_download_files(
            "rounakbanik/the-movies-dataset", path=str(RAW_DIR), unzip=True
        )
        # The Kaggle archive ships movies_metadata.csv and ratings.csv already
        # named correctly, so no renaming is needed.
        return (RAW_DIR / "movies_metadata.csv").exists()
    except Exception as e:
        logger.warning("Kaggle download failed (%s). Falling back to synthetic data.", e)
        return False


def generate_synthetic_dataset():
    """Schema-identical stand-in for movies_metadata.csv + ratings.csv."""
    random.seed(42)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    movies_path = RAW_DIR / "movies_metadata.csv"
    ratings_path = RAW_DIR / "ratings.csv"

    logger.info("Generating synthetic dataset: %s movies", N_MOVIES)

    start_year, end_year = 2000, 2023  # 20+ year span, matches resume claim

    with open(movies_path, "w", newline="", encoding="utf-8") as mf:
        writer = csv.writer(mf)
        writer.writerow([
            "movie_id", "title", "release_date", "runtime", "budget",
            "revenue", "popularity", "original_language", "genres",
        ])
        for movie_id in range(1, N_MOVIES + 1):
            year = random.randint(start_year, end_year)
            month = random.randint(1, 12)
            day = random.randint(1, 28)
            release_date = date(year, month, day).isoformat()

            runtime = max(60, int(random.gauss(105, 22)))
            # Budget/revenue are heavily right-skewed in real movie data
            budget = max(0, int(random.lognormvariate(15.5, 1.4)))
            revenue_multiplier = random.lognormvariate(0.6, 1.1)
            revenue = int(budget * revenue_multiplier) if budget > 0 else int(random.lognormvariate(13, 1.6))
            popularity = round(max(0.1, random.lognormvariate(1.2, 0.9)), 4)
            lang = random.choices(LANGUAGES, weights=[55, 8, 8, 6, 6, 5, 4, 4, 2, 2])[0]
            n_genres = random.randint(1, 3)
            movie_genres = random.sample(GENRES, n_genres)

            writer.writerow([
                movie_id,
                f"Synthetic Movie #{movie_id}",
                release_date,
                runtime,
                budget,
                revenue,
                popularity,
                lang,
                "|".join(movie_genres),
            ])

    # Ratings: ~80 ratings/movie on average (skewed - popular movies get more)
    with open(ratings_path, "w", newline="", encoding="utf-8") as rf:
        writer = csv.writer(rf)
        writer.writerow(["movie_id", "user_id", "rating"])
        for movie_id in range(1, N_MOVIES + 1):
            n_ratings = max(1, int(random.lognormvariate(4.0, 1.1)))
            quality = random.gauss(3.4, 0.7)  # this movie's "true" quality
            for _ in range(n_ratings):
                user_id = random.randint(1, 50000)
                rating = min(5.0, max(0.5, round(random.gauss(quality, 0.6) * 2) / 2))
                writer.writerow([movie_id, user_id, rating])

    logger.info("Synthetic data written to %s and %s", movies_path, ratings_path)


def main():
    if not try_kaggle_download():
        generate_synthetic_dataset()
    logger.info("Extract (MovieLens) stage complete.")


if __name__ == "__main__":
    main()
