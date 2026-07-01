"""
transform_spark.py
====================
Transform stage of the MoviAnalytics pipeline, run with PySpark.

Reads the raw CSVs produced by the extract scripts, cleans and joins them,
aggregates ratings per movie, derives ROI, and runs a small PySpark MLlib
KMeans model to bucket movies into rating clusters (the "top-rated movie
clusters" bullet on the resume).

Output: Parquet files under data/processed/ ready for the load stage.
"""
import os
import logging
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("transform_spark")

RAW_DIR = Path(os.environ.get("MOVIANALYTICS_DATA_DIR", "/opt/airflow/data/raw"))
PROCESSED_DIR = Path(os.environ.get("MOVIANALYTICS_PROCESSED_DIR", "/opt/airflow/data/processed"))


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("MoviAnalytics-Transform")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def load_raw(spark: SparkSession):
    movies = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(str(RAW_DIR / "movies_metadata.csv"))
    )
    ratings = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(str(RAW_DIR / "ratings.csv"))
    )
    cpi = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(str(RAW_DIR / "cpi.csv"))
    )
    return movies, ratings, cpi


def clean_movies(movies):
    movies = movies.dropDuplicates(["movie_id"])
    movies = movies.filter(F.col("movie_id").isNotNull() & F.col("title").isNotNull())
    movies = movies.withColumn("release_date", F.to_date("release_date"))
    # Null out implausible budget/revenue rather than dropping the row,
    # since many real-world MovieLens/TMDB rows have 0/missing financials.
    movies = movies.withColumn(
        "budget", F.when(F.col("budget") > 1000, F.col("budget")).otherwise(F.lit(None))
    ).withColumn(
        "revenue", F.when(F.col("revenue") > 1000, F.col("revenue")).otherwise(F.lit(None))
    )
    movies = movies.withColumn("runtime", F.when(F.col("runtime").between(1, 600), F.col("runtime")))
    return movies


def explode_genres(movies):
    genre_long = (
        movies.select("movie_id", F.explode(F.split(F.col("genres"), "\\|")).alias("genre_name"))
        .withColumn("genre_name", F.trim(F.col("genre_name")))
        .filter(F.col("genre_name") != "")
    )
    return genre_long


def aggregate_ratings(ratings):
    return ratings.groupBy("movie_id").agg(
        F.avg("rating").alias("avg_rating"),
        F.count("rating").alias("rating_count"),
    )


def add_rating_clusters(movie_facts):
    """KMeans on [avg_rating, rating_count, popularity] -> 3 clusters,
    relabeled by mean avg_rating so cluster 2 is always 'top-rated'."""
    feature_cols = ["avg_rating", "rating_count", "popularity"]
    ready = movie_facts.dropna(subset=feature_cols)

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    kmeans = KMeans(featuresCol="features", predictionCol="raw_cluster", k=3, seed=42)

    vec = assembler.transform(ready)
    scaler_model = scaler.fit(vec)
    scaled = scaler_model.transform(vec)
    model = kmeans.fit(scaled)
    clustered = model.transform(scaled)

    # Re-rank clusters by avg_rating ascending so labels are stable/interpretable
    cluster_means = (
        clustered.groupBy("raw_cluster").agg(F.avg("avg_rating").alias("m"))
        .orderBy("m").collect()
    )
    relabel = {row["raw_cluster"]: i for i, row in enumerate(cluster_means)}
    relabel_udf = F.udf(lambda c: relabel.get(c, None), T.IntegerType())

    clustered = clustered.withColumn("rating_cluster", relabel_udf(F.col("raw_cluster")))
    return clustered.select("movie_id", "rating_cluster")


def main():
    spark = get_spark()
    logger.info("Spark session started: %s", spark.version)

    movies_raw, ratings_raw, cpi = load_raw(spark)

    movies = clean_movies(movies_raw)
    genre_long = explode_genres(movies)
    rating_agg = aggregate_ratings(ratings_raw)

    movie_facts = (
        movies.join(rating_agg, on="movie_id", how="left")
        .withColumn(
            "roi",
            F.when(
                (F.col("budget").isNotNull()) & (F.col("budget") > 0) & (F.col("revenue").isNotNull()),
                (F.col("revenue") - F.col("budget")) / F.col("budget"),
            ),
        )
        .withColumn("year_month", F.date_format("release_date", "yyyy-MM"))
    )

    clusters = add_rating_clusters(movie_facts)
    movie_facts = movie_facts.join(clusters, on="movie_id", how="left")

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    movie_facts.write.mode("overwrite").parquet(str(PROCESSED_DIR / "movie_facts"))
    genre_long.write.mode("overwrite").parquet(str(PROCESSED_DIR / "movie_genres"))
    cpi.write.mode("overwrite").parquet(str(PROCESSED_DIR / "cpi"))

    logger.info(
        "Transform complete: %d movies, %d genre rows, %d CPI rows",
        movie_facts.count(), genre_long.count(), cpi.count(),
    )
    spark.stop()


if __name__ == "__main__":
    main()
