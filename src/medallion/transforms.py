"""Pure DataFrame transforms for the medallion pipeline.

Every function takes a DataFrame and returns a DataFrame, with no I/O,
so each one can be unit-tested locally without a Databricks workspace.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

MAX_TRIP_MINUTES = 6 * 60
MAX_FARE = 500.0


def to_bronze(df: DataFrame, source: str) -> DataFrame:
    """Land raw data as-is and add ingestion metadata."""
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_source", F.lit(source)
    )


def with_quality_flags(df: DataFrame) -> DataFrame:
    """Attach a `_dq_failure` column. NULL means the row passed every rule."""
    minutes = (
        F.unix_timestamp("tpep_dropoff_datetime")
        - F.unix_timestamp("tpep_pickup_datetime")
    ) / 60.0
    failure = (
        F.when(F.col("tpep_pickup_datetime").isNull(), F.lit("null_pickup_ts"))
        .when(F.col("tpep_dropoff_datetime").isNull(), F.lit("null_dropoff_ts"))
        .when(minutes <= 0, F.lit("non_positive_duration"))
        .when(minutes > MAX_TRIP_MINUTES, F.lit("duration_too_long"))
        .when(F.col("trip_distance") < 0, F.lit("negative_distance"))
        .when(
            (F.col("fare_amount") < 0) | (F.col("fare_amount") > MAX_FARE),
            F.lit("fare_out_of_range"),
        )
    )
    return df.withColumn("_dq_failure", failure)


def split_valid_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid, quarantine). Quarantined rows keep the failure reason."""
    flagged = with_quality_flags(df)
    valid = flagged.filter(F.col("_dq_failure").isNull()).drop("_dq_failure")
    quarantine = flagged.filter(F.col("_dq_failure").isNotNull())
    return valid, quarantine


def to_silver(valid: DataFrame) -> DataFrame:
    """Dedupe and derive analytics-ready columns from validated rows."""
    return (
        valid.dropDuplicates(
            [
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "pickup_zip",
                "dropoff_zip",
                "fare_amount",
            ]
        )
        .withColumn(
            "trip_minutes",
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            )
            / 60.0,
        )
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    )


def to_gold_daily_zone(silver: DataFrame) -> DataFrame:
    """Daily KPIs per pickup zip code."""
    return (
        silver.groupBy("pickup_date", "pickup_zip")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
            F.round(F.avg("trip_minutes"), 2).alias("avg_trip_minutes"),
        )
        .withColumn("_computed_at", F.current_timestamp())
    )
