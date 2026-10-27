import pytest
from pyspark.sql import SparkSession

from medallion import transforms as T


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("medallion-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "pickup_zip",
    "dropoff_zip",
]


def make_trips(spark, rows):
    from pyspark.sql import functions as F

    df = spark.createDataFrame(rows, COLUMNS)
    return df.select(
        F.to_timestamp("tpep_pickup_datetime").alias("tpep_pickup_datetime"),
        F.to_timestamp("tpep_dropoff_datetime").alias("tpep_dropoff_datetime"),
        "trip_distance",
        "fare_amount",
        "pickup_zip",
        "dropoff_zip",
    )


def test_bronze_adds_metadata(spark):
    df = make_trips(spark, [("2024-01-01 10:00:00", "2024-01-01 10:20:00", 3.0, 12.5, 10001, 10002)])
    out = T.to_bronze(df, "src")
    assert {"_ingested_at", "_source"} <= set(out.columns)
    assert out.first()["_source"] == "src"


def test_quality_split_routes_bad_rows_with_reason(spark):
    df = make_trips(
        spark,
        [
            ("2024-01-01 10:00:00", "2024-01-01 10:20:00", 3.0, 12.5, 10001, 10002),  # good
            ("2024-01-01 10:00:00", "2024-01-01 09:00:00", 3.0, 12.5, 10001, 10002),  # negative duration
            ("2024-01-01 10:00:00", "2024-01-01 10:20:00", -1.0, 12.5, 10001, 10002),  # negative distance
            ("2024-01-01 10:00:00", "2024-01-01 10:20:00", 3.0, 9999.0, 10001, 10002),  # fare too high
            (None, "2024-01-01 10:20:00", 3.0, 12.5, 10001, 10002),  # null pickup
        ],
    )
    valid, quarantine = T.split_valid_quarantine(df)
    reasons = {r["_dq_failure"] for r in quarantine.collect()}
    assert valid.count() == 1
    assert reasons == {
        "non_positive_duration",
        "negative_distance",
        "fare_out_of_range",
        "null_pickup_ts",
    }


def test_silver_dedupes_and_derives_columns(spark):
    row = ("2024-01-01 10:00:00", "2024-01-01 10:30:00", 5.0, 20.0, 10001, 10002)
    silver = T.to_silver(make_trips(spark, [row, row]))
    assert silver.count() == 1
    first = silver.first()
    assert first["trip_minutes"] == pytest.approx(30.0)
    assert str(first["pickup_date"]) == "2024-01-01"


def test_gold_aggregates_per_day_and_zone(spark):
    rows = [
        ("2024-01-01 10:00:00", "2024-01-01 10:10:00", 2.0, 10.0, 10001, 10002),
        ("2024-01-01 11:00:00", "2024-01-01 11:30:00", 4.0, 20.0, 10001, 10003),
        ("2024-01-01 12:00:00", "2024-01-01 12:20:00", 3.0, 15.0, 10002, 10001),
    ]
    gold = T.to_gold_daily_zone(T.to_silver(make_trips(spark, rows)))
    by_zip = {r["pickup_zip"]: r for r in gold.collect()}
    assert by_zip[10001]["trip_count"] == 2
    assert by_zip[10001]["total_fare"] == 30.0
    assert by_zip[10001]["avg_fare"] == 15.0
    assert by_zip[10002]["trip_count"] == 1
