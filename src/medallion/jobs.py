"""Entry point for the bronze, silver and gold jobs.

Usage: python jobs.py --layer {bronze,silver,gold} --catalog CATALOG --schema SCHEMA
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from medallion import transforms as T

SOURCE_TABLE = "samples.nyctaxi.trips"


def run_bronze(spark: SparkSession, catalog: str, schema: str) -> None:
    raw = spark.read.table(SOURCE_TABLE)
    T.to_bronze(raw, SOURCE_TABLE).write.mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.bronze_trips"
    )


def run_silver(spark: SparkSession, catalog: str, schema: str) -> None:
    bronze = spark.read.table(f"{catalog}.{schema}.bronze_trips")
    valid, quarantine = T.split_valid_quarantine(bronze)
    T.to_silver(valid).write.mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.silver_trips"
    )
    quarantine.write.mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.quarantine_trips"
    )


def run_gold(spark: SparkSession, catalog: str, schema: str) -> None:
    silver = spark.read.table(f"{catalog}.{schema}.silver_trips")
    T.to_gold_daily_zone(silver).write.mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.gold_daily_pickup_zone"
    )


LAYERS = {"bronze": run_bronze, "silver": run_silver, "gold": run_gold}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", required=True, choices=LAYERS)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")
    LAYERS[args.layer](spark, args.catalog, args.schema)


if __name__ == "__main__":
    main()
