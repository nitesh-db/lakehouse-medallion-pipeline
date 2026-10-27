"""
Unit tests for the nine production data-engineering problems.

Each test focuses on one problem and runs locally without a cluster.
"""

from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from medallion import transforms_nine_problems as t9


@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing."""
    return SparkSession.builder.master("local[1]").appName("test-nine-problems").getOrCreate()


# ============================================================================
# PROBLEM 1: LATE-ARRIVING DATA
# ============================================================================


def test_merge_orders_with_late_arrivals(spark):
    """Test that late-arriving data is handled correctly in MERGE."""
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_total", DoubleType()),
        StructField("updated_at", TimestampType()),
    ])

    now = datetime.utcnow()
    target_data = [
        ("ORD001", "CUST001", 100.0, now - timedelta(hours=1)),
        ("ORD002", "CUST002", 200.0, now - timedelta(hours=2)),
    ]
    target_df = spark.createDataFrame(target_data, schema=schema)

    # Same order, but with newer timestamp
    source_data = [
        ("ORD001", "CUST001", 110.0, now),  # Updated
        ("ORD003", "CUST003", 300.0, now),  # New
    ]
    source_df = spark.createDataFrame(source_data, schema=schema)

    result = t9.merge_orders_with_late_arrivals(target_df, source_df)

    assert result.count() == 3
    assert result.filter("order_id = 'ORD001'").select("order_total").collect()[0][0] == 110.0


# ============================================================================
# PROBLEM 2: DUPLICATES & IDEMPOTENCY
# ============================================================================


def test_cross_batch_event_dedup(spark):
    """Test deduplication of events across batches."""
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("event_timestamp", TimestampType()),
        StructField("event_type", StringType()),
    ])

    now = datetime.utcnow()
    data = [
        ("EVT001", "CUST001", now, "click"),
        ("EVT001", "CUST001", now, "click"),  # Duplicate
        ("EVT002", "CUST001", now + timedelta(minutes=1), "purchase"),
    ]
    df = spark.createDataFrame(data, schema=schema)

    result = t9.cross_batch_event_dedup(df)

    assert result.count() == 2
    assert result.filter("event_id = 'EVT001'").count() == 1


# ============================================================================
# PROBLEM 3: REVENUE RECONCILIATION
# ============================================================================


def test_reconcile_revenue(spark):
    """Test revenue reconciliation with 6 statuses."""
    orders_schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_date", DateType()),
        StructField("order_total", DoubleType()),
    ])

    payments_schema = StructType([
        StructField("payment_id", StringType()),
        StructField("order_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("type", StringType()),
        StructField("payment_date", DateType()),
    ])

    orders_data = [
        ("ORD001", "CUST001", datetime.utcnow().date(), 100.0),
        ("ORD002", "CUST002", datetime.utcnow().date(), 200.0),
        ("ORD003", "CUST003", datetime.utcnow().date(), 150.0),
    ]
    orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

    payments_data = [
        ("PAY001", "ORD001", 100.0, "payment", datetime.utcnow().date()),  # FULLY_PAID
        ("PAY002", "ORD002", 150.0, "payment", datetime.utcnow().date()),  # PARTIAL_PENDING
        ("PAY003", "ORD002", 60.0, "payment", datetime.utcnow().date()),   # Total 210 > 200 (OVERPAID)
    ]
    payments_df = spark.createDataFrame(payments_data, schema=payments_schema)

    result = t9.reconcile_revenue(orders_df, payments_df)

    assert result.count() >= 2
    fully_paid = result.filter("order_id = 'ORD001'").select("revenue_status").collect()
    assert fully_paid[0][0] in ["FULLY_PAID"]

    partial = result.filter("order_id = 'ORD003'").select("revenue_status").collect()
    assert partial[0][0] == "NO_PAYMENT"


def test_revenue_by_day_cash_vs_accrual(spark):
    """Test cash vs accrual revenue aggregation."""
    orders_schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_date", TimestampType()),
        StructField("order_total", DoubleType()),
    ])

    now = datetime.utcnow()
    orders_data = [
        ("ORD001", "CUST001", now, 100.0),
        ("ORD002", "CUST002", now, 200.0),
    ]
    orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

    result = t9.revenue_by_day_cash_vs_accrual(orders_df)

    assert result.count() == 2  # One row per basis type
    bases = result.select("basis").rdd.flatMap(lambda x: x).collect()
    assert "accrual" in bases
    assert "cash" in bases


# ============================================================================
# PROBLEM 4: PII MASKING
# ============================================================================


def test_mask_pii_email(spark):
    """Test email PII masking."""
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("email", StringType()),
    ])

    data = [
        ("CUST001", "john@example.com"),
        ("CUST002", None),
    ]
    df = spark.createDataFrame(data, schema=schema)

    result = t9.mask_pii_email(df)

    masked = result.filter("customer_id = 'CUST001'").select("email_masked").collect()[0][0]
    assert masked.startswith("jo")
    assert "***@***" in masked


def test_mask_pii_phone(spark):
    """Test phone PII masking."""
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("phone", StringType()),
    ])

    data = [
        ("CUST001", "5551234567"),
    ]
    df = spark.createDataFrame(data, schema=schema)

    result = t9.mask_pii_phone(df)

    masked = result.select("phone_masked").collect()[0][0]
    assert masked.endswith("4567")


# ============================================================================
# PROBLEM 5: GDPR DELETE
# ============================================================================


def test_build_suppression_list(spark):
    """Test creation of GDPR suppression list."""
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
    ])

    data = [
        ("CUST001", "john@example.com", "5551234567"),
        ("CUST002", "jane@example.com", "5559876543"),
    ]
    df = spark.createDataFrame(data, schema=schema)

    result = t9.build_suppression_list(df)

    assert result.count() == 2
    assert result.filter("customer_id = 'CUST001'").select("status").collect()[0][0] == "pending"


def test_apply_gdpr_deletes_bronze(spark):
    """Test hard deletion from bronze layer."""
    bronze_schema = StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_total", DoubleType()),
    ])

    suppress_schema = StructType([
        StructField("customer_id", StringType()),
    ])

    bronze_data = [
        ("ORD001", "CUST001", 100.0),
        ("ORD002", "CUST002", 200.0),
        ("ORD003", "CUST001", 150.0),
    ]
    bronze_df = spark.createDataFrame(bronze_data, schema=bronze_schema)

    suppress_data = [("CUST001",)]
    suppress_df = spark.createDataFrame(suppress_data, schema=suppress_schema)

    result = t9.apply_gdpr_deletes_bronze(bronze_df, suppress_df)

    assert result.count() == 1
    assert result.select("customer_id").collect()[0][0] == "CUST002"


# ============================================================================
# PROBLEM 6: SILENT FAILURES (DQ FRAMEWORK)
# ============================================================================


def test_dq_check_null_timestamps(spark):
    """Test DQ check for null timestamps."""
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("order_date", DateType()),
        StructField("created_at", TimestampType()),
    ])

    now = datetime.utcnow()
    data = [
        ("ORD001", datetime.utcnow().date(), now),
        ("ORD002", None, now),  # Null order_date
        ("ORD003", datetime.utcnow().date(), None),  # Null created_at
    ]
    df = spark.createDataFrame(data, schema=schema)

    passing, failing = t9.dq_check_null_timestamps(df)

    assert passing.count() == 1
    assert failing.count() == 2


def test_dq_check_duplicate_events(spark):
    """Test DQ check for duplicate events."""
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("event_timestamp", TimestampType()),
    ])

    now = datetime.utcnow()
    data = [
        ("EVT001", "CUST001", now),
        ("EVT001", "CUST001", now),  # Duplicate
        ("EVT002", "CUST001", now + timedelta(minutes=1)),
    ]
    df = spark.createDataFrame(data, schema=schema)

    passing, duplicates = t9.dq_check_duplicate_events(df, ["event_id", "customer_id"])

    assert passing.count() == 2
    assert duplicates.count() == 1


def test_dq_results_table(spark):
    """Test consolidation of DQ failures into a results table."""
    failures_schema = StructType([
        StructField("order_id", StringType()),
        StructField("_dq_check", StringType()),
    ])

    data = [
        ("ORD001", "null_timestamp"),
        ("ORD002", "null_timestamp"),
        ("ORD003", "duplicate_event"),
    ]
    df = spark.createDataFrame(data, schema=failures_schema)

    result = t9.dq_results_table(df)

    assert result.count() == 2
    assert result.filter("check_name = 'null_timestamp'").select("failure_count").collect()[0][0] == 2


# ============================================================================
# PROBLEM 7: CLICKSTREAM SESSIONIZATION
# ============================================================================


def test_sessionize_clicks(spark):
    """Test clickstream sessionization with 30-min inactivity."""
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("event_id", StringType()),
        StructField("event_timestamp", TimestampType()),
        StructField("event_type", StringType()),
        StructField("page", StringType()),
    ])

    now = datetime.utcnow()
    data = [
        ("CUST001", "EVT001", now, "click", "home"),
        ("CUST001", "EVT002", now + timedelta(minutes=5), "click", "product"),
        ("CUST001", "EVT003", now + timedelta(minutes=40), "click", "home"),  # New session (30+ min gap)
    ]
    df = spark.createDataFrame(data, schema=schema)

    result = t9.sessionize_clicks(df, inactivity_minutes=30)

    assert result.count() == 3
    # First two events should have same session_id, third should be different
    sessions = result.select("session_id").distinct().collect()
    assert len(sessions) == 2


def test_compute_funnel_metrics(spark):
    """Test funnel path computation."""
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("session_id", StringType()),
        StructField("event_timestamp", TimestampType()),
        StructField("page", StringType()),
    ])

    now = datetime.utcnow()
    data = [
        ("CUST001", "SES001", now, "home"),
        ("CUST001", "SES001", now + timedelta(minutes=1), "product"),
        ("CUST001", "SES001", now + timedelta(minutes=2), "checkout"),
    ]
    df = spark.createDataFrame(data, schema=schema)

    result = t9.compute_funnel_metrics(df)

    assert result.count() == 1
    funnel_path = result.select("funnel_path").collect()[0][0]
    assert len(funnel_path) == 3
    assert funnel_path[0] == "home"
    assert funnel_path[-1] == "checkout"


# ============================================================================
# PROBLEM 8: SCD2 DIMENSIONS
# ============================================================================


def test_hash_diff(spark):
    """Test hash_diff computation for SCD2 change detection."""
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
    ])

    data = [
        ("CUST001", "John Doe", "john@example.com"),
        ("CUST002", "Jane Doe", "jane@example.com"),
    ]
    df = spark.createDataFrame(data, schema=schema)

    result = t9.hash_diff(df, ["name", "email"])

    assert result.count() == 2
    hashes = result.select("hash_diff").distinct().collect()
    assert len(hashes) == 2


def test_dq_check_scd2_overlap(spark):
    """Test DQ check for overlapping SCD2 date ranges."""
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("name", StringType()),
        StructField("start_date", DateType()),
        StructField("end_date", DateType()),
        StructField("is_current", BooleanType()),
    ])

    today = datetime.utcnow().date()
    data = [
        ("CUST001", "John Doe v1", today - timedelta(days=10), today - timedelta(days=1), False),
        ("CUST001", "John Doe v2", today - timedelta(days=5), None, True),  # Overlaps with v1
    ]
    df = spark.createDataFrame(data, schema=schema)

    result = t9.dq_check_scd2_overlap(df)

    assert result.count() == 1  # One overlap detected


# ============================================================================
# PROBLEM 9: COST CONTROL
# ============================================================================


def test_incremental_merge_on_new_data(spark):
    """Test cost control: skip merge if no new data."""
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("order_total", DoubleType()),
    ])

    target_data = [("ORD001", 100.0), ("ORD002", 200.0)]
    target_df = spark.createDataFrame(target_data, schema=schema)

    # Source with only existing orders: no new data
    source_data = [("ORD001", 100.0)]
    source_df = spark.createDataFrame(source_data, schema=schema)

    result, has_new = t9.incremental_merge_on_new_data(target_df, source_df)

    assert has_new is False
    assert result.count() == 2  # Unchanged

    # Source with new order
    new_source_data = [("ORD003", 300.0)]
    new_source_df = spark.createDataFrame(new_source_data, schema=schema)

    result2, has_new2 = t9.incremental_merge_on_new_data(target_df, new_source_df)

    assert has_new2 is True
    assert result2.count() == 3


def test_compute_cost_monitoring_view(spark):
    """Test FinOps cost monitoring view."""
    schema = StructType([
        StructField("job_name", StringType()),
        StructField("cluster_size", StringType()),
        StructField("run_date", TimestampType()),
        StructField("duration_seconds", LongType()),
        StructField("cost_usd", DoubleType()),
    ])

    now = datetime.utcnow()
    data = [
        ("job_etl", "small", now, 3600, 2.50),
        ("job_etl", "small", now + timedelta(hours=1), 1800, 1.25),
        ("job_ml", "large", now, 7200, 15.00),
    ]
    df = spark.createDataFrame(data, schema=schema)

    result = t9.compute_cost_monitoring_view(df)

    assert result.count() == 2
    etl_cost = result.filter("job_name = 'job_etl'").select("total_cost_usd").collect()[0][0]
    assert etl_cost == pytest.approx(3.75, rel=0.01)
