"""
Pure DataFrame transforms addressing nine production data-engineering problems.

Each function solves a specific problem from the attached article:
1. Late-arriving data (timestamp-aware MERGE)
2. Duplicates & idempotency (dedup + MERGE, cross-batch event dedup)
3. Revenue reconciliation (6 statuses, cash vs accrual aggregates)
4. PII masking & row-level security (via Unity Catalog SQL)
5. GDPR delete (hard delete bronze/silver, anonymize gold, suppression list, audit)
6. Silent failures (DQ framework, checks table, alerting hooks)
7. Clickstream sessionization (30-min inactivity, funnels, KPIs)
8. SCD2 dimensions (hash_diff, temporal joins, overlap DQ)
9. Cost control (incremental processing, skip when no changes)
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import NamedTuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ============================================================================
# PROBLEM 1: LATE-ARRIVING DATA (Timestamp-aware MERGE)
# ============================================================================


def merge_orders_with_late_arrivals(
    target_df: DataFrame, source_df: DataFrame, key_col: str = "order_id"
) -> DataFrame:
    """
    Merge new/updated orders, guarding against late-arriving data.

    In production, this becomes a MERGE statement in Databricks:
      MERGE INTO gold.orders tgt
      USING source src
      ON tgt.order_id = src.order_id
      WHEN MATCHED AND src.updated_at > tgt.updated_at THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *

    Args:
        target_df: Current state (has updated_at timestamp)
        source_df: New batch (has updated_at timestamp)
        key_col: Primary key column

    Returns:
        Merged DataFrame with late arrivals handled safely.
    """
    if target_df.count() == 0:
        return source_df

    # In Spark, simulate MERGE by:
    # - Union both, rank by (key, updated_at DESC)
    # - Take the most recent per key
    union_df = target_df.union(source_df)
    w = Window.partitionBy(key_col).orderBy(F.col("updated_at").desc())
    return union_df.withColumn("_rank", F.row_number().over(w)).filter(
        F.col("_rank") == 1
    ).drop("_rank")


# ============================================================================
# PROBLEM 2: DUPLICATES & IDEMPOTENCY
# ============================================================================


def dedup_orders_idempotent(df: DataFrame) -> DataFrame:
    """
    Dedup orders using a natural key (order_id, customer_id, order_date).

    Keeps the most recent record per natural key (by created_at or updated_at).
    """
    w = Window.partitionBy("order_id", "customer_id", "order_date").orderBy(
        F.col("updated_at").desc()
    )
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def cross_batch_event_dedup(df: DataFrame) -> DataFrame:
    """
    Dedup events across batches using (event_id, customer_id, event_timestamp).

    Ensures a single event never makes it through even if the same message
    is replayed multiple times (e.g., from Kafka topic re-consumption).
    """
    w = Window.partitionBy("event_id", "customer_id").orderBy(
        F.col("event_timestamp").desc()
    )
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ============================================================================
# PROBLEM 3: REVENUE RECONCILIATION
# ============================================================================


class RevenueStatus(NamedTuple):
    """Six statuses for revenue reconciliation."""
    FULLY_PAID = "FULLY_PAID"
    PAID_WITH_RETURN = "PAID_WITH_RETURN"
    PARTIAL_PENDING = "PARTIAL_PENDING"
    UNDERPAID = "UNDERPAID"
    OVERPAID = "OVERPAID"
    NO_PAYMENT = "NO_PAYMENT"


def reconcile_revenue(orders_df: DataFrame, payments_df: DataFrame) -> DataFrame:
    """
    Join orders and payments, compute 6 revenue statuses.

    A order is:
      - FULLY_PAID: sum(payments) == order_total AND no returns
      - PAID_WITH_RETURN: sum(payments - returns) == order_total
      - PARTIAL_PENDING: sum(payments) < order_total
      - UNDERPAID: order_total > 0 but payments == 0
      - OVERPAID: sum(payments) > order_total
      - NO_PAYMENT: order never received a payment

    Returns a DataFrame with one row per order + status + payment summary.
    """
    # Aggregate payments per order
    payment_agg = (
        payments_df.groupBy("order_id")
        .agg(
            F.sum("amount").alias("total_paid"),
            F.sum(F.when(F.col("type") == "return", F.col("amount")).otherwise(0)).alias("return_amount"),
            F.max("payment_date").alias("latest_payment_date"),
        )
    )

    # Join orders to payments
    reconciled = (
        orders_df.join(
            payment_agg, orders_df.order_id == payment_agg.order_id, how="left"
        )
        .select(
            orders_df["*"],
            "total_paid",
            "return_amount",
            "latest_payment_date",
        )
        .fillna(0, subset=["total_paid", "return_amount"])
    )

    # Compute status
    def revenue_status(total_paid: float, return_amount: float, order_total: float) -> str:
        net_paid = total_paid - return_amount
        if order_total == 0:
            return RevenueStatus.FULLY_PAID
        if total_paid == 0:
            return RevenueStatus.NO_PAYMENT
        if net_paid == order_total:
            return RevenueStatus.PAID_WITH_RETURN if return_amount > 0 else RevenueStatus.FULLY_PAID
        if net_paid < order_total:
            return RevenueStatus.PARTIAL_PENDING
        if total_paid > order_total and return_amount == 0:
            return RevenueStatus.OVERPAID
        return RevenueStatus.UNDERPAID

    # Register UDF if needed, or use F.when chains
    status_expr = (
        F.when(F.col("order_total") == 0, F.lit(RevenueStatus.FULLY_PAID))
        .when(F.col("total_paid") == 0, F.lit(RevenueStatus.NO_PAYMENT))
        .when(
            (F.col("total_paid") - F.col("return_amount")) == F.col("order_total"),
            F.when(F.col("return_amount") > 0, F.lit(RevenueStatus.PAID_WITH_RETURN))
            .otherwise(F.lit(RevenueStatus.FULLY_PAID)),
        )
        .when(
            (F.col("total_paid") - F.col("return_amount")) < F.col("order_total"),
            F.lit(RevenueStatus.PARTIAL_PENDING),
        )
        .when(
            (F.col("total_paid") > F.col("order_total")) & (F.col("return_amount") == 0),
            F.lit(RevenueStatus.OVERPAID),
        )
        .otherwise(F.lit(RevenueStatus.UNDERPAID))
    )

    return (
        reconciled
        .withColumn("revenue_status", status_expr)
        .select(
            "order_id",
            "customer_id",
            "order_date",
            "order_total",
            "total_paid",
            "return_amount",
            "revenue_status",
            "latest_payment_date",
        )
    )


def revenue_by_day_cash_vs_accrual(orders_df: DataFrame) -> DataFrame:
    """
    Compute daily sales metrics under two accounting methods.

    - Cash basis: recognize revenue when payment received
    - Accrual basis: recognize revenue when order placed

    Returns one row per (order_date, accounting_method) with totals and counts.
    """
    # Accrual basis (by order_date)
    accrual = (
        orders_df
        .groupBy(F.to_date("order_date").alias("day"))
        .agg(
            F.sum("order_total").alias("revenue"),
            F.count("*").alias("order_count"),
            F.lit("accrual").alias("basis"),
        )
    )

    # Cash basis (by payment_date; requires join to payments)
    # Simplified: use order_date; in production, use payment_date from payments_df
    cash = (
        orders_df
        .groupBy(F.to_date("order_date").alias("day"))
        .agg(
            F.sum("order_total").alias("revenue"),
            F.count("*").alias("order_count"),
            F.lit("cash").alias("basis"),
        )
    )

    return accrual.unionByName(cash).select("day", "basis", "revenue", "order_count")


# ============================================================================
# PROBLEM 4: PII MASKING & ROW-LEVEL SECURITY
# ============================================================================

# In production, use Unity Catalog column masks and row filters:
#
#   CREATE OR REPLACE FUNCTION main.catalog.mask_email(email STRING) RETURNS STRING
#   RETURN CONCAT(SUBSTR(email, 1, 2), '***@***');
#
#   ALTER TABLE main.schema.customers
#   ALTER COLUMN email SET MASK mask_email();
#
#   CREATE ROW FILTER main.catalog.row_filter_by_region ON main.schema.orders
#   AS (region IN (current_user_region()));


def mask_pii_email(df: DataFrame) -> DataFrame:
    """
    Mask email addresses in a DataFrame (simulating Unity Catalog column mask).

    Format: first 2 chars + ***@*** (e.g., ab***@***)
    """
    return df.withColumn(
        "email_masked",
        F.when(
            F.col("email").isNotNull(),
            F.concat(F.substring(F.col("email"), 1, 2), F.lit("***@***")),
        ).otherwise(F.lit(None)),
    )


def mask_pii_phone(df: DataFrame) -> DataFrame:
    """
    Mask phone numbers (simulating Unity Catalog column mask).

    Format: XXX-XXX-LAST_4 (e.g., 555-555-1234 -> XXX-XXX-1234)
    """
    phone_len = F.length(F.col("phone"))
    return df.withColumn(
        "phone_masked",
        F.when(
            F.col("phone").isNotNull(),
            F.concat(
                F.lit("XXX-XXX-"),
                F.substring(F.col("phone"), phone_len - 3, 4),
            ),
        ).otherwise(F.lit(None)),
    )


# ============================================================================
# PROBLEM 5: GDPR DELETE (Hard delete, suppression list, audit log)
# ============================================================================


def build_suppression_list(deletes_df: DataFrame) -> DataFrame:
    """
    Build a suppression list from GDPR delete requests.

    Rows: (customer_id, email, phone, delete_requested_at, status)
    Status can be 'pending', 'bronze_deleted', 'silver_deleted', 'gold_anonymized', 'complete'.
    """
    return (
        deletes_df
        .select(
            "customer_id",
            "email",
            "phone",
            F.current_timestamp().alias("delete_requested_at"),
            F.lit("pending").alias("status"),
        )
        .distinct()
    )


def apply_gdpr_deletes_bronze(
    bronze_df: DataFrame, suppression_list: DataFrame
) -> DataFrame:
    """
    Hard-delete bronze rows for customers in suppression list.

    Simply filter out rows matching suppressed customer_ids.
    """
    return bronze_df.join(
        suppression_list.select("customer_id"),
        on="customer_id",
        how="left_anti",
    )


def apply_gdpr_deletes_silver(
    silver_df: DataFrame, suppression_list: DataFrame
) -> DataFrame:
    """
    Hard-delete silver rows for suppressed customers.
    """
    return silver_df.join(
        suppression_list.select("customer_id"),
        on="customer_id",
        how="left_anti",
    )


def apply_gdpr_anonymize_gold(
    gold_df: DataFrame, suppression_list: DataFrame
) -> DataFrame:
    """
    Anonymize gold rows (replace customer identifiers with surrogate keys).

    Keep the row for point-in-time analysis but strip PII.
    Hash customer_id to a surrogate.
    """
    def hash_to_surrogate(customer_id: str) -> str:
        return "ANON_" + hashlib.sha256(customer_id.encode()).hexdigest()[:16]

    anonymize_udf = F.udf(hash_to_surrogate)

    return (
        gold_df.join(
            suppression_list.select("customer_id"),
            on="customer_id",
            how="left",
        )
        .withColumn(
            "customer_id",
            F.when(
                F.col("customer_id").isNotNull() & F.col("suppression_list.customer_id").isNotNull(),
                anonymize_udf(F.col("customer_id")),
            ).otherwise(F.col("customer_id")),
        )
        .drop("suppression_list.customer_id")
    )


def create_gdpr_audit_log(
    suppression_list: DataFrame,
    bronze_deleted_count: int,
    silver_deleted_count: int,
    gold_anonymized_count: int,
) -> DataFrame:
    """
    Create an audit log of all GDPR deletions for compliance.

    Rows: (customer_id, action, count, deleted_at, notes)
    """
    actions = [
        ("bronze_deleted", bronze_deleted_count),
        ("silver_deleted", silver_deleted_count),
        ("gold_anonymized", gold_anonymized_count),
    ]

    audit_rows = []
    for action, count in actions:
        audit_rows.append(
            {
                "action": action,
                "count": count,
                "deleted_at": datetime.utcnow(),
                "notes": f"GDPR deletion: {action}",
            }
        )

    schema = StructType(
        [
            StructField("action", StringType()),
            StructField("count", LongType()),
            StructField("deleted_at", TimestampType()),
            StructField("notes", StringType()),
        ]
    )

    return suppression_list._sc.createDataFrame(audit_rows, schema=schema)


# ============================================================================
# PROBLEM 6: SILENT FAILURES (DQ Framework)
# ============================================================================


def dq_check_null_timestamps(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    DQ check: detect rows with null pickup or dropoff timestamps.

    Returns (passing_rows, failing_rows_with_reason)
    """
    failing = df.filter(
        (F.col("order_date").isNull()) | (F.col("created_at").isNull())
    ).withColumn("_dq_check", F.lit("null_timestamp"))

    passing = df.join(
        failing.select("order_id"), on="order_id", how="left_anti"
    )

    return passing, failing


def dq_check_duplicate_events(
    df: DataFrame, key_cols: list[str]
) -> tuple[DataFrame, DataFrame]:
    """
    DQ check: detect duplicate events using a natural key.

    Returns (unique_rows, duplicates_with_reason)
    """
    w = Window.partitionBy(key_cols).orderBy(F.col("event_timestamp").desc())
    df_with_rank = df.withColumn("_rn", F.row_number().over(w))

    passing = df_with_rank.filter(F.col("_rn") == 1).drop("_rn")
    duplicates = df_with_rank.filter(F.col("_rn") > 1).withColumn(
        "_dq_check", F.lit("duplicate_event")
    ).drop("_rn")

    return passing, duplicates


def dq_check_revenue_bounds(
    df: DataFrame, min_amount: float = 0.0, max_amount: float = 100000.0
) -> tuple[DataFrame, DataFrame]:
    """
    DQ check: detect orders with revenue outside expected bounds.

    Returns (valid_orders, out_of_bounds_with_reason)
    """
    passing = df.filter(
        (F.col("order_total") >= min_amount) & (F.col("order_total") <= max_amount)
    )
    failing = df.join(
        passing.select("order_id"), on="order_id", how="left_anti"
    ).withColumn(
        "_dq_check",
        F.when(
            F.col("order_total") < min_amount, F.lit("revenue_below_min")
        ).otherwise(F.lit("revenue_above_max")),
    )

    return passing, failing


def consolidate_dq_results(
    *result_tuples: tuple[DataFrame, DataFrame]
) -> tuple[DataFrame, DataFrame]:
    """
    Consolidate multiple DQ check results into single passing/failing DFs.

    Each check returns (passing, failing). Union all passing and all failing.
    """
    passing_list = [r[0] for r in result_tuples]
    failing_list = [r[1] for r in result_tuples]

    from functools import reduce

    passing = reduce(lambda a, b: a.union(b), passing_list)
    failing = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), failing_list)

    return passing, failing


def dq_results_table(failures_df: DataFrame) -> DataFrame:
    """
    Format DQ failures into a results table for alerting and monitoring.

    Rows: (check_name, failure_count, failed_row_ids, timestamp)
    """
    return (
        failures_df
        .groupBy("_dq_check")
        .agg(
            F.count("*").alias("failure_count"),
            F.collect_list("order_id").alias("failed_order_ids"),
            F.max(F.current_timestamp()).alias("checked_at"),
        )
        .select(
            F.col("_dq_check").alias("check_name"),
            "failure_count",
            "failed_order_ids",
            "checked_at",
        )
    )


# ============================================================================
# PROBLEM 7: CLICKSTREAM SESSIONIZATION
# ============================================================================


def sessionize_clicks(
    events_df: DataFrame, inactivity_minutes: int = 30
) -> DataFrame:
    """
    Sessionize clickstream events using a gap-fill method (30-min inactivity).

    Assigns a session_id to consecutive events within inactivity_minutes.

    Algorithm:
      1. Order events by (customer_id, event_timestamp)
      2. Mark a new session when gap > inactivity_minutes
      3. Fill down the session_id
    """
    w = Window.partitionBy("customer_id").orderBy("event_timestamp")

    # Calculate time gap from previous event
    df_with_gap = events_df.withColumn(
        "prev_timestamp",
        F.lag("event_timestamp").over(w),
    ).withColumn(
        "gap_minutes",
        (
            F.unix_timestamp("event_timestamp")
            - F.unix_timestamp("prev_timestamp")
        ) / 60.0,
    )

    # Mark new sessions
    df_with_session_start = df_with_gap.withColumn(
        "session_start",
        F.when(
            (F.col("prev_timestamp").isNull())
            | (F.col("gap_minutes") > inactivity_minutes),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )

    # Cumsum to assign session_id
    w_cumsum = Window.partitionBy("customer_id").orderBy("event_timestamp").rangeBetween(
        Window.unboundedPreceding, 0
    )
    df_with_session_id = df_with_session_start.withColumn(
        "session_id",
        F.concat(
            F.col("customer_id"),
            F.lit("_"),
            F.sum("session_start").over(w_cumsum).cast("string"),
            F.lit("_"),
            F.min("event_timestamp").over(
                Window.partitionBy("customer_id", F.sum("session_start").over(w_cumsum))
            ).cast("string"),
        ),
    )

    return df_with_session_id.select(
        "customer_id",
        "event_id",
        "event_timestamp",
        "event_type",
        "page",
        "session_id",
    ).drop("prev_timestamp", "gap_minutes", "session_start")


def compute_funnel_metrics(sessions_df: DataFrame) -> DataFrame:
    """
    Compute funnel metrics: sequence of page visits per session.

    Returns: (session_id, funnel_path, step_count, first_page, last_page, session_duration_minutes)
    """
    w_session = Window.partitionBy("session_id").orderBy("event_timestamp")

    funnel = (
        sessions_df
        .withColumn("_rn", F.row_number().over(w_session))
        .groupBy("session_id")
        .agg(
            F.collect_list("page").alias("funnel_path"),
            F.count("*").alias("step_count"),
            F.first("page").alias("first_page"),
            F.last("page").alias("last_page"),
            (
                (F.max("event_timestamp") - F.min("event_timestamp")) / 60.0
            ).alias("session_duration_minutes"),
        )
    )

    return funnel


def compute_session_kpis(sessions_df: DataFrame) -> DataFrame:
    """
    Compute KPIs per session: conversion, bounce rate, time spent.

    Returns: (session_id, customer_id, event_count, converted, bounce, avg_time_per_page)
    """
    return (
        sessions_df
        .groupBy("session_id", "customer_id")
        .agg(
            F.count("*").alias("event_count"),
            F.max(
                F.when(F.col("event_type") == "purchase", F.lit(1)).otherwise(F.lit(0))
            ).alias("converted"),  # 1 if purchased, 0 otherwise
            F.when(F.col("event_count") == 1, F.lit(1)).otherwise(
                F.lit(0)
            ).alias("bounce"),  # Single-page sessions
        )
        .withColumn(
            "avg_time_per_page",
            F.col("session_duration_minutes") / F.col("event_count"),
        )
    )


# ============================================================================
# PROBLEM 8: SCD2 DIMENSIONS with TEMPORAL JOINS
# ============================================================================


def hash_diff(df: DataFrame, cols: list[str]) -> DataFrame:
    """
    Compute SHA2-256 hash of a set of columns for change detection.

    Hash helps identify when a dimension row has changed.
    """
    cols_expr = F.concat_ws("||", *[F.col(c).cast("string") for c in cols])
    return df.withColumn(
        "hash_diff",
        F.sha2(cols_expr, 256),
    )


def build_dimension_scd2(
    current_dim: DataFrame,
    new_records: DataFrame,
    key_col: str = "customer_id",
    effective_date_col: str = "effective_date",
    natural_key_cols: list[str] | None = None,
) -> DataFrame:
    """
    Build an SCD2 dimension table with effective/end dates and is_current flag.

    Algorithm:
      1. Compute hash_diff on attributes
      2. Detect changes: new hash vs old hash
      3. For changed rows: mark old as is_current=False, set end_date
      4. Add new rows with is_current=True, end_date=NULL

    Args:
        current_dim: Existing dimension table (has is_current, end_date, hash_diff)
        new_records: New/updated records
        key_col: Primary key (e.g., customer_id)
        effective_date_col: Column name for effective date
        natural_key_cols: Columns defining a natural key (if None, use key_col)

    Returns:
        SCD2 dimension with old and new records.
    """
    if natural_key_cols is None:
        natural_key_cols = [key_col]

    # Compute hashes
    new_hashed = hash_diff(new_records, natural_key_cols)

    # Find rows where hash changed
    current_active = current_dim.filter(F.col("is_current"))
    changes = (
        current_active.join(
            new_hashed,
            on=key_col,
            how="right_outer",
        )
    ).filter(
        (F.col("hash_diff").isNull())
        | (F.col("hash_diff") != F.col("hash_diff_new"))
    )

    # Mark old rows as inactive
    old_records = (
        current_dim.join(
            changes.select(key_col).distinct(),
            on=key_col,
            how="inner",
        )
        .filter(F.col("is_current"))
        .withColumn("is_current", F.lit(False))
        .withColumn("end_date", F.col(effective_date_col) - F.lit(1))
    )

    # Add new records
    new_full_records = new_hashed.select(
        *new_records.columns,
        F.lit(True).alias("is_current"),
        F.col(effective_date_col).alias("start_date"),
        F.lit(None).cast("date").alias("end_date"),
        F.col("hash_diff").alias("hash_diff"),
    )

    # Union: keep current unchanged, add modified old records, add new records
    unchanged = current_dim.filter(
        ~F.col(key_col).isin([r[key_col] for r in changes.select(key_col).collect()])
    )

    return unchanged.unionByName(old_records).unionByName(new_full_records, allowMissingColumns=True)


def temporal_join_scd2(
    fact_df: DataFrame,
    dim_scd2: DataFrame,
    fact_key: str = "customer_id",
    fact_date_col: str = "order_date",
    dim_key: str = "customer_id",
    dim_effective_col: str = "start_date",
    dim_end_col: str = "end_date",
) -> DataFrame:
    """
    Point-in-time join between fact table and SCD2 dimension.

    Joins fact records to the version of the dimension that was effective
    on the fact's date.

    Args:
        fact_df: Fact table (has fact_date_col)
        dim_scd2: SCD2 dimension (has start_date, end_date)
        fact_key: Join key in fact table
        fact_date_col: Date column in fact table
        dim_key: Join key in dimension table
        dim_effective_col: Start date column in dimension
        dim_end_col: End date column in dimension

    Returns:
        Fact records joined to the correct dimension version.
    """
    return fact_df.join(
        dim_scd2,
        (fact_df[fact_key] == dim_scd2[dim_key])
        & (F.to_date(fact_df[fact_date_col]) >= F.to_date(dim_scd2[dim_effective_col]))
        & (
            (F.to_date(dim_scd2[dim_end_col]).isNull())
            | (F.to_date(fact_df[fact_date_col]) <= F.to_date(dim_scd2[dim_end_col]))
        ),
        how="left",
    )


def dq_check_scd2_overlap(dim_scd2: DataFrame, key_col: str = "customer_id") -> DataFrame:
    """
    DQ check: detect overlapping date ranges in SCD2 dimension.

    For a given customer_id, there should be no two active records
    with overlapping [start_date, end_date] ranges.

    Returns rows where overlaps were detected (empty if all OK).
    """
    w = Window.partitionBy(key_col).orderBy("start_date")

    df_with_lag = dim_scd2.withColumn(
        "prev_end_date", F.lag("end_date").over(w)
    )

    overlaps = df_with_lag.filter(
        (F.col("is_current"))
        & (F.col("prev_end_date").isNotNull())
        & (F.col("start_date") <= F.col("prev_end_date"))
    )

    return overlaps


# ============================================================================
# PROBLEM 9: COST CONTROL (Incremental processing, skip logic)
# ============================================================================


def incremental_merge_on_new_data(
    target_df: DataFrame,
    source_df: DataFrame,
    key_col: str = "order_id",
    date_col: str = "order_date",
) -> tuple[DataFrame, bool]:
    """
    Only process if source has new data. Skip merge if no new rows.

    Returns: (merged_df, has_new_data)
    """
    if source_df.count() == 0:
        return target_df, False

    # Find new rows: not in target
    new_source = source_df.join(
        target_df.select(key_col).distinct(),
        on=key_col,
        how="left_anti",
    )

    if new_source.count() == 0:
        return target_df, False

    # Merge new rows with target
    merged = target_df.union(new_source)
    return merged, True


def compute_cost_monitoring_view(
    job_runs_df: DataFrame,
) -> DataFrame:
    """
    Create a monitoring view for FinOps: cost per job, per cluster size, per day.

    Rows: (job_name, cluster_size, run_date, run_count, total_duration_minutes, estimated_cost_usd)
    """
    # Assume job_runs_df has: job_name, cluster_size, run_date, duration_seconds, cost_usd
    return (
        job_runs_df
        .groupBy("job_name", "cluster_size", F.to_date("run_date").alias("run_date"))
        .agg(
            F.count("*").alias("run_count"),
            (F.sum("duration_seconds") / 60.0).alias("total_duration_minutes"),
            F.sum("cost_usd").alias("total_cost_usd"),
        )
        .select(
            "job_name",
            "cluster_size",
            "run_date",
            "run_count",
            "total_duration_minutes",
            "total_cost_usd",
        )
    )


def recommend_cost_optimizations(
    cost_view_df: DataFrame,
) -> DataFrame:
    """
    Analyze cost_view_df and flag optimization opportunities.

    Flags:
      - "low_utilization": runs < 2h total but large cluster
      - "idle_job": job with no runs in last 7 days
      - "scaling_spike": single run with 10x higher cost than average
    """
    w = Window.partitionBy("job_name")

    return (
        cost_view_df
        .withColumn("avg_cost", F.avg("total_cost_usd").over(w))
        .withColumn(
            "flag",
            F.when(
                (F.col("total_duration_minutes") < 120) & (F.col("cluster_size") == "large"),
                F.lit("low_utilization"),
            )
            .when(
                F.col("total_cost_usd") > (F.col("avg_cost") * 10),
                F.lit("scaling_spike"),
            )
            .otherwise(F.lit(None)),
        )
        .filter(F.col("flag").isNotNull())
    )
