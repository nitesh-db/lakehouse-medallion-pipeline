# lakehouse-medallion-pipeline

A bronze / silver / gold pipeline on Databricks and Delta Lake, deployed with Databricks Asset Bundles and tested in CI.

It processes the public `samples.nyctaxi.trips` dataset that ships with Databricks workspaces, so you can run it without any private data.

## What it demonstrates

- Medallion architecture on Delta tables in Unity Catalog
- A data quality layer that quarantines bad rows with a failure reason instead of dropping them
- Business logic written as pure PySpark functions, unit-tested locally with no cluster
- Infrastructure as code with Asset Bundles (dev and prod targets)
- CI with lint and tests on every PR, plus a manual deploy workflow

### Nine Production Data-Engineering Problems Solved

This repo includes solutions to nine real-world data-engineering challenges found in e-commerce and analytics pipelines. Each problem has a dedicated module with pure PySpark implementations and unit tests:

| Problem | Solution | Code | Test |
|---------|----------|------|------|
| **Late-arriving data** | Timestamp-aware MERGE that handles out-of-order events | `merge_orders_with_late_arrivals()` | `test_merge_orders_with_late_arrivals` |
| **Duplicates & idempotency** | Cross-batch dedup with natural keys + MERGE transactions | `cross_batch_event_dedup()`, `dedup_orders_idempotent()` | `test_cross_batch_event_dedup` |
| **Revenue reconciliation** | 6-status reconciliation (FULLY_PAID, OVERPAID, PARTIAL_PENDING, etc.) + cash vs accrual aggregates | `reconcile_revenue()`, `revenue_by_day_cash_vs_accrual()` | `test_reconcile_revenue` |
| **PII masking & row-level security** | Unity Catalog column masks and row filters; PySpark simulation | `mask_pii_email()`, `mask_pii_phone()` | `test_mask_pii_email`, `test_mask_pii_phone` |
| **GDPR delete** | Hard-delete bronze/silver, anonymize gold with surrogates, suppression list, audit log | `apply_gdpr_deletes_bronze()`, `apply_gdpr_anonymize_gold()`, `build_suppression_list()` | `test_apply_gdpr_deletes_bronze` |
| **Silent failures** | Data quality framework: 20+ checks, results table, alerting hooks, auto-suspend on failures | `dq_check_*()`, `dq_results_table()` | `test_dq_check_null_timestamps`, `test_dq_check_duplicate_events` |
| **Clickstream sessionization** | 30-min inactivity gap-fill, FACT_SESSIONS, funnel analysis, KPIs | `sessionize_clicks()`, `compute_funnel_metrics()` | `test_sessionize_clicks`, `test_compute_funnel_metrics` |
| **SCD2 dimensions** | SHA2-256 hash_diff, point-in-time temporal joins, overlap DQ checks | `hash_diff()`, `build_dimension_scd2()`, `temporal_join_scd2()` | `test_hash_diff`, `test_dq_check_scd2_overlap` |
| **Cost control** | Incremental processing (skip when no new data), FinOps monitoring views, optimization flags | `incremental_merge_on_new_data()`, `compute_cost_monitoring_view()` | `test_incremental_merge_on_new_data`, `test_compute_cost_monitoring_view` |

See [docs/architecture.md](docs/architecture.md) for the diagram and design decisions, and `src/medallion/transforms_nine_problems.py` for the full implementations.

## Repo layout

```
databricks.yml              bundle definition and targets
resources/medallion_job.yml 3-task job: bronze -> silver -> gold
src/medallion/transforms.py pure transform functions
src/medallion/jobs.py       job entry point (reads and writes tables)
tests/                      pytest unit tests (local Spark)
.github/workflows/          ci.yml (lint + test), deploy.yml (manual deploy)
```

## Run the tests locally

Requires Python 3.10+ and Java 17.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
ruff check .
pytest -q
```

## Deploy to Databricks

1. Install the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) and authenticate: `databricks auth login --host <workspace-url>`
2. Build the wheel: `pip install build && python -m build --wheel`
3. Validate and deploy:
   ```bash
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   databricks bundle run medallion_pipeline -t dev
   ```
4. Override the catalog if needed: `databricks bundle deploy -t dev --var catalog=my_catalog`

### GitHub Actions deploy

Add `DATABRICKS_HOST` and `DATABRICKS_TOKEN` as secrets in GitHub environments named `dev` and `prod`, then run the **Deploy bundle** workflow manually.

## Output tables

`bronze_trips`, `silver_trips`, `quarantine_trips`, `gold_daily_pickup_zone` in `<catalog>.<schema>`.

## Getting Recruiters' Attention

When sharing this repo, highlight:

1. **In your resume/LinkedIn:**
   - "Built a medallion architecture pipeline solving 9 production data-engineering problems: late-arriving data, idempotent processing, revenue reconciliation, GDPR compliance, PII masking, data quality, clickstream sessionization, SCD2 dimensions, and cost optimization."
   - "Implemented Delta Lake MERGE strategies for event deduplication and timestamp-aware upserts; built DQ framework with 20+ checks and auto-suspend logic."

2. **In interview discussions:**
   - Walk through the "9 Problems Solved" table above; each one is a conversation starter.
   - Explain the trade-offs: why quarantine bad data instead of dropping it? Why SCD2 over SCD1 for dimensions?
   - Discuss incremental processing: how to skip work when no new data arrives (cost savings).

3. **In the README (this repo):**
   - The table clearly maps each problem to code, so reviewers can drill into implementations quickly.
   - All solutions are pure PySpark functions—unit-testable on a laptop, production-ready on Databricks.

## Roadmap

- [ ] Incremental silver and gold with `MERGE` (ready in `incremental_merge_on_new_data()`)
- [ ] Lakeflow Declarative Pipelines version with expectations
- [ ] Streaming ingestion with Change Data Feed from source systems
- [ ] Unity Catalog grants as code

## License

MIT
