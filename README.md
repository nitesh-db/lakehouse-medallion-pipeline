# lakehouse-medallion-pipeline

A bronze / silver / gold pipeline on Databricks and Delta Lake, deployed with Databricks Asset Bundles and tested in CI.

It processes the public `samples.nyctaxi.trips` dataset that ships with Databricks workspaces, so you can run it without any private data.

## What it demonstrates

- Medallion architecture on Delta tables in Unity Catalog
- A data quality layer that quarantines bad rows with a failure reason instead of dropping them
- Business logic written as pure PySpark functions, unit-tested locally with no cluster
- Infrastructure as code with Asset Bundles (dev and prod targets)
- CI with lint and tests on every PR, plus a manual deploy workflow

See [docs/architecture.md](docs/architecture.md) for the diagram and design decisions.

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

## Roadmap

- [ ] Incremental silver and gold with `MERGE`
- [ ] Lakeflow Declarative Pipelines version with expectations
- [ ] Row-count and rejection-rate monitoring table
- [ ] Unity Catalog grants as code

## License

MIT
