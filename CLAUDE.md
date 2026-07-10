# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An undergraduate thesis benchmark comparing three data-integration architectures — **ETL, ELT, and Hybrid** — for loading TPC-H data into a **PostgreSQL** warehouse. The single workload is **TPC-H Query 9** (product-type profit measure), run across three source file formats (**csv, jsonl, parquet**). The research measures per-phase wall-clock latency and CPU/memory usage. This is a measurement harness, not a production system — there are no automated tests or lint config; "running" means executing a pipeline scenario and inspecting timings/logs.

## The three architectures (the core mental model)

All three read the same 6 TPC-H tables (`nation, part, supplier, partsupp, orders, lineitem`) and produce the same Query 9 result (175 rows). They differ in **where the transform work happens** and **how much data crosses the Spark→Postgres boundary**:

- **ETL** (`src/etl/etl_query9.py`): PySpark does everything in-memory — extract → join/filter/aggregate → write only the final 175-row result to `result_etl.etl_query9`. Raw tables never land in Postgres. This is by far the fastest architecture.
- **ELT** (`src/elt/elt_query9.py`): PySpark extracts and bulk-loads **all raw columns, untyped** into `raw.elt_<table>` (`mode=append`). Transform runs later entirely in the database via **dbt** (`models/elt/`). Heaviest Postgres load; slowest overall.
- **Hybrid** (`src/hybrid/hybrid_query9.py`): PySpark extracts, then a **clean** step selects only the ~2–6 Query-9 columns and casts types (column pruning), then loads into `raw.hybrid_<table>`. Transform runs in **dbt** (`models/hybrid/`). Far fewer bytes hit Postgres than ELT, so ~2x faster than ELT.

The ETL/ELT/Hybrid Python scripts are near-identical in Spark setup and extraction — the meaningful differences are the presence/absence of the transform (ETL) and clean (Hybrid) phases. When editing one, check whether the change should apply to all three for a fair benchmark.

### dbt layer (ELT + Hybrid transform)

`dbt_transform/` is a single dbt project (profile name `dbt_transform`, dbt-postgres adapter). Models are split by architecture:

- `models/elt/staging/*` and `models/hybrid/staging/*` — materialized as **views**, one per raw table, doing casts/column-pruning. ELT staging casts more because ELT raw data is untyped; Hybrid staging is thinner because Spark already cleaned.
- `models/{elt,hybrid}/marts/mart_*_query9.sql` — materialized as **tables**, containing the actual Query 9 SQL. Written to schemas `result_elt` / `result_hybrid` (configured via `+schema:` in `dbt_project.yml`, which appends to the target schema).
- `models/sources.yml` maps the `raw_data` source to the `raw` Postgres schema (tables `elt_*` and `hybrid_*`).

Select a subset with `dbt run --select path:models/elt` (or `path:models/hybrid`).

## Running scenarios

### Environment prerequisites (not in git)

- **`src/.env`** — Postgres connection, read by all Spark scripts via python-dotenv: `DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS, DB_RAW_SCHEMA`.
- **`~/.dbt/profiles.yml`** — dbt Postgres profile named `dbt_transform` (gitignored). Verify with `cd dbt_transform && dbt debug`.
- **JDBC driver** is pulled automatically by Spark (`org.postgresql:postgresql:42.5.4`).
- Java 17 + `SPARK_LOCAL_IP=127.0.0.1` are required for Spark to bind on both macOS and WSL. See `README.md` for the exact `JAVA_HOME`/history-server invocations (the repo was run on macOS; paths in `README.md` and `scripts/` use Homebrew).

### Scenario control via env vars

The Spark scripts are parameterized by environment variables (used for Spark app-name / event-log naming): `FILE_FORMAT_RUN` (`csv|jsonl|parquet`, default `csv`), `RUN_ID` (default `run1`), `SCALE_FACTOR` (default `sf5`), `KEEP_UI_OPEN` (`true` holds the Spark UI at :4040 until Enter).

### Manual single-scenario run

```bash
# ETL (transform in Spark, writes result table directly)
FILE_FORMAT_RUN=parquet python src/etl/etl_query9.py

# ELT: load raw, then transform in dbt
FILE_FORMAT_RUN=parquet python src/elt/elt_query9.py
cd dbt_transform && dbt run --select path:models/elt

# Hybrid: extract+clean+load, then transform in dbt
FILE_FORMAT_RUN=parquet python src/hybrid/hybrid_query9.py
cd dbt_transform && dbt run --select path:models/hybrid
```

**Important:** ELT and Hybrid use `mode=append` when loading raw tables, so `raw.elt_*` / `raw.hybrid_*` must be **truncated between runs** or rows accumulate and corrupt results/timings. See the `truncate ... restart identity cascade` snippets in `README.md`. (ETL uses `mode=overwrite`, so it self-cleans.)

### Personal convenience tooling (NOT canonical — do not treat as a project deliverable)

`scripts/run_all_with_metrics_mac.sh` and everything it generates (`logs/`, incl. `logs/overnight/`, `logs/metrics/`, `logs/spark-logs/`, and `scripts/summarize_resources.py`'s output) are just the author's personal run helper — a convenience wrapper to batch the 9 scenarios × 3 runs on the MacBook. **Treat them as non-existent for documentation purposes:** do not describe them in the thesis, do not present the `.sh` as "the official way to run," and do not treat `logs/` as a canonical source folder. They may be deleted/regenerated at any time. The canonical way to run is the manual per-scenario commands above; the durable results artifact is the author's `thesis-docs/tpch_sf5_experiment_result_analysis.xlsx`.

## Dataset generation

Source data is TPC-H `.tbl` files from DBGEN (not in git; `dataset/` is gitignored). `dataset/convert.py` uses **DuckDB** with explicit TPC-H schemas to convert each `.tbl` into `dataset/{csv,jsonl,parquet}/<table>.<ext>` (Parquet uses Snappy). JSONL is emitted as line-delimited JSON and is read in Spark via `.format("json")` (there is no native `jsonl` format — this special case exists in every extract function).

## Data & results caveats

- The results are **scale factor SF5 (~5GB), run on a MacBook Air M3 (8GB, macOS)** — the office laptop, used because the original personal Windows laptop was underspec. The thesis draft's methodology still describes the *original* plan (SF10/SF50 on the Windows/WSL laptop); that text is stale. The current direction is to write up the SF5 + MacBook Air M3 data now, with an SF10 run to follow. Expect config/path/scale mismatches between code, tooling, and the thesis PDF.
- The durable analyzed-results artifact is `thesis-docs/tpch_sf5_experiment_result_analysis.xlsx` (the author's, formula-driven). STDEV cells read `#NAME?` because it was generated headless — recompute in Excel if needed.

## Language note

Code comments, the `README.md`, and thesis docs mix Indonesian and English. Match the surrounding language when editing a file.
