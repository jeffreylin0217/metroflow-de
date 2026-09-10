# MetroFlow v1 final engineering audit

Measured on macOS-26.6-arm64-arm-64bit, Python 3.12.14, DuckDB 1.5.5. Evidence: [real_validation.json](measurements/real_validation.json). These are single-machine observed runtimes, not service-level guarantees.

## Architecture and actual technologies

Official sources → Python/requests + SQLite manifest → immutable raw → PyArrow monthly Bronze Parquet → DuckDB external views → dbt staging → dimensional fact/dimensions → tested daily marts → CSV/Parquet reporting. Python, SQL, Parquet, PyArrow, DuckDB, dbt-core/dbt-duckdb, SQLite, pytest, Git/GitHub and GitHub Actions are used.

## Sources, dates and counts

Official NYC TLC Yellow Taxi trip records, official TLC zone lookup (265 rows), and NOAA NCEI daily-summaries for Central Park USW00094728 (181 daily observations). Trip source range: **2025-01 through 2025-06**; analytical pickup dates **2025-01-01 through 2025-06-30**. See [official source URLs and contracts](SOURCES.md).

| Source month | Source trip rows | Raw trip bytes |
|---|---:|---:|
| 2025-01 | 3,475,226 | 59,158,238 |
| 2025-02 | 3,577,543 | 60,343,086 |
| 2025-03 | 4,145,257 | 69,964,745 |
| 2025-04 | 3,970,553 | 67,352,824 |
| 2025-05 | 4,591,845 | 77,837,865 |
| 2025-06 | 4,322,960 | 73,542,954 |

- Exact source and Bronze trip count: **24,083,384**.
- Retained fact count (ACCEPTED plus SUSPICIOUS): **24,082,454**.
- Strict ACCEPTED classification: **17,898,515**.
- SUSPICIOUS but retained: **6,183,939**.
- INVALID and excluded from fact: **930**.
- Six TLC partitions and six daily-weather source months processed; one reference source.

All NOAA precipitation/min/max-temperature dates in this measured range have values, and no returned NOAA quality flag rejected a value. Source flags and missingness remain part of the contract; coverage is not assumed for future runs.

## Storage and runtime

| Storage category | Bytes | MiB |
|---|---:|---:|
| Immutable raw (all three sources) | 408,233,903 | 389.32 |
| Bronze Parquet | 489,969,889 | 467.27 |
| Published warehouse | 1,103,376,384 | 1052.26 |
| Current reporting generation | 4,693,160 | 4.48 |

Processed storage (Bronze + warehouse + current reporting): **1,598,039,433 bytes**. Temporary candidate databases, Python environment and older reporting generations are excluded; peak disk usage is higher.

| Measured run | Seconds | Work |
|---|---:|---|
| First six-month run | 175.397 | Ten new files; January + zones already cached; all six partitions built |
| Unchanged rerun | 0.332 | Thirteen local sources verified; zero partitions rebuilt; dbt skipped |
| Forced March backfill | 18.664 | March replaced; marts and tests rerun |
| Full refresh | 24.553 | All loaded history rebuilt from cached raw; dbt build + test + exports |

The first six-month measurement is **not** a cold-download benchmark. Full refresh includes Bronze, warehouse, tests and reporting, but no network download. The source manifest gives exact per-file provenance, sizes, hashes and schema fingerprints.

## Models and grain

Ten dbt models: stg_taxi_trips, stg_zones, stg_weather, dim_date, dim_zone, fct_trips, mart_zone_daily, mart_borough_daily, mart_weather_demand, mart_quality.

| Materialized Gold model | Rows |
|---|---:|
| dim_date | 181 |
| dim_zone | 265 |
| mart_zone_daily | 43,732 |
| mart_borough_daily | 1,448 |
| mart_weather_demand | 181 |
| mart_quality | 18 |
| fct_trips | 24,082,454 |

Staging trips: source record, key source SHA plus row ordinal. Zones: location key. Weather: station date. Fact: retained source record. Date dimension: calendar date. Zone mart: date × pickup zone. Borough mart: date × borough. Weather-demand mart: date. Quality mart: source month × status. Descriptions, keys, assumptions and upstream references are also stored in dbt schema.yml.

## Incremental, idempotency, backfills and schema behavior

The manifest audits downloaded versions; successful state tracks raw/Bronze hashes and implementation fingerprint. Unchanged runs skip work. Adding months ingests new partitions and updates the fact incrementally. Changed fact months are deleted and reinserted transactionally, including when replacement produces zero rows. Small marts and global checks rebuild or scan the full loaded history.

Force-month uses local immutable bytes; check-remote retrieves publisher candidates and preserves prior versions; full-refresh rebuilds all loaded months. Raw checksums are verified on skips, and duplicate source bytes across months are rejected. Required names and type families are validated; safe numeric/timestamp casts support compatible changes. All TLC schemas are fingerprinted, while documented optional fields remain preserved in raw.

The unchanged real rerun, forced March backfill and full refresh passed equivalence checks. Fixture comparison uses exact sorted rows. Real fact evidence uses per-partition counts and order-independent sums of full-row hashes; every small mart cell is compared, with exact equality except floating averages (relative tolerance 1e-12, absolute 1e-9). Maximum observed float deltas: backfill 1.8189894035458565e-12, full refresh 1.8189894035458565e-12. Hash sums are bounded-memory evidence rather than a mathematical collision-free proof. Tiny aggregate-order effects are documented instead of claiming byte-identical floating averages.

## Tests, warnings and CI

**21 pytest tests pass**, including actual offline dbt lifecycle runs. Coverage includes schema drift, checksums, source-version changes, failed/invalid downloads, duplicate source detection, source identity, dimension uniqueness, weather dates/flags, writer locking, invalid timestamps, incrementals, idempotency, forced/empty backfills, full-refresh equivalence, export schemas, run metadata and publication failure recovery.

Real **dbt build succeeds** with 10 models and 38 tests. Separate **dbt test succeeds: 37 PASS, 1 WARN, 0 FAIL, 0 ERROR**. The intentional warning finds one duplicated observed signature: two June 23 records, 07:27:34–07:27:43, unknown zone 264 to 264, zero miles, recorded total $4.50. They are suspicious and retained; selected field equality is not proof of one real-world ride.

Python compile/import and git diff --check pass. GitHub Actions uses Ubuntu + Python 3.12, pinned dependencies, compile/import checks, pytest including dbt build/test, and whitespace validation. CI never downloads TLC or NOAA data and needs no source credentials. Current remote CI status is available in GitHub Actions.

## Scheduling and BI status

Pipeline orchestration is handled by the Python CLI; no recurring schedule is installed by this repository.

Power BI-ready exports: dim_date, dim_zone, mart_zone_daily, mart_borough_daily, mart_weather_demand and mart_quality, each as CSV and Parquet. schemas.json, quality.json and source_manifest.json supplement them. No completed Power BI dashboard is claimed. See POWER_BI.md for grain-safe import relationships.
