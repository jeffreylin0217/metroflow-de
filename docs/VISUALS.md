# Understanding the README visuals

The visuals summarize the existing pipeline. Matplotlib is an optional Python
plotting library; it does not add a new data platform, service, or transformation
framework. The three charts can be interpreted from the underlying SQL-level aggregates.

## What each chart means

| Chart | Input | Calculation | Important limitation |
|---|---|---|---|
| Retained trips by month | mart_weather_demand | GROUP BY month, SUM(trip_count) | Monthly totals are not normalized for the number of days. |
| Pickup location totals | mart_borough_daily | GROUP BY borough, SUM(trip_count) | Includes every lookup category: Unknown, N/A and EWR as well as NYC boroughs. |
| Measured batch operations | real_validation.json | Read the four recorded durations | Each bar is one different operation, not repeated benchmark trials or an optimization speedup. |

"Retained" includes ACCEPTED and SUSPICIOUS records. INVALID records are excluded
from these demand charts. Neither the charts nor the fact table claim that every
retained row is a verified unique real-world ride.

The first measured six-month load reused January and the zone lookup; it downloaded
the remaining ten files. Rerun, backfill and full refresh used local source files.
Runtime depends on the machine and cache. The unchanged rerun checks local hashes
and skips dbt, which explains its short runtime.

## Read the script in three parts

1. `collect()` reads one published reporting directory into an in-memory DuckDB
   connection. It groups the small marts, reads the recorded timings and checks
   that the totals agree with the benchmark. It does not query millions of raw
   trip rows or modify the warehouse.
2. `render()` draws ordinary bar charts, adds labels and saves the PNG. Most of
   its code controls spacing, colors and labels.
3. `main()` chooses paths and writes the image and `snapshot_values.json`.
   The JSON contains exact counts and source checksums, so rounded chart labels
   can be traced back to their inputs.

The `data/reporting` symlink is resolved once so the script reads a single
publication, even if another run later publishes a new reporting generation.
The hashes are ordinary SHA-256 checksums, using the same idea as the source
manifest. They identify the bytes used to generate the picture.

## Diagram interpretation

The architecture diagram shows the pipeline stages. The entity diagram shows
three logical relationships: many fact rows can share a pickup date, a pickup
zone or a dropoff zone. dbt checks those keys; the diagram does not claim enforced
DuckDB foreign-key constraints.

The separate lineage diagram shows how SQL produces the daily marts. Aggregation
is not a foreign-key relationship between a trip and a daily total. Weather is
joined once demand has been grouped by date, protecting the model grain.

## Validation checklist

- Reconcile the monthly `GROUP BY` totals with the retained fact count and confirm
  that 930 INVALID rows are excluded.
- Confirm that EWR, Unknown and other lookup categories remain visible when
  reconciling pickup-location totals.
- Trace a sample date from trips to zone totals, borough totals and the daily
  weather mart.
- Verify that an unchanged rerun skips completed warehouse work rather than
  implying faster full-data processing.
- Regenerate the visualization and confirm that the plotted numeric values remain
  consistent with the recorded reporting generation and benchmark evidence.

These checks help ensure that the README visuals remain reproducible and consistent
with the published pipeline outputs.
