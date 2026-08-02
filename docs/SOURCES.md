# Official source contracts

- [TLC trip record portal](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Yellow taxi dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
- Monthly files: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
- Zones: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
- [NOAA NCEI Access API](https://www.ncei.noaa.gov/support/access-data-service-api-user-documentation)

Initial scope: January–June 2025, Yellow Taxi, six monthly files. January URL verified HTTP 200, 59,158,238 bytes. TLC publishes vendor-submitted records and does not guarantee their accuracy. The 2025 optional `cbd_congestion_fee` column is explicitly documented by TLC.

Weather: daily-summaries, Central Park station USW00094728, monthly API requests, metric units, PRCP/SNOW/TMIN/TMAX, includeAttributes=true. No API key. Keep raw attributes; values with a nonempty NOAA quality flag are null in Bronze. PRCP/SNOW are millimeters, temperatures Celsius. Missing measurements remain null, never zero-filled. Central Park is a citywide weather proxy, not zone-level measurements or causal evidence. Daily observations and taxi local calendar dates are joined after aggregation; observation-day boundaries are not exact trip-time conditions.

Raw versions are SHA-256-named immutable files. A normal run verifies local hashes; `--check-remote` explicitly downloads a candidate to detect publisher corrections. It never silently assumes remote files cannot change. Old versions remain on disk.
