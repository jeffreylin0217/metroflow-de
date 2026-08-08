# Quality policy v1

Rules were selected after profiling the full real January file; see INITIAL_PROFILE.txt.

INVALID: missing/reversed timestamps, pickup outside the source month, null/nonfinite/negative distance, null/nonfinite money, missing/unrecognized zone keys. These rows remain in immutable raw and Bronze and in the stg_taxi_trips view. Query that view with `quality_status='INVALID'` to inspect quarantine; no second stored quarantine copy is needed. They are excluded from the fact. Out-of-month pickups are quarantined so monthly ownership is deterministic; this is a deliberate coverage limitation.

SUSPICIOUS but retained: negative money (can represent corrections/refunds), zero distance, missing/nonpositive/noninteger passenger counts, unfamiliar payment codes, or duration >180 minutes. Three hours is an analytical review threshold, not a TLC regulatory limit; it never deletes a trip. Missing passenger counts are not guessed. Borough and weather demand metrics include suspicious trips. Filter the fact if a question requires stricter quality.

ACCEPTED: neither INVALID nor SUSPICIOUS under these rules. This does not certify ground truth. Official unknown zones (264/265) remain valid lookup members. Taxi timestamps are local wall-clock values; daylight-saving transitions can affect elapsed-time interpretations.

`mart_quality`, reporting/quality.json and per-run JSON expose status counts, overlapping reason counts and retained/excluded totals. dbt reconciliation checks input = all statuses and retained = every demand mart total. A warning-level duplicate-observation test groups a documented subset of fields; it does not delete records or assert real-world identity. Its result count is recorded in dbt run_results.json.

NOAA input dates and station are validated before joins. Nonempty NOAA quality flags null the corresponding measurement; blank and missing measurements remain null. Temperature/precipitation coverage should be inspected in exports. No missing weather is invented.

Schema contract accepts numeric widths and timestamp units, safely casts to canonical Bronze types, and fails on missing required fields or incompatible types. Optional fields are retained in raw, fingerprinted, and deliberately omitted from the small analytical projection. Physical row numbers are preserved by Arrow batch order. Schema fingerprints are auditable in the SQLite ledger and history; see tests for drift examples.
