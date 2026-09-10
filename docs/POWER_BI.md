# Power BI-ready reporting

No .pbix dashboard is included or claimed. A successful pipeline writes six stable datasets in both CSV and Parquet, with schemas.json documenting column names/types. Resolve `data/reporting` to its versioned directory before refreshing multiple files.

| Dataset | Grain | Suggested use |
|---|---|---|
| dim_date | calendar date | Date slicer and weekday labels |
| dim_zone | TLC zone key | Zone/borough labels |
| mart_zone_daily | date × pickup zone | Zone performance and demand |
| mart_borough_daily | date × borough | Borough overview |
| mart_weather_demand | station date | Daily demand and observed weather |
| mart_quality | source month × status | Quality exclusions and warnings |

In Power BI Desktop: Get Data → Text/CSV, choose files from one reporting generation, and set date columns to Date. Import dim_date and dim_zone; create one-to-many, single-direction relationships to mart_zone_daily on date/zone_key. Do not join aggregate fact tables directly to one another. Weather-demand and borough tables can share dim_date independently. Do not sum across overlapping marts.

Financial values are recorded dollars rounded to cents in the fact and summed as DECIMAL. Negative corrections remain, so these are recorded fare/amount metrics, not audited revenue. Passenger counts represent observed usable values; compare trips_with_passenger_count with trip_count. Distance is miles, duration is minutes, precipitation/snowfall are mm, temperatures Celsius. Daily weather is a single-station proxy; missing measurements remain blank.

Potential pages: Executive Overview, Demand Trends, Zone/Borough Performance, Weather vs Demand, Data Quality. Build these only when they answer a clear question. The project deliverable is the reporting layer.
