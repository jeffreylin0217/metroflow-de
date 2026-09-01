"""Generate the initial explicit dbt column/grain documentation (development utility)."""
from pathlib import Path
import yaml

models = {
 'stg_taxi_trips': ('One source file row; key trip_id = source SHA-256 plus zero-based physical row. Upstream bronze.trips and stg_zones. Local wall-clock timestamps; invalid rows retained with overlapping reason flags.', {'trip_id':['unique','not_null'],'source_period':['not_null'],'quality_status':['not_null',{'accepted_values':{'arguments':{'values':['INVALID','SUSPICIOUS','ACCEPTED']}}}]}),
 'stg_zones': ('One TLC location key; upstream bronze.zones. Uniqueness validated before any join.', {'zone_key':['unique','not_null']}),
 'stg_weather': ('One Central Park date; upstream bronze.weather. Full source-month date coverage required, missing or quality-rejected measurements remain NULL.', {'date':['unique','not_null']}),
 'dim_zone': ('One official TLC location key; upstream stg_zones; includes official Unknown/N/A zones.', {'zone_key':['unique','not_null'],'borough':['not_null']}),
 'dim_date': ('One calendar date spanning loaded weather months; upstream stg_weather. Sunday=0; gaps between noncontiguous loaded months remain in the calendar.', {'date_key':['unique','not_null']}),
 'fct_trips': ('One non-INVALID source record, including suspicious records; upstream stg_taxi_trips. trip_id is source identity, not a real-world trip ID. Duplicate-looking rides are preserved. Source period is replaced transactionally.', {'trip_id':['unique','not_null'],'source_period':['not_null'],'pickup_zone_key':[{'relationships':{'arguments':{'to':"ref('dim_zone')",'field':'zone_key'}}},'not_null'],'dropoff_zone_key':[{'relationships':{'arguments':{'to':"ref('dim_zone')",'field':'zone_key'}}},'not_null'],'pickup_date':[{'relationships':{'arguments':{'to':"ref('dim_date')",'field':'date_key'}}},'not_null'],'quality_status':[{'accepted_values':{'arguments':{'values':['ACCEPTED','SUSPICIOUS']}}}]}),
 'mart_zone_daily': ('One date × pickup zone with retained trips; upstream fct_trips. Composite key (date, zone_key). AVG ignores nulls; passenger totals are observed values, not imputed.', {'date':['not_null'],'zone_key':['not_null']}),
 'mart_borough_daily': ('One date × pickup borough; upstream mart_zone_daily and unique dim_zone. Composite key (date, borough). Sum trip counts after many-to-one join.', {'date':['not_null'],'borough':['not_null']}),
 'mart_weather_demand': ('One loaded station date, including zero-demand days; upstream dim_date, mart_zone_daily, stg_weather. No trip-to-weather join. Trailing window is up to seven available observation rows.', {'date':['unique','not_null']}),
 'mart_quality': ('One source month × quality status present; upstream stg_taxi_trips. Composite key (source_period, quality_status). Reason counts overlap and must not be added together.', {'source_period':['not_null'],'quality_status':['not_null']})}
Path('dbt/models/schema.yml').write_text(yaml.safe_dump({'version':2,'models':[{'name':name,'description':desc,'columns':[{'name':n,'data_tests':tests} for n,tests in cols.items()]} for name,(desc,cols) in models.items()]},sort_keys=False))
