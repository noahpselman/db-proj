CREATE TABLE chicago_transportation_hourly_hbase (
key STRING,
duration_seconds BIGINT,
miles_tenths BIGINT,
tip_cents BIGINT,
additional_charges_cents BIGINT,
trip_total_cents BIGINT,
num_rides BIGINT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' =
':key#b,
stats:duration_seconds#b,
stats:miles_tenths#b,
stats:tip_cents#b,
stats:additional_charges_cents#b,
stats:trip_total_cents#b,
stats:num_rides#b')
TBLPROPERTIES ('hbase.table.name' = 'chicago_transportation_hourly');

INSERT OVERWRITE TABLE chicago_transportation_hourly_hbase
SELECT
    key STRING,
    duration_seconds,
    miles_tenths,
    tip_cents,
    additional_charges_cents,
    trip_total_cents,
    num_rides
FROM chicago_transportation_hourly
