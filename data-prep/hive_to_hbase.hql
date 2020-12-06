CREATE TABLE chicago_transportation_hourly_hbase (
key STRING,
duration_seconds_rs BIGINT,
duration_seconds_taxi BIGINT,
miles_tenths_rs BIGINT,
miles_tenths_taxi BIGINT,
trip_total_cents_rs BIGINT,
trip_total_cents_taxi BIGINT,
tip_cents_rs BIGINT,
tip_cents_taxi BIGINT,
num_rides_rs BIGINT,
num_rides_taxi BIGINT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' =
':key,
stats:duration_seconds_rs#b,
stats:duration_seconds_taxi#b,
stats:miles_tenths_rs#b,
stats:miles_tenths_taxi#b,
stats:trip_total_cents_rs#b,
stats:trip_total_cents_taxi#b,
stats:tip_cents_rs#b,
stats:tip_cents_taxi#b,
stats:num_rides_rs#b,
stats:num_rides_taxi#b')
TBLPROPERTIES ('hbase.table.name' = 'chicago_transportation_hourly');

INSERT OVERWRITE TABLE chicago_transportation_hourly_hbase
SELECT
    key,
    duration_seconds_rs,
    duration_seconds_taxi,
    miles_tenths_rs,
    miles_tenths_taxi,
    trip_total_cents_rs,
    trip_total_cents_taxi,
    tip_cents_rs,
    tip_cents_taxi,
    num_rides_rs,
    num_rides_taxi
    FROM chicago_transportation_hourly
