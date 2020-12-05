CREATE TABLE chicago_transportation_months (yearmonth STRING, month)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key',items:month)
TBLPROPERTIES ('hbase.table.name' = 'chicago_transportation_months')

INSERT OVERWRITE TABLE chicago_transportation_months
    SELECT DISTINCT CONCAT(SPLIT(key, ':')[1], ' ', SPLIT(key, ':')[0]),
    SPLIT(key, ':')[0]
    FROM transportation_hourly_joined ;


CREATE TABLE chicago_transportation_distinct_areas (area STRING, area_dup STRING)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key, area:area')
TBLPROPERTIES ('hbase.table.name' = 'chicago_transportation_areas')

INSERT OVERWRITE TABLE chicago_transportation_distinct_areas
(SELECT DISTINCT SPLIT(key, ':')[3], SPLIT(key, ':')[3]
FROM chicago_transportation_hourly);

-- temporarily assuming pickup locations are all loations

((SELECT DISTINCT pickup_community_area as area
FROM chicago_transportation_hourly)
UNION
(SELECT DISTINCT dropoff_community_area as area
FROM chicago_transportation_hourly)))



