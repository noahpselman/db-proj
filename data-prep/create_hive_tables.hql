-- CTA

CREATE EXTERNAL TABLE cta_ridership
(
    service_date STRING,
    day_type STRING,
    bus_count BIGINT,
    rail_count BIGINT,
    total_count BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
    "separatorChar" = "\,",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
    location '/tmp/nselman/proj/data/cta';
TBLPROPERTIES (
    "skip.header.line.count"="1")
) ;

CREATE TABLE cta_ridership_orc
(
    service_date STRING,
    day_type STRING,
    bus_count BIGINT,
    rail_count BIGINT,
    total_count BIGINT
)
STORE AS orc

INSERT OVERWRITE TABLE cta_ridership_orc select * from cta_ridership
WHERE service_date IS NOT NULL AND bus_count IS NOT NULL
AND rail_count IS NOT NULL ;


----------------------------------------

-- RIDESHARE

CREATE EXTERNAL TABLE rideshare
(
    trip_id STRING,
    start_time STRING,
    end_time STRING,
    duration_seconds STRING,
    distance_miles STRING,
    pickup_census_tract STRING,
    dropoff_census_tract STRING,
    pickup_community_area STRING,
    dropoff_community_area STRING,
    fare FLOAT,
    tip FLOAT,
    additional_charges FLOAT,
    trip_total FLOAT,
    shared_trip_authorized STRING,
    number_pools BIGINT,
    pickup_centroid_latitude STRING,
    pickup_centroid_longitude STRING,
    pickup_centroid_location STRING,
    dropoff_centroid_latitude STRING,
    dropoff_centroid_longitude STRING,
    dropoff_centroid_location STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
    "separatorChar" = "\,",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
    location '/tmp/nselman/proj/data/rideshare';
TBLPROPERTIES (
    "skip.header.line.count"="1")
) ;


CREATE TABLE rideshare_orc
(
    trip_id STRING,
    start_time STRING,
    end_time STRING,
    duration_seconds STRING,
    distance_miles STRING,
    pickup_census_tract STRING,
    dropoff_census_tract STRING,
    pickup_community_area STRING,
    dropoff_community_area STRING,
    fare FLOAT,
    tip FLOAT,
    additional_charges FLOAT,
    trip_total FLOAT,
    shared_trip_authorized STRING,
    number_pools BIGINT,
    pickup_centroid_latitude STRING,
    pickup_centroid_longitude STRING,
    pickup_centroid_location STRING,
    dropoff_centroid_latitude STRING,
    dropoff_centroid_longitude STRING,
    dropoff_centroid_location STRING
)
STORE AS orc

INSERT OVERWRITE TABLE rideshare_orc select * from rideshare
WHERE start_time IS NOT NULL AND distance_miles IS NOT NULL
AND duration_seconds IS NOT NULL ;

--------------------------------------------------

-- TAXI

CREATE EXTERNAL TABLE taxis
(
    trip_id STRING,
    taxi_id STRING,
    start_time STRING,
    end_time STRING,
    duration_seconds STRING,
    distance_miles STRING,
    pickup_census_tract STRING,
    dropoff_census_tract STRING,
    pickup_community_area STRING,
    dropoff_community_area STRING,
    fare FLOAT,
    tip FLOAT,
    tolls FLOAT,
    additional_charges FLOAT,
    trip_total FLOAT,
    company STRING,
    shared_trip_authorized STRING,
    pickup_centroid_latitude STRING,
    pickup_centroid_longitude STRING,
    pickup_centroid_location STRING,
    dropoff_centroid_latitude STRING,
    dropoff_centroid_longitude STRING,
    dropoff_centroid_location STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
    "separatorChar" = "\,",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
    location '/tmp/nselman/proj/data/taxi';
TBLPROPERTIES (
    "skip.header.line.count"="1")
) ;


CREATE TABLE taxis_orc
(
    trip_id STRING,
    start_time STRING,
    end_time STRING,
    duration_seconds STRING,
    distance_miles STRING,
    pickup_census_tract STRING,
    dropoff_census_tract STRING,
    pickup_community_area STRING,
    dropoff_community_area STRING,
    fare FLOAT,
    tip FLOAT,
    additional_charges FLOAT,
    trip_total FLOAT,
    shared_trip_authorized STRING,
    number_pools BIGINT,
    pickup_centroid_latitude STRING,
    pickup_centroid_longitude STRING,
    pickup_centroid_location STRING,
    dropoff_centroid_latitude STRING,
    dropoff_centroid_longitude STRING,
    dropoff_centroid_location STRING
)
STORE AS orc

INSERT OVERWRITE TABLE taxis_orc select * from taxis
WHERE start_time IS NOT NULL AND distance_miles IS NOT NULL
AND duration_seconds IS NOT NULL ;

---------------------------------

----------------------------------------

-- COVID

CREATE EXTERNAL TABLE covid_cases (
    service_Date STRING,
    cases_total BIGINT,
    death_total BIGINT,
    hosp_total BIGINT,
    cases0_17 BIGINT,
    cases18_29 BIGINT,
    cases30_39 BIGINT,
    cases40_49 BIGINT,
    cases50_59 BIGINT,
    cases60_69 BIGINT,
    cases70_79 BIGINT,
    cases80plus BIGINT,
    cases_age_unknown BIGINT,
    cases_female BIGINT,
    cases_male BIGINT,
    cases_gender_unknown BIGINT,
    cases_latin BIGINT,
    cases_asian_nonlatin BIGINT,
    cases_black_nonlatin BIGINT,
    cases_white_nonlatin BIGINT,
    cases_otherrace_nonlatin BIGINT,
    cases_unknown_race BIGINT,
    death0_17 BIGINT,
    death18_29 BIGINT,
    death30_39 BIGINT,
    death40_49 BIGINT,
    death50_59 BIGINT,
    death60_69 BIGINT,
    death70_79 BIGINT,
    death80plus BIGINT,
    death_age_unknown BIGINT,
    death_female BIGINT,
    death_male BIGINT,
    death_gender_unknown BIGINT,
    death_latin BIGINT,
    death_asian_nonlatin BIGINT,
    death_black_nonlatin BIGINT,
    death_white_nonlatin BIGINT,
    death_otherrace_nonlatin BIGINT,
    death_unknown_race BIGINT,
    hosp0_17 BIGINT,
    hosp18_29 BIGINT,
    hosp30_39 BIGINT,
    hosp40_49 BIGINT,
    hosp50_59 BIGINT,
    hosp60_69 BIGINT,
    hosp70_79 BIGINT,
    hosp80plus BIGINT,
    hosp_age_unknown BIGINT,
    hosp_female BIGINT,
    hosp_male BIGINT,
    hosp_gender_unknown BIGINT,
    hosp_latin BIGINT,
    hosp_asian_nonlatin BIGINT,
    hosp_black_nonlatin BIGINT,
    hosp_white_nonlatin BIGINT,
    hosp_otherrace_nonlatin BIGINT,
    hosp_unknown_race BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "\,",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
    location '/tmp/nselman/proj/data/covid'
TBLPROPERTIES (
    "skip.header.line.count"="1"
)

CREATE TABLE covid_cases_orc (
    service_date STRING,
    cases_total BIGINT,
    death_total BIGINT,
    hosp_total BIGINT,
    cases0_17 BIGINT,
    cases18_29 BIGINT,
    cases30_39 BIGINT,
    cases40_49 BIGINT,
    cases50_59 BIGINT,
    cases60_69 BIGINT,
    cases70_79 BIGINT,
    cases80plus BIGINT,
    cases_age_unknown BIGINT,
    cases_female BIGINT,
    cases_male BIGINT,
    cases_gender_unknown BIGINT,
    cases_latin BIGINT,
    cases_asian_nonlatin BIGINT,
    cases_black_nonlatin BIGINT,
    cases_white_nonlatin BIGINT,
    cases_otherrace_nonlatin BIGINT,
    cases_unknown_race BIGINT,
    death0_17 BIGINT,
    death18_29 BIGINT,
    death30_39 BIGINT,
    death40_49 BIGINT,
    death50_59 BIGINT,
    death60_69 BIGINT,
    death70_79 BIGINT,
    death80plus BIGINT,
    death_age_unknown BIGINT,
    death_female BIGINT,
    death_male BIGINT,
    death_gender_unknown BIGINT,
    death_latin BIGINT,
    death_asian_nonlatin BIGINT,
    death_black_nonlatin BIGINT,
    death_white_nonlatin BIGINT,
    death_otherrace_nonlatin BIGINT,
    death_unknown_race BIGINT,
    hosp0_17 BIGINT,
    hosp18_29 BIGINT,
    hosp30_39 BIGINT,
    hosp40_49 BIGINT,
    hosp50_59 BIGINT,
    hosp60_69 BIGINT,
    hosp70_79 BIGINT,
    hosp80plus BIGINT,
    hosp_age_unknown BIGINT,
    hosp_female BIGINT,
    hosp_male BIGINT,
    hosp_gender_unknown BIGINT,
    hosp_latin BIGINT,
    hosp_asian_nonlatin BIGINT,
    hosp_black_nonlatin BIGINT,
    hosp_white_nonlatin BIGINT,
    hosp_otherrace_nonlatin BIGINT,
    hosp_unknown_race BIGINT
)
STORED AS orc ;

INSERT OVERWRITE TABLE covid_cases_orc SELECT * FROM covid_cases
WHERE service_date IS NOT NULL AND cases_total IS NOT NULL ;

--------------------------------------

CREATE EXTERNAL TABLE chicago_community_areas (
area_name STRING,
area_num STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = "\,",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
    location '/tmp/nselman/proj/data/community_areas/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
)
