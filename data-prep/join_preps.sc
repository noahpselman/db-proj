

val transportation_hourly_joined = spark.sql(
  """  SELECT
  CONCAT(year, ':', m.month_name, ':', daytype, ':', c.area_name, ':', d.area_name, ':', hour) AS key,
  sum(duration_seconds_rs) AS duration_seconds_rs,
  sum(duration_seconds_taxi) AS duration_seconds_taxi,
  sum(miles_tenths_rs) AS miles_tenths_rs,
  sum(miles_tenths_taxi) AS miles_tenths_taxi,
  sum(trip_total_cents_rs) AS trip_total_cents_rs,
  sum(trip_total_cents_taxi) AS trip_total_cents_taxi,
  sum(tip_cents_rs) AS tip_cents_rs,
  sum(tip_cents_taxi) AS tip_cents_taxi,
  sum(num_rides_rs) AS num_rides_rs,
  sum(num_rides_taxi) AS num_rides_taxi
  FROM (
    SELECT
           (case when rs.service_date is not null then rs.service_date else taxi.service_date end) AS service_date,
           (case when rs.year is not null then rs.year else taxi.year end) AS year,
           (case when rs.month is not null then rs.month else taxi.month end) AS month,
           (case when rs.hour is not null then rs.hour else taxi.hour end) AS hour,
           (case when rs.pickup_community_area is not null then rs.pickup_community_area else taxi.pickup_community_area end) AS pickup_community_area,
           (case when rs.dropoff_community_area is not null then rs.dropoff_community_area else taxi.dropoff_community_area end) AS dropoff_community_area,
           duration_seconds_rs,
           duration_seconds_taxi,
           miles_tenths_rs,
           miles_tenths_taxi,
           trip_total_cents_rs,
           trip_total_cents_taxi,
           tip_cents_rs,
           tip_cents_taxi,
           num_rides_rs,
           num_rides_taxi,
           daytype
    FROM rideshare_hourly rs
    FULL OUTER JOIN taxi_hourly taxi on rs.service_date == taxi.service_date
    AND rs.hour == taxi.hour
    AND rs.pickup_community_area == taxi.pickup_community_area
    AND rs.dropoff_community_area == taxi.dropoff_community_area
    LEFT JOIN daytype_map dt ON rs.dayofweek == dt.day OR taxi.dayofweek == dt.day) a
    LEFT JOIN month_map m ON m.month_num = a.month
    LEFT JOIN chicago_community_areas c ON c.area_num = a.pickup_community_area
    LEFT JOIN chicago_community_areas d ON d.area_num = a.dropoff_community_area
    WHERE pickup_community_area != '' AND dropoff_community_area != ''
    AND pickup_community_area is not NULL AND dropoff_community_area is not NULL
    AND c.area_num IS NOT NULL AND d.area_num IS NOT NULL
    GROUP BY year, m.month_name, daytype, hour, c.area_name, d.area_name
    """.stripMargin
)
transportation_hourly_joined.createOrReplaceTempView("transportation_hourly_joined")


import org.apache.spark.sql.SaveMode
transportation_hourly_joined.write.mode(SaveMode.Overwrite).saveAsTable("chicago_transportation_hourly")


//LEFT JOIN cta_ridership_orc cta on cta.service_date == rs.service_date
//OR cta.service_Date == taxi.service_date


val test = spark.sql("SELECT pickup_community_area FROM transportation_hourly_joined WHERE key IS NULL")


val int = spark.sql(
"""    SELECT
           (case when rs.service_date is not null then rs.service_date else taxi.service_date end) AS service_date,
           (case when rs.year is not null then rs.year else taxi.year end) AS year,
           (case when rs.month is not null then rs.month else taxi.month end) AS month,
           (case when rs.hour is not null then rs.hour else taxi.hour end) AS hour,
           (case when rs.pickup_community_area is not null then rs.pickup_community_area else taxi.pickup_community_area end) AS pickup_community_area,
           (case when rs.dropoff_community_area is not null then rs.dropoff_community_area else taxi.dropoff_community_area end) AS dropoff_community_area,
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
    FROM rideshare_hourly rs
    FULL OUTER JOIN taxi_hourly taxi on rs.service_date = taxi.service_date
    AND rs.hour = taxi.hour
    AND rs.pickup_community_area = taxi.pickup_community_area
    AND rs.dropoff_community_area = taxi.dropoff_community_area
"""
)

//LEFT JOIN cta_ridership_orc cta on cta.service_date == rs.service_date
//OR cta.service_Date == taxi.service_date
//LEFT JOIN daytype_map dt ON rs.dayofweek == dt.day OR taxi.dayofweek == dt.day


int.write.mode(SaveMode.Overwrite).saveAsTable("int")


val taxi_day = spark.sql("SELECT * FROM taxi_hourly WHERE hour == '01AM' AND service_date == '01/01/2020'" +
  "AND pickup_community_area == '24' AND dropoff_community_area == '3'")
val rs_day = spark.sql("SELECT * FROM rideshare_hourly WHERE hour == '01AM' AND service_date == '01/01/2020'" +
  "AND pickup_community_area == '24' AND dropoff_community_area == '3'")

val int_day  = spark.sql("SELECT * FROM rideshare_hourly WHERE hour == '01AM' AND service_date == '01/01/2020'" +
  "AND pickup_community_area == '24' AND dropoff_community_area == '3'")