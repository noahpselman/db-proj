val taxi_prep = spark.sql(
  """
    SELECT
    start_time,
    "taxi" AS ridetype,
    pickup_community_area,
    dropoff_community_area,
    duration_seconds,
    distance_miles,
    tip,
    additional_charges,
    trip_total
    FROM taxis_orc
    WHERE start_time IS NOT NULL AND start_time != ''
    AND pickup_community_area IS NOT NULL AND dropoff_community_area IS NOT NULL
    AND pickup_community_area != '' AND dropoff_community_area != ''
    """
)


val rs_prep = spark.sql(
  """
    SELECT
    start_time,
    "rideshare" AS ridetype,
    pickup_community_area,
    dropoff_community_area,
    duration_seconds,
    distance_miles,
    tip,
    additional_charges,
    trip_total
    FROM rideshare_orc
    WHERE start_time IS NOT NULL AND start_time != ''
    AND pickup_community_area IS NOT NULL AND dropoff_community_area IS NOT NULL
    AND pickup_community_area != '' AND dropoff_community_area != ''
    """
)


val vertically_joined = taxi_prep.union(rs_prep)
vertically_joined.createOrReplaceTempView("vertically_joined")

val hourly_data = spark.sql(
  """
    SELECT
      CONCAT(year, ':', m.month_name, ':', daytype, ':', c.area_name, ':', d.area_name, ':', hour, ':', ridetype) AS key,
      CAST(FLOOR(SUM(duration_seconds)) AS BIGINT) AS duration_seconds,
      CAST(FLOOR(sum(distance_miles)*10) AS BIGINT) AS miles_tenths,
      CAST(FLOOR(sum(tip)*100) AS BIGINT) AS tip_cents,
      CAST(FLOOR(sum(additional_charges)*100) AS BIGINT) AS additional_charges_cents,
      CAST(FLOOR(sum(trip_total)*100) AS BIGINT) AS trip_total_cents,
      CAST(COUNT(1) AS BIGINT) AS num_rides
    FROM
      (SELECT
        SUBSTRING(start_time, 1, 10) AS service_date,
        SUBSTRING(start_time, 1, 2) AS month,
        SUBSTRING(start_time, 7, 4) AS year,
        EXTRACT(dayofweek FROM CONCAT(SUBSTRING(start_time, 7, 4), '-',
        SUBSTRING(start_time, 1, 2), '-',
        SUBSTRING(start_time, 4, 2))) AS dayofweek,
        CONCAT(SUBSTRING(start_time, 12, 2), SUBSTRING(start_time, 21, 2)) AS hour,
        ridetype,
        pickup_community_area,
        dropoff_community_area,
        duration_seconds,
        distance_miles,
        tip,
        additional_charges,
        trip_total
      FROM vertically_joined) a
    LEFT JOIN daytype_map dt ON a.dayofweek = dt.day
    LEFT JOIN month_map m ON m.month_num = a.month
    LEFT JOIN chicago_community_areas c ON c.area_num = a.pickup_community_area
    LEFT JOIN chicago_community_areas d ON d.area_num = a.dropoff_community_area
    WHERE pickup_community_area != '' AND dropoff_community_area != ''
    AND pickup_community_area is not NULL AND dropoff_community_area is not NULL
    AND c.area_num IS NOT NULL AND d.area_num IS NOT NULL
    GROUP BY year, m.month_name, daytype, hour, c.area_name, d.area_name, ridetype
    """.stripMargin
)

import org.apache.spark.sql.SaveMode
hourly_data.write.mode(SaveMode.Overwrite).saveAsTable("chicago_transportation_hourly")
