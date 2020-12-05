val taxi_hourly = spark.sql(
  """SELECT SUBSTRING(start_time, 1, 10) AS service_date,
           SUBSTRING(start_time, 1, 2) AS month,
           SUBSTRING(start_time, 7, 4) AS year,
           EXTRACT(dayofweek FROM CONCAT(SUBSTRING(start_time, 7, 4), '-',
           SUBSTRING(start_time, 1, 2), '-',
           SUBSTRING(start_time, 4, 2))) AS dayofweek,
           CONCAT(SUBSTRING(start_time, 12, 2), SUBSTRING(start_time, 21, 2)) AS hour,
           sum(duration_seconds) AS duration_seconds_taxi,
           CAST(FLOOR(sum(distance_miles)*10) AS BIGINT) AS miles_tenths_taxi,
           pickup_community_area,
           dropoff_community_area,
           CAST(FLOOR(sum(fare)*100) AS BIGINT) AS fare_cents_taxi,
           CAST(FLOOR(sum(tip)*100) AS BIGINT) AS tip_cents_taxi,
           CAST(FLOOR(sum(additional_charges)*100) AS BIGINT) AS additional_charges_cents_taxi,
           CAST(FLOOR(sum(trip_total)*100) AS BIGINT) AS trip_total_cents_taxi,
           COUNT(1) AS num_rides_taxi
    FROM taxis_orc
    WHERE start_time IS NOT NULL AND start_time != ''
    AND pickup_community_area IS NOT NULL AND dropoff_community_area IS NOT NULL
    AND pickup_community_area != '' AND dropoff_community_area != ''
    GROUP BY year, month, service_date, dayofweek, hour, pickup_community_area, dropoff_community_area LIMIT 100000
    """.stripMargin
)
taxi_hourly.createOrReplaceTempView("taxi_hourly")