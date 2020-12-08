val month_map = Seq(
  ("01", "January"),
  ("02", "February"),
  ("03", "March"),
  ("04", "April"),
  ("05", "May"),
  ("06", "June"),
  ("07", "July"),
  ("08", "August"),
  ("09", "September"),
  ("10", "October"),
  ("11", "November"),
  ("12", "December")
).toDF("month_num", "month_name")
month_map.createOrReplaceTempView("month_map")

val daytype_map = Seq(
  (1, "Weekend"),
  (2, "Weekday"),
  (3, "Weekday"),
  (4, "Weekday"),
  (5, "Weekday"),
  (6, "Weekday"),
  (7, "Weekend"),
).toDF("day", "daytype")
daytype_map.createOrReplaceTempView("daytype_map")

