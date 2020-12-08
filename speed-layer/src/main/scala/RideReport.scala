

case class RideReport (
    ridetype: String,
    pickup_community_area: String,
    dropoff_community_area: String,
    year: String,
    month: String,
    hour: String,
    daytype: String,
    duration_seconds: Long,
    miles_tenths: Long,
    tip_cents: Long,
    trip_total_cents: Long)