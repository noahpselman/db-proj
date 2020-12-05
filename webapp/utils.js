const hours = [
    "12 AM",
    "01 AM",
    "02 AM",
    "03 AM",
    "04 AM",
    "05 AM",
    "06 AM",
    "07 AM",
    "08 AM",
    "09 AM",
    "10 AM",
    "11 AM",
    "12 PM",
    "01 PM",
    "02 PM",
    "03 PM",
    "04 PM",
    "05 PM",
    "06 PM",
    "07 PM",
    "08 PM",
    "09 PM",
    "10 PM",
    "11 PM"
];

const cols = [
    "price_per_mile_rs",
    "price_per_minute_rs",
    "price_per_ride_rs",
    "tips_per_ride_rs",
    "num_rides_rs",
    "price_per_mile_taxi",
    "price_per_minute_taxi",
    "price_per_ride_taxi",
    "tips_per_ride_taxi",
    "num_rides_taxi",
];

function cleanRow(row) {
    console.info("cleanRow called with ", row)
    if ('stats:num_rides_rs' in row) {
        row['has_rs'] = true
    }
    else {
        row['has_rs'] = false
    }
    if ('stats:num_rides_taxi' in row) {
        row['has_taxi'] = true
    }
    else {
        row['hax_taxi'] = false
    }
    console.info("returning...", row)
    return row
};

function createRow(dataRow) {
    var displayRow = {}
    if (dataRow['has_rs']) {
        let total_price_rs = (dataRow['stats:trip_total_cents_rs'] / 100).toFixed(2);
        displayRow['price_per_mile_rs'] = (total_price_rs /
            (dataRow['stats:miles_tenths_rs'] / 10)).toFixed(2)
        displayRow['price_per_minute_rs'] = (total_price_rs /
            (dataRow['stats:duration_seconds_rs'] / 60)).toFixed(2)
        displayRow['price_per_ride_rs'] = (total_price_rs /
            dataRow['stats:num_rides_rs']).toFixed(2)
        displayRow['tips_per_ride_rs'] = ((dataRow['stats:tip_cents_rs'] / 100) /
            dataRow['stats:num_rides_rs']).toFixed(2)
        displayRow['num_rides_rs'] = dataRow['stats:num_rides_rs']
    }
    else {
        displayRow['price_per_mile_rs'] = "-"
        displayRow['price_per_minute_rs'] = "-"
        displayRow['price_per_ride_rs'] = "-"
        displayRow['tips_per_ride_rs'] = "-"
        displayRow['num_rides_rs'] = "-"
    }

    if (dataRow['has_taxi']) {
        let total_price_taxi = (dataRow['stats:trip_total_cents_taxi'] / 100).toFixed(2);
        displayRow['price_per_mile_taxi'] = (total_price_taxi /
            (dataRow['stats:miles_tenths_taxi'] / 10)).toFixed(2)
        displayRow['price_per_minute_taxi'] = (total_price_taxi /
            (dataRow['stats:duration_seconds_taxi'] / 60)).toFixed(2)
        displayRow['price_per_ride_taxi'] = (total_price_taxi /
            dataRow['stats:num_rides_taxi']).toFixed(2)
        displayRow['tips_per_ride_taxi'] = ((dataRow['stats:tip_cents_taxi'] / 100) /
            dataRow['stats:num_rides_taxi']).toFixed(2)
        displayRow['num_rides_taxi'] = dataRow['stats:num_rides_taxi']
    }
    else {
        displayRow['price_per_mile_taxi'] = "-"
        displayRow['price_per_minute_taxi'] = "-"
        displayRow['price_per_ride_taxi'] = "-"
        displayRow['tips_per_ride_taxi'] = "-"
        displayRow['num_rides_taxi'] = "-"
    }
    return displayRow
};

function createEmptyRow() {
    const emptyRow = {}
    cols.forEach(function(col) {
        emptyRow[col] = "-"
    })
    return emptyRow
};


function orderHours(data) {

    const orderedTable = []
    hours.forEach(function (hour) {
        var displayRow = {}
        if (hour in data) {
            const cleanedRow = cleanRow(data[hour])
            displayRow = createRow(cleanedRow)
        }
        else {
            displayRow = createEmptyRow()
        }
        displayRow['hour'] = hour
        orderedTable.push(displayRow)
    })
    return orderedTable
};

module.exports = { createRow, createEmptyRow, orderHours };