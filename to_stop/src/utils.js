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

const months = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December"
}

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
]

function getMonthName(monthNumber) {
    return months[monthNumber]
}

function cleanRow(row) {
    // console.info("cleanRow called with ", row)
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
    // console.info("returning...", row)
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

function counterToNumber(c) {
    return Number(Buffer.from(c).readBigInt64BE());
}

function addZero(i) {
    if (i < 10) {
        i = "0" + i;
    }
    return i;
}

function rowToMap(row) {
    console.info('rowToMap called')
    var stats = {}
    row.forEach(function (item) {
        let hour = item['key'].split(":")[5];
        let ridetype = item['key'].split(":")[6];
        var ridetype_str = ridetype
        if (ridetype == "rideshare") {
            ridetype_str = "rs"
        }
        let hourString = hour.substr(0,2).concat(" ", hour.substr(2, 2));
        if ((hourString in stats === false)) {
            stats[hourString] = {}
        }
        // we can call counterToNumber here after we adjust the hbase table
        stats[hourString][item['column'].concat("_", ridetype_str)] = counterToNumber(item['$'])
    });
    console.info('stats:', stats)
    return stats;
}

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

function isNumber(string) {
    if (typeof string != "string" || string === '') return false
    return !isNaN(string) && !isNaN(parseFloat(string))
}

module.exports = { orderHours, rowToMap, isNumber, addZero, getMonthName };