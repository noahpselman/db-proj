'use strict';
const http = require('http');
var assert = require('assert');
const express = require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);
const utils = require('./utils.js');



const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4])})

// console.info("running please appear please please")


function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		// console.info(item['key'])
		let hour = item['key'].split(":")[5];
		let hourString = hour.substr(0,2).concat(" ", hour.substr(2, 2));
		if ((hourString in stats === false)) {
			stats[hourString] = {}
		}
		stats[hourString][item['column']] = Number(item['$'])
	});
	return stats;
}

// hclient.table('chicago_transportation_hourly').row("2020:September:Weekend:WEST TOWN:WEST TOWN:12AM").get((error, value) => {
// 	console.info(value)
// 	console.info(error)
// })


function weather_delay(weather, statRow) {
	var flights = statRow["delay:" + weather + "_flights"];
	var delays = statRow["delay:" + weather + "_delays"];
	if(flights == 0)
		return " - ";
	return (delays/flights).toFixed(1); /* One decimal place */
}

app.use(express.static('public'));
app.get('/hourly-ride-areas.html',function (req, res) {
	console.log("hourly-ride-areas.html called")
	const key=req.query['year'] + ":" + req.query['month'] + ":" + req.query['daytype'] + ":" +
		req.query['pickup_community_area'] + ":" + req.query['dropoff_community_area'];
	console.info("key:", key);

    hclient.table('chicago_transportation_hourly').scan({
		filter: {type : "PrefixFilter",
			value: key}, maxVersions: 1
	}, function (err, rows) {
    	console.info("rows:", rows)
		console.info(err)
		const data = utils.rowToMap(rows);
    	console.info(data)

		const outputTable = utils.orderHours(data)
		console.info(outputTable)

		const mustacheData = {
			'year': req.query['year'],
			'month': req.query['month'],
			'pickup_community_area': req.query['pickup_community_area'],
			'dropoff_community_area': req.query['dropoff_community_area'],
			'daytype': req.query['daytype'],
			'tableData': outputTable
		};
		const template = filesystem.readFileSync("hourly-result.mustache").toString();
		const html = mustache.render(template, mustacheData);
		res.send(html)
	});
});

app.get('/hourly-rides.html', function (req, res) {
	hclient.table('chicago_transportation_areas').scan({ maxVersions: 1}, (err, rows) => {
		// console.info(rows)
		var template = filesystem.readFileSync("hourly-rides.mustache").toString();
		var html = mustache.render(template, {
			areas : rows
		});
		res.send(html)
	})
});

app.get('/submit-rides.html', function (req, res) {
	hclient.table('chicago_transportation_areas').scan({ maxVersions: 1}, (err, rows) => {
		// console.info(rows)
		var template = filesystem.readFileSync("submit-rides.mustache").toString();
		var html = mustache.render(template, {
			areas : rows
		});
		res.send(html)
	})
});


app.get('/submit-rides-data.html',function (req, res) {
	console.info('the block of code that gets user inputs is running')
	const pickup_community_area = req.query['pickup_community_area'];
	let dropoff_community_area = req.query['dropoff_community_area'];
	// I KNOW! THIS CAN BE REFACTORED!  I DONT HAVE TIME!!!!
	let duration_seconds = utils.isNumber(req.query['distance'].trim()) ? Math.round(req.query['distance'] * 60) : 60;
	let distance_miles_tenths = utils.isNumber(req.query['duration'].trim()) ? Math.round(req.query['duration'] * 10) : 10;
	let trip_total_cents = utils.isNumber(req.query['total'].trim()) ? Math.round(req.query['total'] * 100) : 100;
	let tip_cents = utils.isNumber(req.query['tip'].trim()) ? Math.round(req.query['tip'] * 100) : 100;

	const d = new Date();
	var hour = d.getHours()
	var AM_PM = (hour == 0 || (hour > 12 && hour < 24)) ? "PM" : "AM"
	if (hour > 12) {
		hour = hour - 12
	}

	var report = {
		'pickup_community_area': pickup_community_area,
		'dropoff_community_area': dropoff_community_area,
		'year': d.getFullYear(),
		'month': d.getMonth(),
		'hour': hour.toString().concat(" ", AM_PM)
	}

	if (req.query['rideType'].toLowerCase() === 'taxi') {
		console.info("entered taxi")
		report['duration_seconds_taxi'] = duration_seconds
		report['miles_tenths_taxi']= distance_miles_tenths
		report['tip_cents_taxi']= tip_cents
		report['trip_total_cents_taxi']= trip_total_cents
		}
	else if (req.query['rideType'].toLowerCase() === 'rideshare') {
		console.info("endered rideshare")
		report['duration_seconds_rs'] = duration_seconds
		report['miles_tenths_rs']= distance_miles_tenths
		report['tip_cents_rs']= tip_cents
		report['trip_total_cents_rs']= trip_total_cents
	}
	else {
		console.info("else block entered")
		// Return message that says invalid string - probs would have time to implement before the due date
		let msg = "Bad inputs - wish there was a cleaner way to inform you but I don't know node.js - I guess you have to restart the app now"
		throw new Error(msg)
		res.status(500).send(msg)
	}

	console.info(report)

	// kafkaProducer.send([{ topic: 'nselman-chicago-transportation', messages: JSON.stringify(report)}],
	// 	function (err, data) {
	// 		console.log("Kafka Error: " + err)
	// 		console.log(data);
	// 		console.log(report);
	// 		res.redirect('submit-weather.html');
	// 	});
});

	
app.listen(port);
