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

console.info(utils.createEmptyRow());


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

app.use(express.static('src/public'));
app.get('/hourly-ride-areas.html',function (req, res) {
	const key=req.query['year'] + ":" + req.query['month'] + ":" + req.query['daytype'] + ":" +
		req.query['pickup_community_area'] + ":" + req.query['dropoff_community_area'];
	console.info("key:", key);

    hclient.table('chicago_transportation_hourly').scan({
		filter: {type : "PrefixFilter",
			value: key}, maxVersions: 1
	}, function (err, rows) {
    	console.info("rows:", rows)
		console.info(err)
		const data = rowToMap(rows);
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
		const template = filesystem.readFileSync("src/hourly-result.mustache").toString();
		const html = mustache.render(template, mustacheData);
		res.send(html)
	});
});

app.get('/hourly-rides.html', function (req, res) {
	hclient.table('chicago_transportation_areas').scan({ maxVersions: 1}, (err, rows) => {
		// console.info(rows)
		var template = filesystem.readFileSync("src/hourly-rides.mustache").toString();
		var html = mustache.render(template, {
			areas : rows
		});
		res.send(html)
	})
});
	
app.listen(port);
