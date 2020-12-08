'use strict';

const express= require('express');
const mustache = require('mustache');
const filesystem = require('fs');
const utils = require('./utils')
const app = express();
const port = Number(process.argv[2]);
const hbase = require('hbase')
const hclient = hbase({host: process.argv[3], port: Number(process.argv[4])});

// console.info("running please appear please please")

// hclient.table('chicago_transportation_hourly').row("2020:September:Weekend:WEST TOWN:WEST TOWN:12AM").get((error, value) => {
// 	console.info(value)
// 	console.info(error)
// })

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
    	// console.info("rows:", rows)
		// console.info(err)
		const data = utils.rowToMap(rows);
    	// console.info(data)

		const outputTable = utils.orderHours(data)
		// console.info(outputTable)

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


const kafka = require('kafka-node');
const Producer = kafka.Producer;
const KeyedMessage = kafka.KeyedMessage;
const kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
const kafkaProducer = new Producer(kafkaClient);

app.get('/submit-rides-data.html',function (req, res) {
	console.info('the block of code that gets user inputs is running')
		// console.info(req.query['rideType'].toLowerCase())
	if (['rideshare', 'taxi'].includes(!(req.query['ridetype'].toLowerCase()) &&
		['weekend', 'weekday'].includes(req.query['daytype'].toLowerCase()))) {
		res.redirect('submit-rides.html')
	}
	else {
		let pickup_community_area = req.query['pickup_community_area'];
		let dropoff_community_area = req.query['dropoff_community_area'];
		let daytype = req.query['daytype'];
		let ridetype = req.query['ridetype'];
		// I KNOW! THIS CAN BE REFACTORED!  I DONT HAVE TIME!!!!
		let distance_miles_tenths = utils.isNumber(req.query['distance'].trim()) ? Math.round(req.query['distance'] * 60) : 60;
		let duration_seconds = utils.isNumber(req.query['duration'].trim()) ? Math.round(req.query['duration'] * 10) : 10;
		let trip_total_cents = utils.isNumber(req.query['total'].trim()) ? Math.round(req.query['total'] * 100) : 100;
		let tip_cents = utils.isNumber(req.query['tip'].trim()) ? Math.round(req.query['tip'] * 100) : 100;

		const d = new Date();
		var hour = d.getHours()
		var AM_PM = (hour > 12) ? "PM" : "AM"
		if (hour > 12) {
			hour = hour - 12
		}
		if (hour === 0) {
			hour = 12
		}

		var report = {
			'pickup_community_area': pickup_community_area,
			'dropoff_community_area': dropoff_community_area,
			'daytype': daytype,
			'year': d.getFullYear().toString(),
			'month': utils.getMonthName(d.getMonth() + 1).toString(),
			'hour': utils.addZero(hour).concat(AM_PM),
			'ridetype': ridetype.toLowerCase(),
			'duration_seconds': duration_seconds,
			'miles_tenths': distance_miles_tenths,
			'tip_cents': tip_cents,
			'trip_total_cents': trip_total_cents
		}

		console.info(report)

		kafkaProducer.send([{topic: 'nselman-chicago-transportation', messages: JSON.stringify(report)}],
			function (err, data) {
				console.log("Kafka Error: " + err)
				console.log(data);
				console.log(report);
				res.redirect('submit-rides.html');
			});
	}
});

	
app.listen(port);
