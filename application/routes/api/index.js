let express = require('express');
let router = new express.Router();

const BusStopTimings = require('./BusStopTimings');
const BusServiceTimings = require('./BusServiceTimings');
const BusService = require('./BusService');

router.get('/timings/bus/:busService', BusServiceTimings);
router.get('/timings/:busStopCode', BusStopTimings);
router.get('/bus/:busService', BusService);

module.exports = router;