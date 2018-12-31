const mrtLines = require('../../timings/mrt/station-data.json');

let mrtLineAbbreviations = {
    "CCL": "Circle Line",
    "CEL": "Circle Line Extension",
    "CGL": "Changi Airport Branch Line",
    "DTL": "Downtown Line",
    "EWL": "East West Line",
    "NEL": "North East Line",
    "NSL": "North South Line",
    "PEL": "Punggol LRT East Loop",
    "PWL": "Punggol LRT West Loop",
    "SEL": "Sengkang LRT East Loop",
    "SWL": "Sengkang LRT West Loop",
    "BPL": "Bukit Panjang LRT"
}

function getMRTStation(code) {
    let found = null;

    Object.values(mrtLines).forEach(mrtLine => {
        let station = mrtLine.stations.filter(station => station.stationNumber === code);
        if (station.length) found = station[0];
    });

    return found;
}

function hasResumed(status) {
    return status.includes('service') && status.includes('resume');
}

function parse(data) {
    let latestMessage = data.Message[0];
    if (!latestMessage) return [];

    let contents = latestMessage.Content;
    let lineStatuses = contents.match(/(\d{4}hrs: [A-Z]+ - [^.]+)/g);
    lineStatuses = lineStatuses.filter(status => !hasResumed(status));

    let disruptions = [];
    lineStatuses.forEach(status => {
        let statusInfoParts = status.match(/\d{4}hrs: ([A-Z]+) - ([^.]+)/);

        let line = statusInfoParts[1],
            message = statusInfoParts[2];

        let response = data.AffectedSegments.filter(lineResponse => lineResponse.Line == line)[0];

        disruptions.push({ message, response, line });
    });

    return disruptions.map(parseDisruption).map(disruption => {
        let {line} = disruption;
        let start = findDisruptionStart(line, data.Message);

        disruption.since = start;
        return disruption;
    });
};

function findDisruptionStart(line, messages) {
    let times = [];

    messages.forEach(message => {
        let time = new Date(message.CreatedDate),
            contents = message.Content;

        if (contents.includes(line) && !hasResumed(contents))
            times.push(time);
    });

    return times.sort((a, b) => a - b)[0];
}

function parseDisruption(disruption) {
    let {message, response, line} = disruption;

    let disruptionType = determineDisruptionType(message);
    if (response)
        response = parseResponse(response);
    else response = 'NO_RESPONSE';

    return {
        disruptionType,
        line,
        response
    };
}

function listToMRTStations(stations) {
    return stations.split(',').map(stationNumber => {
        let mainStationNumber = stationNumber.split('|')[0];

        return getMRTStation(mainStationNumber);
    });
}

function parseResponse(response) {
    let stations = response.Stations;
    let fbbStations = response.FreeMRTShuttle;
    let fpbStations = response.FreePublicBus;
    let line = response.Line;

    stations = listToMRTStations(stations);
    fbbStations = listToMRTStations(fbbStations);

    if (fpbStations.toLowerCase().includes('free bus service') && fpbStations.includes('island')) {
        fpbStations = 'ISLAND_WIDE';
    } else {
        fpbStations = listToMRTStations(stations);
    }

    return {
        stations, fbbStations, fpbStations,
        disruptionDirection: response.Direction,
        fbbDirection: response.MRTShuttleDirection
    };
}

function determineDisruptionType(message) {
    message = message.toLowerCase();
    let cause = message.match(/due to(?: a)? ([\w ]+)/);
    if (cause) cause = cause[1];

    if (message.includes('additional travelling time')) {
        let disruptionTime = message.match(/(\d+) ?min\w*/);
        if (disruptionTime) disruptionTime = disruptionTime[1] * 1;

        return {
            disruptionType: 'DELAYED_SERVICE',
            disruptionTime,
            cause
        };
    } else if (message.includes('no train service')) {
        return {
            disruptionType: 'NO_SERVICE',
            cause
        }
    }
}

module.exports = {parse};
