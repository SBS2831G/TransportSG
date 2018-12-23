let express = require('express');
let router = new express.Router();

let operatorCss = {
    'Go Ahead Singapore': 'gas',
    'SBS Transit': 'sbst',
    'Singapore Bus Services': 'sbs',
    'Tower Transit Singapore': 'tts',
    'LTA Storage': 'lta',
    'Trans Island Buses': 'tibs',
    'SMRT Buses': 'smrt'
};

router.get('/', (req, res) => {
    res.render('bus/lookup');
});

router.post('/', (req, res) => {
    let query = req.body.query;
    let method = req.body.method;

    if (method === 'rego') {
        searchRego(req, res, query * 1);
    } else if (method === 'service') {
        searchByService(req, res, query);
    } else {
        res.status(400).end('Invalid method');
    }
});

function searchByService(req, res, query) {
    let parts = query.match(/^([A-Z]+)? ?(\d+[ABCeM\*]?)?\/?([\d\/]+[ABCeM]?)?/);

    let depot = parts[1],
        service = parts[2],
        crossOvers = parts[3];

    let or = [];
    if (service) {
        if (!service.includes('*'))
            or.push({'operator.permService': service});
        service = service.replace('*', '');
        or.push({
            'operator.crossOvers': {
                $in: [service]
            }
        });
    }
    if (crossOvers) {
        let svcs = crossOvers.split('/');
        or.push({
            'operator.crossOvers': {
                $in: svcs
            }
        });
    }

    res.db.getCollection('bus registrations').findDocuments({$or: or}).toArray((err, buses) => {
        if (depot)
            buses = buses.filter(bus => {
                return bus.operator.depot === depot;
            });
        renderBuses(req, res, buses);
    });
}

function searchRego(req, res, number) {
    let buses = res.db.getCollection('bus registrations');

    buses.findDocuments({
        'registration.number': number
    }).toArray((err, buses) => {
        renderBuses(req, res, buses);
    });
}

function performChecks(busRegos, buses, callback) {
    let promises = [];

    buses = buses.map(bus => {
        if ((new Date() - bus.busData.deregDate > 0 || bus.misc.notes.toLowerCase().includes('scrapped')) && bus.operator.status === '') {
            bus.operator.status = 'Retired';
            promises.push(new Promise(resolve => {
                busRegos.updateDocument({
                    'registration.prefix': bus.registration.prefix,
                    'registration.number': bus.registration.number
                }, {
                    $set: {
                        'operator.status': 'Retired'
                    }
                }, () => {
                    resolve();
                });
            }));
        }

        return bus;
    });

    Promise.all(promises).then(() => {
        callback(buses);
    });
}

function renderBuses(req, res, buses) {
    // buses = buses.map(bus => {
    //     let deregDate = bus.busData.deregDate;
    //     if (!deregDate) return bus;
    //
    //     let difference = deregDate - new Date();
    //
    //     if (difference > 0) { // bus still alive
    //         difference = new Date(difference);
    //     } else {
    //         difference = new Date(-difference);
    //     }
    //     console.log(difference);
    //
    //     return bus;
    // });

    performChecks(res.db.getCollection('bus registrations'), buses, buses => {
        res.render('bus/lookup/results', {buses, operatorCss});
    });
}

module.exports = router;
