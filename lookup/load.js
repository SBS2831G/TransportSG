const fs = require('fs');
const parse = require('csv-parse/lib/sync');
const async = require('async');

const DatabaseConnection = require('../application/database/DatabaseConnection');

const config = require('../config.json');

let database = new DatabaseConnection(config.databaseURL, 'TransportSG');
let buses = null;

let completed = 0;

const fileTypes = {
  'TIBS': 'TIB',
  'CSS': 'CSS',
  'SBST': 'SBS',
  'SBSR': 'SBS-R',
  'SG': 'SG',
  'SMRT': 'SMB',

  'PA': 'PA',
  'PC': 'PC',
  'PH': 'PH',
  'PZ': 'PZ',
  'SH': 'SH',
  'RU': 'RU',
  'CB': 'CB',
  'WB': 'WB',
  'WC': 'WC',
  'XD': 'XD',

  'R': 'R'
};

database.connect({
  poolSize: 500
}, (err) => {
  buses = database.getCollection('bus registrations');

  load();
});

let currentOperatorIndex = 0;
let highestOperatorIndex = Object.keys(fileTypes).length;

function load() {
  loadOperator(currentOperatorIndex, numberCompleted => {
    console.log(Object.keys(fileTypes)[currentOperatorIndex] + ': ' + numberCompleted + ' entries');
    completed += numberCompleted;

    currentOperatorIndex++;
    if (currentOperatorIndex === highestOperatorIndex) {
      console.log('Completed ' + completed + ' entries');
      process.exit();
    }

    load();
  });
}

async function loadOperator(index, callback) {
  let operatorName = Object.keys(fileTypes)[index];

  var regoPrefix = fileTypes[operatorName];

  fs.readFile('./data/' + operatorName + '.csv', async (err, data) => {
    let busList = parse(data, {
      columns: true,
      trim: true,
      skip_empty_lines: true
    });

    await processRegoSet(regoPrefix, busList, operatorName === 'R').then(numberCompleted => {
      callback(numberCompleted);
    });
  });
}

async function processRegoSet(regoPrefix, busList, readPrefixFromFile) {
  let promises = [];

  let currentBatch = [];
  let count = 0;

  for (let busCSV of busList) {
    if (readPrefixFromFile)
      regoPrefix = busCSV.Prefix;

    if (busCSV['#ID'] === '' || busCSV.Make === '') continue;

    let busData = transformData(regoPrefix, busCSV);

    let query = {
      'registration.prefix': busData.registration.prefix,
      'registration.number': busData.registration.number
    };

    if (currentBatch.length < 50) {
      currentBatch.push([query, busData]);
    } else {
      currentBatch.push([query, busData]);
      await async.forEach(currentBatch, async bus => {
        await updateBus(bus[0], bus[1]);
      });
      count += currentBatch.length;
      currentBatch = [];
    }
  }
  await async.forEach(currentBatch, async bus => {
    await updateBus(bus[0], bus[1]);
  });
  count += currentBatch.length;
  return count;
}

function transformData(regoPrefix, csv) {
  let status;
  if (csv.Svc.includes('(R)')) {
    csv.Svc = csv.Svc.replace('(R)', '').trim();
    status = 'Retired';
  } else if (csv.Svc.includes('(L)')) {
    csv.Svc = csv.Svc.replace('(L)', '').trim();
    status = 'Layup';
  } else if (csv.Svc.includes('(A)')) {
    csv.Svc = csv.Svc.replace('(A)', '').trim();
    status = 'Accident';
  } else if (csv.Svc === 'Not Registered') {
    csv.Depot = csv.Svc = '';
    status = 'Unregistered';
  }
  return {
    registration: {
      prefix: regoPrefix,
      number: csv['#ID']*1,
      checksum: csv[''],
    },
    busData: {
      make: csv.Make,
      model: csv.Model,

      livery: csv.Livery,
      bodywork: csv.Bodywork,
      chassis: csv['Chassis ID'],
      LifespanExpiry: csv['Lifespan Expiry Date'] ? new Date(csv['Dereg Date'] + ' GMT +0000') : null,
      gearbox: csv['Gearbox model'],
      edsModel: csv['EDS model'],
    },
    operator: {
      operator: csv.Operator,
      depot: csv.Depot,
      permService: csv.Svc.split('/')[0],
      crossOvers: csv.Svc.split('/').slice(1).map(svc => svc.replace('*', '')),
      status
    }, fleet: {
      batch: csv.Batch,

      ad: csv['Advertisement Override']
    },
    misc: {
      chair: csv['Chair model'],
      door: csv['Door model'],
      aircon: csv['Air Con model'],
      notes: csv.Notes
    }
  };
}

async function updateBus(query, data) {
  let bus = await buses.findDocument(query)
  if (bus) {
    if (data.operator.operator === bus.operator.operator && !data.operator.permService) {
      data.operator.permService = bus.operator.permService;
      data.operator.crossOvers = bus.operator.crossOvers;
      data.operator.depot = bus.operator.depot;
      data.fleet.ad = bus.fleet.ad;
    }
    await buses.updateDocument(query, {
      $set: data
    });
  } else {
    await buses.createDocument(data);
  }
}
