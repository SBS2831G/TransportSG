const emailjs = require('emailjs');
const path = require('path');

const presents = require('./present_list');
const elfMagic = require('./elves_at_work');
const sherlockHolmes = require('./sherlock_holmes');

const pug = require('pug');

let server = emailjs.server.connect({
    user:    presents.user,
    password: presents.password,
    host:    presents.host,
    ssl:     true
});

function getServices(query) {
    let parsed = elfMagic.resolveServices(elfMagic.parseQuery(query));
    let buses = elfMagic.filterBuses(parsed);
    function e(a) {return a.match(/(\d+)/)[1];}

    return services = Object.values(buses).map(busStop => busStop.map(svc => svc.service))
        .reduce((a, b) => a.concat(b), []).filter((element, index, array) => array.indexOf(element) === index)
        .sort((a, b) => e(a) - e(b));
}

let previous = {};

function arrayDiff(source, check) {
    let missing = [];
    source.forEach(target => {
        if (!check.includes(target)) missing.push(target);
    });

    return missing
}

function findDifferences(data) {
    let different = false;

    let differences = {};
    Object.keys(data).forEach(fieldName => {
        let currentServices = data[fieldName];
        let additions = arrayDiff(currentServices, previous[fieldName] || []);
        let subtractions = arrayDiff(previous[fieldName] || [], currentServices);

        if (additions.concat(subtractions).length !== 0) different = true;

        differences[fieldName] = {additions, subtractions};
    });

    return {differences, different};
}

function createEmailBody(callback) {
    let data = {
        mkivDeployments: getServices('SLBP BBDEP UPDEP HGDEP nwab SD !123M !160A !63M'),
        vsoDeployments: getServices('BNDEP SBSAMDEP BRBP nwab DD'),
        demons: getServices('KJDEP SMBAMDEP nwab DD'),
        mandy: getServices('wab BD'),
        nwabBendy: getServices('nwab BD'),
        expUpsize: getServices('14e 30e 74e 97e 151e 174e 196e DD'),
        budepUpsize: getServices('947 DD'),
        kjdepUpsize: getServices('307 DD'),
        kjdepDownsize: getServices('180 972 SD'),
        kjdepBendy: getServices('61 176 180 700 700A 972 983 985 BD'),
        slbpDownsize: getServices('179 179A 182 192 198 198A 241 247 248 249 251 252 253 254 255 257 SD'),
        bndepDownsize: getServices('23 65 7A SD'),
        sedepBendy: getServices('SEDEP !800 !804 !806 !807 !811 !812 BD'),
        updepUpsize: getServices('120 122 272 273 93 DD'),
        bbdepDownsize: getServices('147e 7B SD')
    };

    sherlockHolmes(present => {
        if (present) data.mkivDeployments = ['123M'].concat(data.mkivDeployments);

        let changes = findDifferences(data);

        if (!changes.different) {
            callback(null);
            return;
        }

        previous = data;

        let email = pug.renderFile(path.join(__dirname, 'present_wrapper.pug'), {data, differences: changes.differences});
        callback(email);
    });

}

function sendEmail(body) {
    let main = presents.people[0];
    let more = presents.slice(1);
    server.send(emailjs.message.create({
       text:    body,
       from:    "me <sbs9642p@gmail.com>",
       to:      `me <${main}>`,
       bcc:     more.map(e => `a cat <${e}>`).join(', '),
       subject:  "Bus Timing Update",
       attachment: [
           {data: body, alternative: true}
       ]
   }), function(err, message) { console.log(err || message); });
}

if (process.env['NODE_ENV'] && process.env['NODE_ENV'] === 'prod') {
    setInterval(() => {
        createEmailBody(body => {
            if (body !== null)
                sendEmail(body);
        });
    }, 1000 * 60 * 5);

    setTimeout(() => {
        createEmailBody(body => {
            sendEmail(body);
        });
    }, 12000);
}
