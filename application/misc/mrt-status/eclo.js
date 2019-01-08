const time = require('time');

function formatDate(date) {
    date = new time.Date(date);
    return date.getDate() + '-' + (date.getMonth() + 1);
}

module.exports = [
    {
        "line": "NEL",
        "stations": "NE12,NE13,NE14,NE15,NE16,NE17",
        "replacementShuttle": ["Shuttle 21"],
        "isActive": () => {
            let dates = ['11-1', '12-1', '22-2', '23-2', '1-3', '2-3'];
            let now = new time.Date();
            now.setTimezone('Asia/Singapore');
            let today = formatDate(now);
            let yesterday = formatDate(now - 24 * 60 * 1000);
            if (!dates.includes(today) && !dates.includes(yesterday)) return false;

            return (now.getHours() >= 23 && dates.includes(today)) ||
                (now.getHours() === 0 && now.getMinutes() <= 30 && dates.includes(yesterday));
        },
        "isScheduled": true,
        "disruptionType": "SCHEDULED"
    },
    {
        "line": "EWL",
        "stations": "EW12,EW13,EW14",
        "replacementShuttle": ["Shuttle 6"],
        "isActive": () => {
            let dates = ['11-1', '12-1'];
            let now = new time.Date();
            now.setTimezone('Asia/Singapore');
            let today = formatDate(now);
            let yesterday = formatDate(now - 24 * 60 * 1000);
            if (!dates.includes(today) && !dates.includes(yesterday)) return false;

            return (now.getHours() >= 23 && now.getMinutes() >= 30 && dates.includes(today)) ||
                (now.getHours() === 0 && now.getMinutes() <= 30 && dates.includes(yesterday));
        },
        "isScheduled": true,
        "disruptionType": "SCHEDULED"
    },
    {
        "line": "BPL",
        "stations": "BP1,BP2,BP3,BP4,BP5,BP6,BP7,BP8,BP9,BP10,BP11,BP12,BP13",
        "replacementShuttle": ["920, 922, 973, 974", "extended"],
        "isActive": () => {
            let now = new time.Date();
            now.setTimezone('Asia/Singapore');

            if (!(now.getMonth() >= 0 && now.getDate() >= 13 && now.getUTCFullYear() === 2019)) return false;

            return (now.getHours() >= 23 && now.getMinutes() >= 30) || (now.getHours() <= 1 && now.getMinutes() <= 30);
        },
        "isScheduled": true,
        "disruptionType": "SCHEDULED"
    },
]
