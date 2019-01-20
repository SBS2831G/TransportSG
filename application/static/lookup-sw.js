const version = "0.0.1";
const cacheName = `bus.transportsg-${version}`;

function cacheFiles(files) {
    return caches.open(cacheName).then(cache => {
        return cache.addAll(files).then(() => self.skipWaiting());
    });
}

self.addEventListener('install', e => {
    const timeStamp = Date.now();
    e.waitUntil(
        cacheFiles([
            '/static/css/style.css',

            '/static/css/bus/lookup.css',
            '/static/css/bus/dropdown.css',
            '/static/css/bus/loading.css',

            '/static/scripts/helper.js',
            '/static/scripts/dropdown.js',
            '/static/scripts/bus/lookup.js',

            '/static/fonts/bree-serif.otf',

            '/',
        ])
    );
});


self.addEventListener('activate', event => {
    event.waitUntil(self.clients.claim());
});

self.addEventListener('fetch', event => {
    if (event.request.method != 'GET') return;

    event.respondWith(
        caches.open(cacheName)
        .then(cache => cache.match(event.request, {ignoreSearch: true}))
        .then(response => {
            return response || fetch(event.request);
        })
    );
});
