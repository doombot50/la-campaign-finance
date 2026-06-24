/* sw.js — service worker for the server-less GitHub Pages dashboard.
 *
 * Goal: make repeat visits near-instant and offline-capable without ever
 * pinning stale data. The dashboard is a single shell (index.html) + a tiny
 * data layer (static_api.js) + nightly-published data artifacts under data/.
 *
 * Strategy, by request kind:
 *   • App shell + same-origin static assets  → stale-while-revalidate.
 *       Serve the cached copy instantly, fetch a fresh one in the background
 *       so the NEXT load is current. A nightly redeploy is thus picked up one
 *       visit later — fine for a shell that changes only on deploy, and far
 *       faster than blocking every load on the network.
 *   • The third-party libraries (Leaflet, Chart.js, fonts) → cache-first.
 *       They're version-pinned and immutable, so once cached they never need
 *       to be refetched; this is the bulk of the repeat-visit byte savings.
 *   • Everything else (cross-origin map tiles, etc.) → straight to network,
 *       untouched.
 *
 * Freshness safety: the app keeps its own record cache in IndexedDB with a
 * built-at staleness check and re-streams when the nightly build advances, so
 * serving a slightly stale data artifact here never strands the user on old
 * numbers — it just paints faster, then reconciles. Bump CACHE_VERSION to
 * force-evict everything on the next activate.
 */
'use strict';

const CACHE_VERSION = 'v1';
const SHELL_CACHE = `lacf-shell-${CACHE_VERSION}`;
const DATA_CACHE  = `lacf-data-${CACHE_VERSION}`;
const LIB_CACHE   = `lacf-lib-${CACHE_VERSION}`;
const ALL_CACHES  = [SHELL_CACHE, DATA_CACHE, LIB_CACHE];

// Precached on install — the minimum needed to paint the shell offline.
// Relative to the SW's scope (the project path on Pages), so this works under
// /<repo>/ as well as at a server root.
const SHELL_ASSETS = [
  './',
  './index.html',
  './static_api.js',
  './manifest.json',
  './icon-192.png',
  './icon-512.png',
  './apple-touch-icon.png',
];

// Third-party hosts whose responses are immutable (version-pinned URLs) and
// worth keeping cached across visits.
const LIB_HOSTS = [
  'unpkg.com',
  'cdn.jsdelivr.net',
  'fonts.googleapis.com',
  'fonts.gstatic.com',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(SHELL_CACHE)
      .then((cache) => cache.addAll(SHELL_ASSETS))
      // A single missing asset shouldn't abort the whole install; the fetch
      // handler will backfill anything that wasn't precached.
      .catch(() => {})
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(
        keys.filter((k) => !ALL_CACHES.includes(k)).map((k) => caches.delete(k))
      ))
      .then(() => self.clients.claim())
  );
});

// Serve from cache, then update the cache in the background for next time.
async function staleWhileRevalidate(request, cacheName) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(request);
  const network = fetch(request)
    .then((res) => {
      if (res && res.ok) cache.put(request, res.clone());
      return res;
    })
    .catch(() => cached);   // offline: fall back to whatever we have
  return cached || network;
}

// Return cache if present; otherwise fetch once and keep it.
async function cacheFirst(request, cacheName) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(request);
  if (cached) return cached;
  const res = await fetch(request);
  // Opaque (no-cors) responses are fine to keep for immutable libs.
  if (res && (res.ok || res.type === 'opaque')) cache.put(request, res.clone());
  return res;
}

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);

  // Immutable third-party libraries → cache-first.
  if (LIB_HOSTS.includes(url.hostname)) {
    event.respondWith(cacheFirst(req, LIB_CACHE));
    return;
  }

  // Only manage our own origin beyond this point; let everything else (map
  // tiles, etc.) hit the network untouched.
  if (url.origin !== self.location.origin) return;

  // Navigations → the shell, served fast and revalidated.
  if (req.mode === 'navigate') {
    event.respondWith(
      staleWhileRevalidate(req, SHELL_CACHE)
        .then((res) => res || caches.match('./index.html'))
    );
    return;
  }

  // Nightly data artifacts → stale-while-revalidate in their own cache.
  if (url.pathname.includes('/data/')) {
    event.respondWith(staleWhileRevalidate(req, DATA_CACHE));
    return;
  }

  // Other same-origin static assets (static_api.js, icons, manifest) → SWR.
  event.respondWith(staleWhileRevalidate(req, SHELL_CACHE));
});
