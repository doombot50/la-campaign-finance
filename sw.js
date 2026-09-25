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
 *       faster than blocking every load on the network. The libraries (Leaflet,
 *       Chart.js), fonts and the parish map are self-hosted under vendor/, so
 *       they're same-origin and ride this path too.
 *   • data/version.json → network-first (short timeout, cached copy offline).
 *       It names the deployed data build; every other data URL carries it as
 *       ?v=<version> (static_api.js). Seeing a new version prunes the entries
 *       cached for older ones.
 *   • Versioned data (data/…?v=…) → cache-first. A versioned URL names one
 *       immutable build, so a cached copy is always the right bytes.
 *   • Per-year record files (contributions/expenditures/loans_yr*.json.gz) →
 *       network only. The app parses them once into IndexedDB, so a second
 *       copy here would just double the storage.
 *   • Unversioned data (e.g. the money-wins story's JSON) → network-first.
 *   • Everything else (cross-origin map tiles, etc.) → straight to network,
 *       untouched.
 *
 * Why data is not stale-while-revalidate: the app re-streams a cycle when its
 * IndexedDB copy is from an older data version, and SWR answered that
 * re-stream with the OLD cached bytes — which were then saved as current, so
 * returning visitors stayed a build behind. Bump CACHE_VERSION to force-evict
 * everything on the next activate.
 */
'use strict';

const CACHE_VERSION = 'v2';
const SHELL_CACHE = `lacf-shell-${CACHE_VERSION}`;
const DATA_CACHE  = `lacf-data-${CACHE_VERSION}`;
const ALL_CACHES  = [SHELL_CACHE, DATA_CACHE];

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
// `event` is required: on a cache hit the response resolves immediately, and
// without event.waitUntil() holding the background fetch open the browser is
// free to kill the worker before cache.put() lands — so the "revalidate" half
// of stale-while-revalidate would silently never happen and the cached copy
// could stay stale indefinitely.
async function staleWhileRevalidate(event, request, cacheName) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(request);
  const network = fetch(request)
    .then((res) => {
      if (res && res.ok) return cache.put(request, res.clone()).then(() => res);
      return res;
    })
    .catch(() => cached);   // offline: fall back to whatever we have
  if (cached) event.waitUntil(network);
  return cached || network;
}

// How long a network-first request waits before falling back to a cached copy
// (only when one exists — with nothing cached it just keeps waiting).
const NETWORK_TIMEOUT_MS = 4000;

// Network first; the cached copy if the network fails or is slow. `onFresh`
// sees a copy of each fresh successful response (used to prune old versions).
// Clones are taken synchronously, before the page can start reading the body.
function networkFirst(event, request, cacheName, onFresh) {
  const network = fetch(request);
  event.waitUntil(network.then((res) => {
    if (!res || !res.ok) return;
    const forCache = res.clone();
    const forHook = onFresh ? res.clone() : null;
    return caches.open(cacheName).then(async (cache) => {
      await cache.put(request, forCache);
      if (forHook) await onFresh(forHook);
    });
  }).catch(() => {}));
  const fromCache = () => caches.open(cacheName).then((c) => c.match(request));
  const slow = new Promise((r) => setTimeout(r, NETWORK_TIMEOUT_MS))
    .then(fromCache)
    .then((hit) => hit || network);
  return Promise.race([network, slow])
    .catch(() => fromCache().then((hit) => hit || Response.error()));
}

// Cache first — only for URLs whose content never changes (versioned data).
async function cacheFirst(event, request, cacheName) {
  const cache = await caches.open(cacheName);
  const hit = await cache.match(request);
  if (hit) return hit;
  const res = await fetch(request);
  if (res && res.ok) event.waitUntil(cache.put(request, res.clone()).catch(() => {}));
  return res;
}

// A fresh data/version.json arrived: drop every cached entry that belongs to
// another data version, so the data cache holds at most one build.
async function pruneOtherVersions(versionRes) {
  let v;
  try { v = (await versionRes.json()).v; } catch (e) { return; }
  if (!v) return;
  const cache = await caches.open(DATA_CACHE);
  for (const req of await cache.keys()) {
    const rv = new URL(req.url).searchParams.get('v');
    if (rv && rv !== v) await cache.delete(req);
  }
}

const RECORD_FILE_RE = /\/data\/(contributions|expenditures|loans)_yr\d{4}\.json\.gz$/;

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);

  // Only manage our own origin; let everything else (map tiles, etc.) hit the
  // network untouched. Libraries and fonts are same-origin (vendor/), so they
  // fall through to the same-origin handling below.
  if (url.origin !== self.location.origin) return;

  // Navigations → the shell, served fast and revalidated.
  if (req.mode === 'navigate') {
    event.respondWith(
      staleWhileRevalidate(event, req, SHELL_CACHE)
        .then((res) => res || caches.match('./index.html'))
    );
    return;
  }

  // Nightly data artifacts (see the strategy notes at the top).
  if (url.pathname.includes('/data/')) {
    if (url.pathname.endsWith('/data/version.json')) {
      event.respondWith(networkFirst(event, req, DATA_CACHE, pruneOtherVersions));
    } else if (RECORD_FILE_RE.test(url.pathname)) {
      return;   // network only — the app keeps the parsed rows in IndexedDB
    } else if (url.searchParams.has('v')) {
      event.respondWith(cacheFirst(event, req, DATA_CACHE));
    } else {
      event.respondWith(networkFirst(event, req, DATA_CACHE));
    }
    return;
  }

  // Other same-origin static assets (static_api.js, icons, manifest) → SWR.
  event.respondWith(staleWhileRevalidate(event, req, SHELL_CACHE));
});
