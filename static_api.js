/* static_api.js — server-less data layer for the LA campaign-finance dashboard
 *
 * Mirrors every read-only /api/* endpoint on top of the static artifacts the
 * nightly workflow publishes (see build_static_api.py). The dashboard uses it
 * when STATIC_MODE is on (GitHub Pages hosting, or ?static=1 for local
 * testing against la_ethics_server.py's /data/ routes, which emulate Pages).
 *
 * Trust contract: every function here reproduces its endpoint's payload
 * byte-for-byte. test_static_client_parity.mjs runs THIS file in Node against
 * a live server and asserts equality — it gates the nightly upload.
 *
 * No dependencies. Attaches window.StaticAPI (or globalThis in Node).
 */
(function (global) {
  'use strict';

  // Relative by default: on a GitHub Pages *project* site the app lives under
  // /<repo>/, so 'data/…' resolves correctly there AND at the local server's
  // root. Node tests override via STATIC_DATA_BASE.
  const base = () => (global.STATIC_DATA_BASE || 'data');

  // ── data version (cache busting) ──────────────────────────────────────────
  // build_pages_site.py writes data/version.json = {v: <content hash of the
  // deployed data>}. Every data URL carries ?v=<that>, so a URL names one
  // immutable build: the service worker can serve it cache-first, and cached
  // bytes from an older deploy can never answer a request for the current one.
  // Fetched once per page load and revalidated (no-cache), never served stale
  // from the HTTP cache. Absent (older site, local ?static=1 without it) →
  // null → plain unversioned URLs, as before.
  let _versionP = null;
  function dataVersion() {
    if (!_versionP) {
      _versionP = fetch(`${base()}/version.json`, { cache: 'no-cache' })
        .then(r => (r.ok ? r.json() : null))
        .then(j => (j && typeof j.v === 'string' && j.v ? j.v : null))
        .catch(() => null);
    }
    return _versionP;
  }
  function _dataUrl(name, v) {
    return `${base()}/${name}` + (v ? `?v=${encodeURIComponent(v)}` : '');
  }

  // ── fetch + gzip plumbing ─────────────────────────────────────────────────
  const _cache = {};   // name -> Promise<parsed JSON>

  function gunzipStream(res) {
    // Pages serves .gz files as opaque bytes (no Content-Encoding), so the
    // client decompresses explicitly. DecompressionStream: all evergreen
    // browsers + Node 17+.
    return res.body.pipeThrough(new DecompressionStream('gzip'));
  }

  async function fetchJSON(name) {
    if (!(name in _cache)) {
      _cache[name] = (async () => {
        const res = await fetch(_dataUrl(name, await dataVersion()));
        if (!res.ok) throw new Error(`HTTP ${res.status} for ${name}`);
        if (name.endsWith('.gz')) {
          return JSON.parse(await new Response(gunzipStream(res)).text());
        }
        return res.json();
      })();
      _cache[name].catch(() => { delete _cache[name]; });   // allow retry
    }
    return _cache[name];
  }

  // Async generator: NDJSON lines from a gzipped year file. A 404 yields
  // nothing — the live server likewise skips year files that don't exist.
  // `resPromise` lets a caller start the fetch ahead of iteration (see
  // streamRecordLines); the finally block cancels the reader so a consumer
  // that breaks out early doesn't leave the connection open.
  async function* ndjsonLines(name, resPromise) {
    const res = await (resPromise || fetch(_dataUrl(name, await dataVersion())));
    if (res.status === 404) return;
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${name}`);
    const reader = gunzipStream(res).getReader();
    try {
      const decoder = new TextDecoder();
      let buf = '';
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        const lines = buf.split('\n');
        buf = lines.pop();
        for (const ln of lines) if (ln) yield ln;
      }
      if (buf.trim()) yield buf.trim();
    } finally {
      reader.cancel().catch(() => {});
    }
  }

  // ── name normalizers (exact mirrors of the server's) ─────────────────────
  // _norm_name: uppercase, strip honorifics/suffixes, A–Z+space, collapse.
  function normName(s) {
    return (s || '').toUpperCase()
      .replace(/\b(DR|MR|MRS|MS|JR|SR|II|III|IV|ESQ|PHD|MD)\.?\b/g, '')
      .replace(/[^A-Z\s]/g, ' ')
      .replace(/\s+/g, ' ').trim();
  }
  // whitespace-collapse + upper (COH cache keys, entity byname keys)
  function wsNorm(s) {
    return (s || '').trim().toUpperCase().replace(/\s+/g, ' ');
  }

  // ── /api/search ───────────────────────────────────────────────────────────
  // Mirrors build_search_payload: every query word must appear (any order);
  // tier 0 when every word starts a name word, else tier 1; $ raised within.
  async function search(q) {
    q = (q || '').trim().toUpperCase();
    if (q.length < 2) return { results: [], query: q, total: 0 };
    const { entries } = await fetchJSON('la_search_index.json.gz');
    const toks = q.split(/[^A-Z0-9]+/).filter(Boolean);
    const word = [], contains = [];
    if (toks.length) {
      for (const e of entries) {
        const nu = e.name_upper, padded = ' ' + nu;
        let hit = true, atWordStart = true;
        for (const t of toks) {
          if (!nu.includes(t)) { hit = false; break; }
          if (!padded.includes(' ' + t)) atWordStart = false;
        }
        if (hit) (atWordStart ? word : contains).push(e);
      }
    }
    word.sort((a, b) => b.total_raised - a.total_raised);
    contains.sort((a, b) => b.total_raised - a.total_raised);
    const ordered = word.concat(contains);
    return {
      results: ordered.slice(0, 25).map(e => ({
        name: e.name, is_candidate: e.is_candidate, total_raised: e.total_raised,
        n_cycles: e.n_cycles, n_races: e.n_races, last_office: e.last_office,
        last_outcome: e.last_outcome, last_date: e.last_date, filer_number: e.filer_number,
      })),
      query: q, total: ordered.length,
    };
  }

  // ── /api/overview · /api/insights · /api/election-results · /api/entity ──
  const overview        = () => fetchJSON('la_overview.json.gz');
  const insights        = () => fetchJSON('la_insights.json.gz');
  const cycleAggregates = () => fetchJSON('la_cycle_agg.json.gz');
  // The flat, dashboard-facing lookup — the same file /api/election-results
  // serves (NOT la_election_results.json, the raw build input that still
  // carries same-name "ambiguous" people and lacks the first+last keys).
  const electionResults = () => fetchJSON('la_election_lookup.json');

  // Numeric filer number for tie-breaking (mirrors _filer_rank on the server).
  function _filerRank(fn) {
    return /^\d+$/.test(fn) ? Number(fn) : -1;
  }
  // name/alias -> filer, built once per loaded entity table (mirrors the
  // server's _ENTITIES_BYNAME). When several filers share a name, the highest
  // filer number wins on both sides — an order-independent rule, because JS
  // iterates numeric object keys ascending rather than in file order.
  const _entityNameIdx = new WeakMap();
  function _entityNameIndex(entities) {
    let idx = _entityNameIdx.get(entities);
    if (!idx) {
      idx = new Map();
      for (const [fn, e] of Object.entries(entities)) {
        for (const nm of [e.name || '', ...(e.aliases || [])]) {
          const key = wsNorm(nm);
          if (!key) continue;
          const cur = idx.get(key);
          if (cur === undefined || _filerRank(fn) > _filerRank(cur)) idx.set(key, fn);
        }
      }
      _entityNameIdx.set(entities, idx);
    }
    return idx;
  }

  async function entity({ name, filer } = {}) {
    const { entities } = await fetchJSON('la_entities.json.gz');
    if (filer) {
      const e = entities[String(filer)];
      if (e) return e;
      // filer absent from the table — fall through to a name match (mirrors
      // the server's _get_entity, which does the same).
    }
    if (name) {
      const fn = _entityNameIndex(entities).get(wsNorm(name));
      if (fn !== undefined) return entities[fn];
    }
    return {};
  }

  // ── /api/entity-profile — lifetime giving + receiving edge lists ─────────
  // Receiving ships whole (small); giving is hash-sharded by build_pages_site.py
  // into GIVING_SHARDS buckets — fetch only the one the donor name lands in.
  // FNV-1a/32 here MUST match the Python shard_of() in build_pages_site.py.
  const GIVING_SHARDS = 128;
  function fnv1a(s) {
    let h = 2166136261;
    for (let i = 0; i < s.length; i++) {
      h ^= s.charCodeAt(i);
      h = Math.imul(h, 16777619) >>> 0;
    }
    return h >>> 0;
  }
  async function entityProfile(filer, name) {
    filer = (filer || '').trim();
    name  = (name || '').trim();
    const norm = normName(name);
    let receiving = null, giving = null;
    if (filer) {
      const donors = await fetchJSON('la_entity_donors.json.gz').catch(() => ({}));
      receiving = donors[filer] || null;
    }
    if (norm) {
      const shard = fnv1a(norm) % GIVING_SHARDS;
      const bucket = await fetchJSON(`la_entity_giving_shard_${shard}.json.gz`).catch(() => ({}));
      giving = bucket[norm] || null;
    }
    return { filer, name, receiving, giving };
  }

  // ── election lookup, by key ───────────────────────────────────────────────
  // A profile badges ONE person, so Pages ships the ~5 MB lookup hash-sharded
  // (build_pages_site.py) and this fetches only the bucket(s) holding the asked
  // keys: {key: entry} for each key present, exactly as in the full lookup.
  const ELECTION_SHARDS = 64;
  async function electionResultsFor(keys) {
    const out = {};
    for (const k of new Set(keys || [])) {
      if (!k) continue;
      // A missing bucket (e.g. a site built before sharding) reads as empty —
      // no badge — like a missing giving shard does in entityProfile.
      const bucket = await fetchJSON(`la_election_lookup_shard_${fnv1a(k) % ELECTION_SHARDS}.json.gz`)
        .catch(() => ({}));
      if (Object.prototype.hasOwnProperty.call(bucket, k)) out[k] = bucket[k];
    }
    return out;
  }

  // ── /api/entity-activity — one filer's full itemized activity ─────────────
  // Pages ships this resharded per filer (build_pages_site.py explodes the
  // canonical map into activity/<filer>.json.gz); the live server slices its
  // in-memory map. A missing file degrades to the empty shell the server returns.
  const _EMPTY_ACT = { c: [], e: [], l: [], nc: 0, ne: 0, nl: 0, cap: 0 };
  async function entityActivity(filer) {
    filer = (filer || '').trim();
    if (!filer) return _EMPTY_ACT;
    const b = await fetchJSON(`activity/${filer}.json.gz`).catch(() => null);
    return b || _EMPTY_ACT;
  }

  // ── /api/races (client-side office/year filtering over the full dump) ────
  const OFFICE_GROUPS = {
    major:    new Set(['governor', 'statewide', 'lt_governor']),
    federal:  new Set(['us_senate', 'us_house', 'president']),
    state:    new Set(['state_senate', 'state_house']),
    board:    new Set(['board', 'supreme_court']),
    judicial: new Set(['judicial']),
    local:    new Set(['local']),
  };
  async function races(office, year) {
    office = (office || 'major').toLowerCase();
    year   = (year || '').trim();
    const dump = await fetchJSON('la_races.json.gz');
    let list = dump.races;
    if (office !== 'all') {
      const group = OFFICE_GROUPS[office];
      list = list.filter(r => group ? group.has(r.office_type) : r.office_type === office);
    }
    if (year) list = list.filter(r => String(r.year) === year);
    // Mirror the endpoint: years are derived from the FILTERED list.
    const years = [...new Set(list.filter(r => r.year > 0).map(r => r.year))].sort((a, b) => b - a);
    return { races: list, total: list.length, years };
  }

  // ── /api/industry-breakdown ───────────────────────────────────────────────
  async function industryBreakdown(filer, cycle) {
    const dump = await fetchJSON('la_industry_breakdown.json.gz');
    const byCycle = dump.filers[String(filer)] || {};
    let breakdown;
    if (cycle) {
      breakdown = byCycle[cycle] || [];
    } else {
      // 'all' = merge cycles (UI always passes a cycle; provided for parity
      // of shape, with totals re-aggregated and donors re-trimmed).
      const totals = {}, counts = {}, donors = {};
      for (const cyc of Object.keys(byCycle).sort()) {
        for (const row of byCycle[cyc]) {
          totals[row.industry] = (totals[row.industry] || 0) + row.total;
          counts[row.industry] = (counts[row.industry] || 0) + row.count;
          (donors[row.industry] = donors[row.industry] || []).push(...row.top_donors);
        }
      }
      breakdown = Object.keys(totals)
        .sort((a, b) => totals[b] - totals[a])
        .map(ind => ({
          industry: ind,
          total: Math.round(totals[ind] * 100) / 100,
          count: counts[ind],
          top_donors: donors[ind].sort((a, b) => b.amount - a.amount).slice(0, 5),
        }));
    }
    return { filer: String(filer), cycle: cycle || 'all',
             has_industry_data: !!dump.has_industry_data, breakdown };
  }

  // ── /api/coh ──────────────────────────────────────────────────────────────
  async function coh(name) {
    const cache = await fetchJSON('ethics_coh_cache.json');
    if (!name) return cache;
    return cache[wsNorm(name)] || {};
  }

  // ── /api/candidate-history (the full client-side join) ───────────────────
  function _isPartyOffice(o) {
    o = (o || '').toUpperCase();
    return ['DSCC', 'RSCC', 'DPEC', 'RPEC',
            'CENTRAL COMMITTEE', 'EXECUTIVE COMMITTEE', 'PARTY COMMITTEE']
      .some(x => o.includes(x));
  }
  // ── Cross-dataset person name matching ──────────────────────────────────────
  // Mirrors _name_key / _namekey_index in la_ethics_server.py so a SoS ballot
  // spelling ("Liz Baker Murrill") and the formal finance/COH spelling
  // ("Elizabeth Murrill") resolve to one another. Keep _NICK_GROUPS byte-for-byte
  // in sync with the server list (the candidate-history parity gate guards drift).
  // The index key is `first\tlast` because JS objects can't key on a tuple.
  const _NICK_GROUPS = [
    "ROBERT BOB BOBBY ROB ROBBIE", "WILLIAM BILL BILLY WILL WILLIE",
    "RICHARD RICK RICKY DICK RICH", "JAMES JIM JIMMY JIMMIE",
    "JOHN JOHNNY JACK JON", "EDWARD ED EDDIE EDDY NED",
    "GERALD GERARD JERRY JEROLD JERROLD", "MICHAEL MIKE MIKEY MICK",
    "CHARLES CHARLIE CHUCK CHAS", "THOMAS TOM TOMMY",
    "JOSEPH JOE JOEY", "DANIEL DAN DANNY", "DAVID DAVE",
    "RONALD RON RONNIE", "DONALD DON DONNIE", "KENNETH KEN KENNY",
    "ANTHONY TONY", "STEPHEN STEVEN STEVE STEVIE", "ANDREW ANDY DREW",
    "MATTHEW MATT", "CHRISTOPHER CHRIS", "NICHOLAS NICK",
    "BENJAMIN BEN BENNY", "SAMUEL SAM SAMMY", "TIMOTHY TIM",
    "PATRICK PAT", "FREDERICK FRED FREDDIE", "GREGORY GREG",
    "JEFFREY JEFF", "JONATHAN JON", "LAWRENCE LARRY",
    "RAYMOND RAY", "DOUGLAS DOUG", "PHILIP PHIL PHILLIP",
    "ALEXANDER ALEX", "EUGENE GENE", "VINCENT VINCE VINNIE",
    "FRANCIS FRANK FRANKIE", "ALBERT AL", "WALTER WALT",
    "HENRY HANK HARRY", "THEODORE TED TEDDY", "LEONARD LEN LENNY",
    "ELIZABETH LIZ BETH BETTY LIZZIE", "MARGARET MAGGIE MEG PEGGY MARGE",
    "KATHERINE KATHRYN KATHY KATE KATIE KAY", "PATRICIA PAT PATTY TRICIA",
    "JENNIFER JEN JENNY", "DEBORAH DEB DEBBIE", "BARBARA BARB",
    "SUSAN SUE SUSIE", "REBECCA BECKY", "VICTORIA VICKI VICKY",
    "CYNTHIA CINDY", "CHRISTINE CHRISTINA CHRIS TINA", "NICOLE NIKKI",
    "STEPHANIE STEPH", "JESSICA JESS", "PAMELA PAM", "SANDRA SANDY",
    "THADDEUS THAD", "THERESA TERESA TERRY TERRI",
  ];
  const _NICK = {};
  for (const grp of _NICK_GROUPS) {
    const forms = grp.split(' ');
    for (const f of forms) if (!(f in _NICK)) _NICK[f] = forms[0];
  }
  // (_nick_first, last) identity, or null when < 2 tokens. Mirrors _name_key,
  // including the leading-initial skip ("J Douglas Welborn" -> DOUGLAS, meeting
  // the "Doug Welborn" ballot spelling).
  function _nameKey(name) {
    const toks = normName(name).split(' ').filter(Boolean);
    if (toks.length < 2) return null;
    const i = (toks[0].length === 1 && toks.length >= 3) ? 1 : 0;
    return (_NICK[toks[i]] || toks[i]) + '\t' + toks[toks.length - 1];
  }
  // Map identity -> the one source key with it; drop collisions (mirrors
  // _namekey_index) so a fuzzy fallback never attaches the wrong person's money.
  // Memoized per loaded artifact object: fetchJSON hands back the same parsed
  // object for the whole session, and rebuilding an index over every name in
  // the candidate index / candidacies / COH cache on each profile open cost
  // tens of thousands of regex normalizations on the main thread.
  const _namekeyIdxCache = new WeakMap();
  function _namekeyIndexOf(obj) {
    let idx = _namekeyIdxCache.get(obj);
    if (!idx) { idx = _namekeyIndex(Object.keys(obj)); _namekeyIdxCache.set(obj, idx); }
    return idx;
  }
  function _namekeyIndex(keys) {
    const idx = {}, collided = new Set();
    for (const k of keys) {
      const nk = _nameKey(k);
      if (!nk) continue;
      if ((nk in idx) && idx[nk] !== k) collided.add(nk);
      else idx[nk] = k;
    }
    for (const nk of collided) delete idx[nk];
    return idx;
  }

  async function candidateHistory(name, filer) {
    const [index, racesRaw, cohCache] = await Promise.all([
      fetchJSON('la_candidate_index.json.gz'),
      fetchJSON('la_candidacies_raw.json.gz'),
      fetchJSON('ethics_coh_cache.json'),
    ]);
    // Exact identity when a filer number is supplied: mirror the server, which
    // serves that one filer's career from the filer-keyed index and only falls
    // back to the name-keyed index when the filer is unknown (or not built yet).
    let filerEntry = null;
    if (filer) {
      try {
        const filerIndex = await fetchJSON('la_filer_index.json.gz');
        filerEntry = filerIndex[String(filer)] || null;
      } catch (e) { /* artifact absent — fall back to the name index */ }
    }
    const norm = normName(name);
    const toks = norm.split(' ');
    const t2 = toks.length >= 2 ? `${toks[0]} ${toks[toks.length - 1]}` : null;
    // Nickname/maiden-name fallback (mirrors the server): bridges SoS-ballot and
    // formal spellings when no filer pins the identity, so the career chart,
    // election history, and certified COH all resolve to the same person.
    const nk     = _nameKey(name);
    const ciIdx  = _namekeyIndexOf(index);
    const crIdx  = _namekeyIndexOf(racesRaw);
    const cohIdx = _namekeyIndexOf(cohCache);

    const financial = filerEntry || index[norm] || (t2 && index[t2]) ||
                      (nk && (nk in ciIdx) ? index[ciIdx[nk]] : null) || {};
    const racesList = racesRaw[norm] || (t2 && racesRaw[t2]) ||
                      (nk && (nk in crIdx) ? racesRaw[crIdx[nk]] : null) || [];
    // Mirror the endpoint: party-committee offices excluded, then sorted by
    // the raw M/D/YYYY date string (lexicographic — same as the server).
    const races = racesList
      .filter(r => !_isPartyOffice(r.office))
      .sort((a, b) => ((a.date || '') < (b.date || '') ? -1 : (a.date || '') > (b.date || '') ? 1 : 0));

    let ethics_coh = cohCache[wsNorm(name)] || null;
    if (!ethics_coh && nk && (nk in cohIdx)) ethics_coh = cohCache[cohIdx[nk]] || null;

    // Hybrid live-cash estimate from the index's monthly buckets — the same
    // computation the server runs (flows after the certified Dec-31 close).
    let coh_estimate = null;
    if (ethics_coh && ethics_coh.ending_coh != null) {
      const base_year = parseInt(ethics_coh.report_year, 10);
      if (base_year && base_year < new Date().getFullYear()) {
        const cutoff = `${base_year}-12`;
        let raised_since = 0, spent_since = 0;
        for (const [mk, flows] of Object.entries(financial.monthly || {})) {
          if (mk > cutoff) {
            raised_since += flows.in || 0;
            spent_since  += flows.out || 0;
          }
        }
        coh_estimate = {
          base: ethics_coh.ending_coh,
          base_year,
          raised_since: Math.round(raised_since * 100) / 100,
          spent_since:  Math.round(spent_since * 100) / 100,
          estimate: Math.round((ethics_coh.ending_coh + raised_since - spent_since) * 100) / 100,
        };
      }
    }

    return {
      financial, races, norm,
      ethics_coh, coh_estimate,
      entity: (await entity({ filer, name })) || null,
    };
  }

  // /api/candidate-history returns entity:null (not {}) when unmatched
  async function _candidateHistoryExact(name, filer) {
    const payload = await candidateHistory(name, filer);
    if (payload.entity && Object.keys(payload.entity).length === 0) payload.entity = null;
    return payload;
  }

  // ── record streams (contributions / expenditures / loans per cycle) ──────
  function cycleYears(cycleYear) {
    const y = parseInt(cycleYear, 10);
    return [y - 1, y];
  }
  async function* streamRecordLines(type, cycleYear) {
    // Start both year fetches up front so the second file's bytes are already
    // in flight while the first streams; output order (y-1 then y) is
    // unchanged, so the live server's byte-parity holds.
    const years = cycleYears(cycleYear);
    const v = await dataVersion();
    const started = years.map(y => {
      const p = fetch(_dataUrl(`${type}_yr${y}.json.gz`, v));
      p.catch(() => {});   // handled when its turn comes — avoid an unhandled rejection
      return p;
    });
    let i = 0;
    try {
      for (; i < years.length; i++) {
        yield* ndjsonLines(`${type}_yr${years[i]}.json.gz`, started[i]);
      }
    } finally {
      // A consumer that stopped early leaves later prefetches unconsumed —
      // cancel their bodies so the connections don't dangle.
      for (let j = i + 1; j < started.length; j++) {
        started[j].then(r => { if (r && r.body) r.body.cancel().catch(() => {}); })
                  .catch(() => {});
      }
    }
  }
  async function records(type, cycleYear) {
    const out = [];
    for await (const ln of streamRecordLines(type, cycleYear)) {
      try { out.push(JSON.parse(ln)); } catch (e) { /* torn line */ }
    }
    return out;
  }

  global.StaticAPI = {
    search, overview, insights, cycleAggregates, electionResults, electionResultsFor, entity, races,
    industryBreakdown, coh, entityProfile, entityActivity,
    candidateHistory: _candidateHistoryExact,
    streamRecordLines, records, dataVersion,
    _internals: { normName, wsNorm, fetchJSON, fnv1a },
  };
})(typeof window !== 'undefined' ? window : globalThis);
