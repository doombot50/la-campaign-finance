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
        const res = await fetch(`${base()}/${name}`);
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
  async function* ndjsonLines(name) {
    const res = await fetch(`${base()}/${name}`);
    if (res.status === 404) return;
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${name}`);
    const reader = gunzipStream(res).getReader();
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
  async function search(q) {
    q = (q || '').trim().toUpperCase();
    if (q.length < 2) return { results: [], query: q, total: 0 };
    const { entries } = await fetchJSON('la_search_index.json.gz');
    const word = [], contains = [];
    for (const e of entries) {
      const pos = e.name_upper.indexOf(q);
      if (pos < 0) continue;
      (pos === 0 || e.name_upper[pos - 1] === ' ' ? word : contains).push(e);
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
  const electionResults = () => fetchJSON('la_election_results.json');

  async function entity({ name, filer } = {}) {
    const { entities } = await fetchJSON('la_entities.json.gz');
    if (filer) return entities[String(filer)] || {};
    if (name) {
      const key = wsNorm(name);
      for (const e of Object.values(entities)) {
        if (wsNorm(e.name) === key) return e;
        for (const a of (e.aliases || [])) if (wsNorm(a) === key) return e;
      }
    }
    return {};
  }

  // ── /api/races (client-side office/year filtering over the full dump) ────
  const OFFICE_GROUPS = {
    major:    new Set(['governor', 'us_senate', 'us_house', 'statewide', 'lt_governor']),
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
  async function candidateHistory(name) {
    const [index, racesRaw, cohCache] = await Promise.all([
      fetchJSON('la_candidate_index.json.gz'),
      fetchJSON('la_candidacies_raw.json.gz'),
      fetchJSON('ethics_coh_cache.json'),
    ]);
    const norm = normName(name);
    const toks = norm.split(' ');
    const t2 = toks.length >= 2 ? `${toks[0]} ${toks[toks.length - 1]}` : null;

    const financial = index[norm] || (t2 && index[t2]) || {};
    const racesList = racesRaw[norm] || (t2 && racesRaw[t2]) || [];
    // Mirror the endpoint: party-committee offices excluded, then sorted by
    // the raw M/D/YYYY date string (lexicographic — same as the server).
    const races = racesList
      .filter(r => !_isPartyOffice(r.office))
      .sort((a, b) => ((a.date || '') < (b.date || '') ? -1 : (a.date || '') > (b.date || '') ? 1 : 0));

    const ethics_coh = cohCache[wsNorm(name)] || null;

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
      entity: (await entity({ name })) || null,
    };
  }

  // /api/candidate-history returns entity:null (not {}) when unmatched
  async function _candidateHistoryExact(name) {
    const payload = await candidateHistory(name);
    if (payload.entity && Object.keys(payload.entity).length === 0) payload.entity = null;
    return payload;
  }

  // ── record streams (contributions / expenditures / loans per cycle) ──────
  function cycleYears(cycleYear) {
    const y = parseInt(cycleYear, 10);
    return [y - 1, y];
  }
  async function* streamRecordLines(type, cycleYear) {
    for (const y of cycleYears(cycleYear)) {
      yield* ndjsonLines(`${type}_yr${y}.json.gz`);
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
    search, overview, insights, electionResults, entity, races,
    industryBreakdown, coh,
    candidateHistory: _candidateHistoryExact,
    streamRecordLines, records,
    _internals: { normName, wsNorm, fetchJSON },
  };
})(typeof window !== 'undefined' ? window : globalThis);
