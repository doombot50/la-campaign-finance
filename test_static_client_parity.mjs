#!/usr/bin/env node
/* test_static_client_parity.mjs — runs the SHIPPED static_api.js in Node
 * against a live la_ethics_server.py and asserts that every static-mode
 * answer equals the live /api/* answer. This is the end-to-end half of the
 * parity gate: build_static_api.py's artifacts were already proven equal to
 * the API by test_static_parity.py; this proves the client-side JS that
 * CONSUMES them reproduces the API too — including the candidate-history
 * join, search ranking, races filtering, and the record streams.
 *
 * Node 18+ (uses global fetch + DecompressionStream). Exits 1 on any drift.
 */
import { spawn } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';

const PORT = 8799;
const API = `http://127.0.0.1:${PORT}`;
globalThis.STATIC_DATA_BASE = `${API}/data`;
await import('./static_api.js');
const S = globalThis.StaticAPI;

const fails = [];
function check(label, ok, detail = '') {
  console.log(`  ${ok ? 'OK  ' : 'FAIL'} ${label}${ok || !detail ? '' : ' — ' + detail}`);
  if (!ok) fails.push(label);
}
function diff(a, b, path = '$') {
  if (a === null || b === null) return a === b ? null : `${path}: ${JSON.stringify(a)} != ${JSON.stringify(b)}`;
  if (typeof a !== typeof b) return `${path}: type ${typeof a} != ${typeof b}`;
  if (Array.isArray(a)) {
    if (!Array.isArray(b)) return `${path}: array vs non-array`;
    if (a.length !== b.length) return `${path}: length ${a.length} != ${b.length}`;
    for (let i = 0; i < a.length; i++) { const d = diff(a[i], b[i], `${path}[${i}]`); if (d) return d; }
    return null;
  }
  if (typeof a === 'object') {
    for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) {
      const d = diff(a[k], b[k], `${path}.${k}`); if (d) return d;
    }
    return null;
  }
  return a === b ? null : `${path}: ${JSON.stringify(a)} != ${JSON.stringify(b)}`;
}
const getJSON = async (p) => (await fetch(`${API}${p}`)).json();

// ── boot server ──────────────────────────────────────────────────────────────
// LA_OFFLINE: serve the on-disk snapshot as-is (no prefetch / re-download), so
// the live API can't drift from the static artifacts mid-comparison.
const srv = spawn('python3', ['la_ethics_server.py'], {
  env: { ...process.env, PORT: String(PORT), LA_OFFLINE: '1' }, stdio: 'ignore',
});
try {
  let up = false;
  for (let i = 0; i < 60 && !up; i++) {
    try { await fetch(`${API}/health`); up = true; } catch { await sleep(500); }
  }
  if (!up) { console.error('FATAL: server did not come up'); process.exit(1); }

  console.log('Static client parity (shipped static_api.js vs live API):');

  // search
  for (const q of ['landry', 'pac', 'broome', 'aeisha']) {
    const d = diff(await getJSON(`/api/search?q=${q}`), await S.search(q), `search[${q}]`);
    check(`search q=${q}`, !d, d);
  }

  // overview / insights
  {
    const live = await getJSON('/api/overview');
    const mine = await S.overview();
    // artifact carries an extra built_at stamp — live payload keys must match
    const d = (() => { for (const k of Object.keys(live)) { const x = diff(live[k], mine[k], `overview.${k}`); if (x) return x; } return null; })();
    check('overview', !d, d);
  }

  // races: default (major), all, filtered
  for (const qs of ['', '?office=all', '?office=state&year=2023']) {
    const d = diff(await getJSON(`/api/races${qs}`),
                   await S.races(new URLSearchParams(qs.slice(1)).get('office'),
                                 new URLSearchParams(qs.slice(1)).get('year')), `races${qs}`);
    check(`races ${qs || '(default major)'}`, !d, d);
  }

  // candidate-history: person with COH + flows, committee, alias-form name, unknown
  for (const name of ['SHARON WESTON BROOME', 'JAMBALAYA PAC', 'Aeisha S. Kelly', 'JEFF LANDRY', 'NOBODY AT ALL XYZ']) {
    const live = await getJSON(`/api/candidate-history?name=${encodeURIComponent(name)}`);
    const mine = await S.candidateHistory(name);
    const d = diff(live, mine, `ch[${name}]`);
    check(`candidate-history ${name}`, !d, d);
  }

  // candidate-history with an explicit filer number: must resolve to that one
  // filer's career on both sides (the filer-keyed index), never a name merge.
  {
    const fn = '4919';
    const ent = await getJSON(`/api/entity?filer=${fn}`);
    const nm = (ent && ent.name) ? ent.name : 'X';
    const live = await getJSON(`/api/candidate-history?name=${encodeURIComponent(nm)}&filer=${fn}`);
    const mine = await S.candidateHistory(nm, fn);
    const d = diff(live, mine, `ch[filer ${fn}]`);
    check(`candidate-history filer=${fn}`, !d, d);
  }

  // industry breakdown for sampled filers (cycle-specific, as the UI calls it)
  for (const [fn, cyc] of [['4919', '2020-2023'], ['4919', '2024-2027']]) {
    const d = diff(await getJSON(`/api/industry-breakdown?filer=${fn}&cycle=${cyc}`),
                   await S.industryBreakdown(fn, cyc), `industry[${fn}/${cyc}]`);
    check(`industry-breakdown filer=${fn} cycle=${cyc}`, !d, d);
  }

  // coh single + batch
  {
    const d1 = diff(await getJSON('/api/coh?name=JEFF%20LANDRY'), await S.coh('JEFF LANDRY'), 'coh[name]');
    check('coh name=JEFF LANDRY', !d1, d1);
  }

  // record streams: static line counts vs live array lengths (cycle 2026)
  for (const type of ['loans', 'expenditures']) {
    const live = await getJSON(`/api/la-${type === 'loans' ? 'loans' : 'expenditures'}?cycle=2026`);
    const mine = await S.records(type, 2026);
    const d = diff(live, mine, `records[${type}]`);
    check(`records ${type} cycle=2026 (${mine.length} records)`, !d, d);
  }
  {
    let n = 0;
    for await (const _ of S.streamRecordLines('contributions', 2026)) n++;
    const live = await getJSON('/api/la-ethics?cycle=2026');
    check(`records contributions cycle=2026 stream count (${n})`, n === live.length,
          `${n} != ${live.length}`);
  }
} finally {
  srv.kill();
}

if (fails.length) {
  console.error(`\nCLIENT PARITY FAILED: ${fails.length} mismatch(es).`);
  process.exit(1);
}
console.log('\nThe shipped static data layer reproduces the live API exactly.');
