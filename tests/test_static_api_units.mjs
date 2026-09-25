// test_static_api_units.mjs — unit tests for static_api.js pure helpers.
// The end-to-end parity gate (test_static_client_parity.mjs) already proves the
// shipped static layer reproduces the live API when the data release is
// present; this covers the dependency-free building blocks so they're checked
// on every push, with no server boot or data needed.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { extract } from './_extract.mjs';

const JS = join(dirname(fileURLToPath(import.meta.url)), '..', 'static_api.js');
const api = extract(JS, { fns: ['normName', 'wsNorm', '_isPartyOffice', 'fnv1a'] });

test('normName: uppercases, strips honorifics/suffixes, keeps A–Z + space', () => {
  assert.equal(api.normName('Dr. John A. Smith Jr.'), 'JOHN A SMITH');
  assert.equal(api.normName("  Mary-Jane  O'Brien III "), 'MARY JANE O BRIEN');
  assert.equal(api.normName(''), '');
  assert.equal(api.normName(null), '');
});

test('wsNorm: trims + collapses whitespace + uppercases (COH/entity keys)', () => {
  assert.equal(api.wsNorm('  jeff   landry '), 'JEFF LANDRY');
  assert.equal(api.wsNorm('JAMBALAYA PAC'), 'JAMBALAYA PAC');
  assert.equal(api.wsNorm(null), '');
});

test('_isPartyOffice: flags party-committee seats', () => {
  assert.equal(api._isPartyOffice('Democratic State Central Committee'), true);
  assert.equal(api._isPartyOffice('Member, DSCC'), true);
  assert.equal(api._isPartyOffice('State Senator -- 5th Senatorial District'), false);
});

// The static client's normName is documented as an exact mirror of the server's
// _norm_name. Lock that invariant down here so the two can't silently drift —
// the whole cross-dataset join depends on identical normalization.
test('normName mirrors the documented server _norm_name pipeline', () => {
  const cases = [
    ['Dr. John A. Smith Jr.', 'JOHN A SMITH'],
    ["Mary-Jane O'Brien III", 'MARY JANE O BRIEN'],
    ['Committee to Elect Jane Doe', 'COMMITTEE TO ELECT JANE DOE'],
  ];
  for (const [input, expected] of cases) {
    assert.equal(api.normName(input), expected, `normName(${input})`);
  }
});

// FNV-1a/32 shard hash — must match _fnv1a in la_ethics_server.py and
// shard_of in build_pages_site.py (tests/test_server_units.py pins the same
// vectors), or a sharded giving / election-lookup fetch misses its bucket.
test('fnv1a: cross-language reference vectors', () => {
  const vectors = { '': 2166136261, 'A': 3289118412,
                    'JEFF LANDRY': 1291349976, 'JOHN BEL EDWARDS': 2036631187 };
  for (const [text, h] of Object.entries(vectors)) assert.equal(api.fnv1a(text), h, text);
});

// ── The whole StaticAPI over an in-memory "Pages site" ─────────────────────
// A fake fetch serves fixture files (gzipped when the name ends .gz) and logs
// every URL, so versioning, sharding and ranking are checked end to end.
import { gzipSync } from 'node:zlib';
const FNV = (s) => { let h = 2166136261; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619) >>> 0; } return h >>> 0; };
const entry = (name, raised) => ({ name, name_upper: name.toUpperCase(), is_candidate: true,
  total_raised: raised, n_cycles: 1, n_races: 1, last_office: '', last_outcome: '',
  last_date: '', filer_number: '' });
const LOOKUP = {
  'JOHN BEL EDWARDS': { outcome: 'Elected', vote_pct: 51.3, office: 'Governor', election_date: '11/16/2019', party: 'DEM' },
  'JEFF LANDRY':      { outcome: 'Elected', vote_pct: 51.6, office: 'Governor', election_date: '10/14/2023', party: 'REP' },
};
const FILES = {
  'version.json': { v: 'abc123' },
  'la_search_index.json.gz': { entries: [entry('John Edwards', 10), entry('John Bel Edwards', 900),
                                         entry('Jeff Landry', 500), entry('Alfred Bell', 5)] },
  'la_entities.json.gz': { entities: {
    '6134': { filer_number: '6134', name: 'Troy Hebert', aliases: [] },
    '1791': { filer_number: '1791', name: 'Troy Hebert', aliases: [] },
    '5788': { filer_number: '5788', name: 'Troy Hebert Sr', aliases: ['TROY HEBERT'] },
  } },
  'la_election_lookup.json': LOOKUP,
};
for (const [k, v] of Object.entries(LOOKUP)) {
  const name = `la_election_lookup_shard_${FNV(k) % 64}.json.gz`;
  FILES[name] = { ...(FILES[name] || {}), [k]: v };
}
const requested = [];
globalThis.STATIC_DATA_BASE = 'data';
globalThis.fetch = async (url) => {
  requested.push(String(url));
  const name = String(url).replace(/^data\//, '').replace(/\?.*$/, '');
  if (!(name in FILES)) return new Response('', { status: 404 });
  const body = Buffer.from(JSON.stringify(FILES[name]));
  return new Response(name.endsWith('.gz') ? gzipSync(body) : body, { status: 200 });
};
await import('../static_api.js');
const S = globalThis.StaticAPI;

test('StaticAPI: every data URL carries the deployed data version', async () => {
  assert.equal(await S.dataVersion(), 'abc123');
  await S.search('jeff');
  const dataUrls = requested.filter(u => !u.endsWith('/version.json'));
  assert.ok(dataUrls.length > 0);
  for (const u of dataUrls) assert.match(u, /\?v=abc123$/, u);
});

test('StaticAPI.search: words match in any order, word-start tier first', async () => {
  const names = async (q) => (await S.search(q)).results.map(r => r.name);
  assert.deepEqual(await names('john edwards'), ['John Bel Edwards', 'John Edwards']);
  assert.deepEqual(await names('edwards john'), ['John Bel Edwards', 'John Edwards']);
  assert.deepEqual(await names('Landry, Jeff'), ['Jeff Landry']);
  assert.deepEqual(await names('bel ed'), ['John Bel Edwards', 'Alfred Bell']);
  assert.deepEqual(await S.search('j'), { results: [], query: 'J', total: 0 });
});

test('StaticAPI.entity: shared name resolves to the highest filer number', async () => {
  assert.equal((await S.entity({ name: 'troy  hebert' })).filer_number, '6134');
  assert.equal((await S.entity({ filer: '1791' })).filer_number, '1791');
  assert.deepEqual(await S.entity({ name: 'nobody' }), {});
});

test('StaticAPI.electionResults: serves the flat lookup, not the raw results file', async () => {
  assert.deepEqual(await S.electionResults(), LOOKUP);
  assert.ok(!requested.some(u => u.includes('la_election_results.json')));
});

test('StaticAPI.electionResultsFor: fetches only the needed shards', async () => {
  const before = requested.length;
  const got = await S.electionResultsFor(['JEFF LANDRY', 'JEFF LANDRY', 'NOT A PERSON']);
  assert.deepEqual(got, { 'JEFF LANDRY': LOOKUP['JEFF LANDRY'] });
  const fetched = requested.slice(before);
  assert.ok(fetched.every(u => /la_election_lookup_shard_\d+\.json\.gz/.test(u)), fetched.join());
});
